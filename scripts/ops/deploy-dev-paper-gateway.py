#!/usr/bin/env python3
"""Jenkins-only, dev-only Paper gateway provisioning. Never prints credentials or container logs."""
import json
import os
import pathlib
import platform
import plistlib
import re
import stat
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
CONFIG = ROOT / 'infra/dev/ibkr-paper-gateway'
SECRETS = pathlib.Path('/Users/abhinav/.config/options-edge/ibkr-paper-gateway')
IMAGE = 'ghcr.io/gnzsnz/ib-gateway@sha256:1b7d2be59d95293b5695f678e2001f524ca0e8b89b4c0f46013df10a934d55c5'
DOCKER = ['docker', '--context', 'desktop-linux']
NAME = 'ibkr-paper-gateway'
AGENT = pathlib.Path('/Users/abhinav/Library/LaunchAgents/com.optionsedge.ibkr-paper-gateway.plist')

def run(args, timeout=30, env=None):
    return subprocess.run(args, cwd=ROOT, env=env, check=True, text=True, capture_output=True, timeout=timeout).stdout.strip()

def credential_files():
    for path, directory in [(SECRETS, True), *[(SECRETS / name, False) for name in ('username', 'password', 'vnc')]]:
        info = path.lstat()
        expected = 0o700 if directory else 0o600
        if stat.S_ISLNK(info.st_mode) or (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)) is not True or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != expected:
            raise RuntimeError('PAPER_CREDENTIAL_PERMISSIONS_INVALID: ' + str(path))
        if not directory and not 1 <= info.st_size <= 1024:
            raise RuntimeError('PAPER_CREDENTIAL_SIZE_INVALID: ' + str(path))

def verify_tree():
    # Repeat at the effect boundary; a prior Jenkins checkout observation is not authority.
    run(['bash', 'scripts/jenkins/verify-permitted-tree.sh', '--dir', '.'], timeout=120)

def compose_args(bootstrap=False):
    args = DOCKER + ['compose', '-f', str(CONFIG / 'compose.yaml')]
    return args + ['-f', str(CONFIG / 'bootstrap.yaml')] if bootstrap else args

def inspect():
    return json.loads(run(DOCKER + ['inspect', NAME]))[0]

def verify_identity(value):
    if value['Config']['Image'] != IMAGE or value['Config'].get('Labels', {}).get('com.docker.compose.project') != NAME or 'TRADING_MODE=paper' not in value['Config']['Env']:
        raise RuntimeError('EXISTING_CONTAINER_IDENTITY_MISMATCH; no replacement performed')

def install_startup():
    payload = plistlib.dumps({'Label': 'com.optionsedge.ibkr-paper-gateway', 'ProgramArguments': ['/usr/bin/open', '-g', '-a', '/Applications/Docker.app'], 'RunAtLoad': True})
    if AGENT.exists():
        if AGENT.is_symlink() or AGENT.read_bytes() != payload:
            raise RuntimeError('EXISTING_STARTUP_AGENT_DIFFERS; no overwrite performed')
    else:
        verify_tree();AGENT.parent.mkdir(parents=True, exist_ok=True)
        with os.fdopen(os.open(AGENT, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as out:
            out.write(payload)
    domain = 'gui/' + str(os.getuid())
    try:
        run(['launchctl', 'print', domain + '/com.optionsedge.ibkr-paper-gateway'])
    except subprocess.CalledProcessError:
        verify_tree();run(['launchctl', 'bootstrap', domain, str(AGENT)])

def main():
    if platform.system() != 'Darwin' or os.environ.get('JOB_NAME') != NAME or 'local-mac' not in os.environ.get('NODE_LABELS', '').split() or not os.environ.get('BUILD_URL'):
        raise RuntimeError('JENKINS_DEV_MAC_REQUIRED')
    permitted = os.environ.get('PERMITTED_SHA', '')
    if not re.fullmatch('[a-f0-9]{40}', permitted) or run(['git', 'rev-parse', 'HEAD']) != permitted:
        raise RuntimeError('EXACT_PERMITTED_SOURCE_REQUIRED')
    if run(['docker', 'context', 'inspect', 'desktop-linux', '--format', '{{.Endpoints.docker.Host}}']) != 'unix:///Users/abhinav/.docker/run/docker.sock':
        raise RuntimeError('LOCAL_DOCKER_SOCKET_REQUIRED')
    if run(DOCKER + ['info', '--format', '{{.OSType}}/{{.Architecture}}']) not in ('linux/aarch64', 'linux/arm64'):
        raise RuntimeError('LOCAL_ARM64_DOCKER_REQUIRED')
    credential_files()
    env = dict(os.environ, IBKR_PAPER_SECRET_ROOT=str(SECRETS))
    run(compose_args() + ['config', '--quiet'], env=env)
    names = run(DOCKER + ['ps', '-a', '--format', '{{.Names}}']).splitlines()
    if NAME in names:
        value = inspect();verify_identity(value)
        if value['State'].get('Health', {}).get('Status') != 'healthy' or value['HostConfig']['RestartPolicy']['Name'] != 'unless-stopped':
            raise RuntimeError('EXISTING_PAPER_GATEWAY_REQUIRES_EXPLICIT_RECOVERY; no automatic replacement or login retry')
        install_startup()
        print('Existing PAPER gateway healthy; unchanged. Account verification remains with broker service.');return
    verify_tree();run(DOCKER + ['pull', IMAGE], timeout=600)
    verify_tree();run(compose_args(True) + ['up', '-d'], env=env, timeout=120)
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        value = inspect();verify_identity(value)
        if not value['State']['Running']:
            raise RuntimeError('PAPER_GATEWAY_LOGIN_EXITED; initial restart disabled; inspect local GUI; credentials/logs not exported')
        if value['State'].get('Health', {}).get('Status') == 'healthy':
            verify_tree();run(DOCKER + ['update', '--restart', 'unless-stopped', NAME])
            value = inspect();verify_identity(value)
            if value['HostConfig']['RestartPolicy']['Name'] != 'unless-stopped':
                raise RuntimeError('PAPER_RESTART_POLICY_NOT_APPLIED')
            install_startup()
            print('PAPER gateway API socket healthy on dev localhost:4002; restart unless-stopped; settings persistent. Broker account/mode verification still required.');return
        time.sleep(2)
    raise RuntimeError('PAPER_API_NOT_READY; container retained with restart disabled; possible authentication/2FA required via local VNC5902; no credentials/logs exported')

if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as failure:
        # Never relay captured third-party stdout/stderr, which may contain authentication material.
        if isinstance(failure, RuntimeError):print(str(failure), file=__import__('sys').stderr)
        else:print('PAPER_GATEWAY_DEPLOYMENT_FAILED: ' + type(failure).__name__ + '; inspect local state without exporting credentials', file=__import__('sys').stderr)
        raise SystemExit(1)
