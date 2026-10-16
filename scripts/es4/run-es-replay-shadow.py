#!/usr/bin/env python3
"""Read-only real Databento/Kafka comparison, without a producer or offset commit."""
import base64
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    source = Path(sys.argv[1]).resolve()
    target = os.environ.get('REPLAY_TARGET','')
    if target not in ('dev','production'): raise ValueError('Unknown replay target')
    dev = target == 'dev'
    kube = os.environ['DEV_KUBECONFIG' if dev else 'ES4_KUBECONFIG']
    role,uid = ('dev','DEV_NS_UID') if dev else ('es4','ES4_NS_UID')
    subprocess.run(['bash','scripts/es4/assert-cluster-identity.sh',kube,role,os.environ[uid]],check=True,timeout=30)
    bootstrap,topic = ('localhost:19092','underlying.es.trades') if dev else ('192.168.100.4:9092','es.underlying.es.trades')
    encoded = subprocess.check_output(['kubectl', '--kubeconfig', kube, '-n', 'options-edge',
        'get', 'secret', 'options-edge-databento-feed-env', '-o', 'jsonpath={.data.DATABENTO_API_KEY}'],
        stderr=subprocess.PIPE, timeout=30).decode()
    key = base64.b64decode(encoded, validate=True).decode()
    if not key: raise RuntimeError('Missing Databento key')
    env = dict(os.environ)
    env.pop("DATABENTO_API_KEY",None)
    # No API key in argv, disk artifacts, or output. Sanitize even vendor errors.
    def invoke(args, timeout, binary=False):
        child_env = dict(env, DATABENTO_API_KEY=key) if binary else env
        result = subprocess.run(args, env=child_env, text=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, timeout=timeout)
        safe = result.stdout.replace(key, '[REDACTED]').replace(encoded, '[REDACTED]')
        print(safe, flush=True)
        if result.returncode: raise RuntimeError('Read-only replay validation failed')
    invoke(['cargo', 'build', '--locked', '--manifest-path', str(source/'rust/Cargo.toml'),
        '--example', 'es-replay-shadow', '--example', 'es-replay-lag', '--example', 'es-replay-settings'], 600)
    invoke(['python3','-B','scripts/es4/validate-es-replay-settings.py',str(source)],60)
    binary = Path(env.get('CARGO_TARGET_DIR', str(source/'rust/target')))/'debug/examples/es-replay-shadow'
    now = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=5)
    # Current rolling hour. Additional opening/session windows can be supplied explicitly.
    windows = [(now-timedelta(hours=1),now)]
    for text in sys.argv[2:]:
        a,b=text.split(','); start,end=datetime.fromisoformat(a),datetime.fromisoformat(b)
        if start.tzinfo is None or end.tzinfo is None:raise ValueError('Replay windows require timezone offsets')
        windows.append((start,end))
    for start,end in windows:
        invoke([str(binary), bootstrap, topic,
            start.isoformat(), end.isoformat()], 210, binary=True)

if __name__ == '__main__':
    try: main()
    except Exception as exc:
        # Subprocess exceptions may contain command arguments, but never the key/env.
        print(type(exc).__name__ + ': replay validation did not pass', file=sys.stderr)
        sys.exit(1)
