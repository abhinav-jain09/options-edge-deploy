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
    kube = os.environ['ES4_KUBECONFIG']
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
        '--example', 'es-replay-shadow'], 600)
    binary = Path(env.get('CARGO_TARGET_DIR', str(source/'rust/target')))/'debug/examples/es-replay-shadow'
    now = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=5)
    # Current rolling hour. Additional opening/session windows can be supplied explicitly.
    windows = [(now-timedelta(hours=1),now)]
    for text in sys.argv[2:]:
        a,b=text.split(','); windows.append((datetime.fromisoformat(a),datetime.fromisoformat(b)))
    for start,end in windows:
        invoke([str(binary), '192.168.100.4:9092', 'es.underlying.es.trades',
            start.isoformat(), end.isoformat()], 210, binary=True)

if __name__ == '__main__':
    try: main()
    except Exception as exc:
        # Subprocess exceptions may contain command arguments, but never the key/env.
        print(type(exc).__name__ + ': replay validation did not pass', file=sys.stderr)
        sys.exit(1)
