"""Preserve only legacy source-proof cache files outside the verified source tree."""
import os
from pathlib import Path
import re


def quarantine(root, build):
    if not re.fullmatch(r'[1-9][0-9]{0,9}', build):
        raise ValueError('Positive bounded Jenkins build number required')
    source = root / 'scripts/jenkins/__pycache__'
    for directory in (root / 'scripts', root / 'scripts/jenkins', source):
        if directory.is_symlink():
            raise ValueError('Symbolic source cache path refused')
    if not source.exists():
        return []
    if not source.is_dir():
        raise ValueError('Real source cache directory required')
    files = sorted(source.glob('cvd-component-source-proof.cpython-*.pyc'))
    if len(files) > 8:
        raise ValueError('Legacy bytecode count exceeds eight')
    for file in files:
        if not re.fullmatch(r'cvd-component-source-proof\.cpython-[0-9]{2,3}\.pyc', file.name) or file.is_symlink() or not file.is_file() or file.stat().st_size > 1 << 20:
            raise ValueError('Regular bounded legacy source-proof bytecode required')
    if not files:
        return []
    parent = root / '.jenkins-tmp'
    if parent.is_symlink():
        raise ValueError('Symbolic quarantine parent refused')
    parent.mkdir(exist_ok=True)
    if len(list(parent.glob('cvd-proof-bytecode-*'))) >= 128:
        raise ValueError('Preserved bytecode quarantine count is full')
    target = parent / ('cvd-proof-bytecode-' + build)
    target.mkdir(mode=0o700)
    for file in files:
        file.rename(target / file.name)
    return [str(target / file.name) for file in files]


def main():
    if not os.environ.get('JENKINS_URL') or os.environ.get('PERMITTED_SHA_GUARD') != 'PASSED' or os.environ.get('ACTION') != 'update-image':
        raise ValueError('Guarded Jenkins image update required')
    moved = quarantine(Path.cwd(), os.environ['BUILD_NUMBER'])
    print('Preserved legacy source-proof bytecode files:', len(moved))


if __name__ == '__main__':
    main()
