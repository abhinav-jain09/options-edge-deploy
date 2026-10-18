#!/usr/bin/env python3
"""Image-only updates for five existing CVD readers/writers. Never creates or scales a workload."""
import json
import hashlib
import copy
import os
from pathlib import Path
import re
import subprocess
import urllib.request
import urllib.parse

NS = 'options-edge'
REGISTRY = '192.168.100.252:5000'
REGISTRY_HTTP = 'http://' + REGISTRY
IMAGE_ENVIRONMENT = 'production'
IMAGE_PLATFORM = 'linux/amd64'
IMAGE_TAG_PREFIX = 'prod'
TARGET_ENVIRONMENT = 'es4'
SERVICES = {
    'es-cvd': ('es-cvd-service', 'es-cvd', 'options-edge-es-cvd'),
    'es-footprint-strike': ('es-footprint-strike-service', 'es-footprint-strike', 'options-edge-es-footprint-strike'),
    'gamma-migration': ('gamma-migration-service', 'gamma-migration', 'options-edge-gamma-migration'),
    'context-tape': ('context-tape-service', 'context-tape', 'options-edge-context-tape'),
    'close-direction': ('close-direction-service', 'close-direction', 'options-edge-close-direction'),
}


def image_parts(service, image):
    if service not in SERVICES:
        raise ValueError('service is outside the CVD image-only allowlist')
    repo = SERVICES[service][2]
    match = re.fullmatch(re.escape(REGISTRY + '/' + repo) + r'(?::[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})?@(sha256:[0-9a-f]{64})', image)
    if not match:
        raise ValueError('exact expected repository and digest required')
    return repo, match[1]


def qualified_image(service, builds, fetch_lock):
    if service not in SERVICES:
        raise ValueError('service is outside the CVD image-only allowlist')
    if not isinstance(builds, list) or len(builds) > 64:
        raise ValueError('bounded Jenkins build list required')
    repo = SERVICES[service][2]
    candidates = []
    for build in builds:
        if type(build.get('number')) is not int or build['number'] < 1:
            raise ValueError('positive Jenkins build number required')
        params = {}
        for action in build.get('actions', []):
            for entry in action.get('parameters', []):
                params[entry['name']] = entry.get('value')
        target = params.get('BUILD_TARGET')
        relevant = target == repo or (target == 'cvd-avro-related' and service != 'close-direction')
        if relevant and params.get('ENVIRONMENT') == IMAGE_ENVIRONMENT:
            candidates.append((build, params))
    if not candidates:
        raise ValueError('no recent production build for this service')
    build, params = max(candidates, key=lambda row: row[0]['number'])
    if build.get('building') is not False or build.get('result') != 'SUCCESS' or params.get('PUSH_IMAGES') is not True:
        raise ValueError('latest relevant production build is not a successful published qualification')
    text = fetch_lock(build['number'])
    if len(text.encode()) > 262144:
        raise ValueError('image lock exceeds 256KiB')
    values = {}
    for line in text.splitlines():
        match = re.fullmatch('([A-Z][A-Z0-9_]*)=(.*)', line)
        if not match or match[1] in values:
            raise ValueError('invalid or duplicate image-lock field')
        values[match[1]] = match[2]
    prefix = 'OPTIONS_EDGE_IMAGE_LOCK_'
    if values.get(prefix + 'FORMAT') != '1' or values.get(prefix + 'SOURCE_REPO') != 'git@github.com:abhinav-jain09/options-edge-processing.git':
        raise ValueError('unexpected image-lock format or source repository')
    if values.get(prefix + 'BUILD_ID') != str(build['number']) or values.get(prefix + 'PLATFORM') != IMAGE_PLATFORM:
        raise ValueError('foreign build or platform in image lock')
    for field, parameter in [('GIT_COMMIT', 'PERMITTED_SHA'), ('CONTRACTS_GIT_COMMIT', 'CONTRACTS_PERMITTED_SHA')]:
        value = values.get(prefix + field, '')
        if not re.fullmatch('[0-9a-f]{40}', value) or value != params.get(parameter):
            raise ValueError('image source differs from the successful build parameters')
    if values.get(prefix + 'GUARD_VERSION') != '2f6fc2ab7aa51eb8a9e8aa350f2d774cd2fb7a253de2591e43645a19a9886b24' or not re.fullmatch('[0-9a-f]{64}', values.get(prefix + 'CONTRACTS_JAR_SHA256', '')):
        raise ValueError('image guard or SDK byte provenance missing')
    matches = []
    for key, value in values.items():
        if key.endswith('_IMAGE'):
            try:
                image_parts(service, value)
            except ValueError:
                continue
            matches.append((key, value))
    if len(matches) != 1:
        raise ValueError('image lock must contain one exact service image')
    key, image = matches[0]
    tag = values.get(prefix + 'TAG', '')
    tag_match = re.fullmatch((re.escape(IMAGE_TAG_PREFIX) + '-' if IMAGE_TAG_PREFIX else '') + str(build['number']) + '-([0-9a-f]{12,40})', tag)
    if not tag_match or not values[prefix + 'GIT_COMMIT'].startswith(tag_match[1]) or ':' + tag + '@' not in image:
        raise ValueError('service image does not use this build and source tag')
    if values.get(key + '_GIT_COMMIT') != values[prefix + 'GIT_COMMIT']:
        raise ValueError('service image source differs from image lock')
    return image, {'processingBuild': build['number'], 'processingSource': values[prefix + 'GIT_COMMIT'],
                   'contractsSource': values[prefix + 'CONTRACTS_GIT_COMMIT'],
                   'contractsJarSha256': values[prefix + 'CONTRACTS_JAR_SHA256']}


def open_direct(url, timeout):
    return urllib.request.build_opener(urllib.request.ProxyHandler({})).open(url, timeout=timeout)


def processing_main():
    result = subprocess.run(['git', 'ls-remote', 'git@github.com:abhinav-jain09/options-edge-processing.git',
                             'refs/heads/main'], check=True, text=True, capture_output=True, timeout=30)
    match = re.fullmatch(r'([0-9a-f]{40})\s+refs/heads/main\s*', result.stdout)
    if not match:
        raise ValueError('one exact processing main branch required')
    return match[1]


def read_url(url, limit):
    with open_direct(url, timeout=15) as response:
        data = response.read(limit + 1)
    if len(data) > limit:
        raise ValueError('Jenkins response exceeds byte bound')
    return data.decode('utf-8')


def patch_for(service, live, image):
    image_parts(service, image)
    name, container, repo = SERVICES[service]
    if live.get('kind') != 'Deployment' or live.get('apiVersion') != 'apps/v1':
        raise ValueError('existing apps/v1 Deployment required')
    meta, spec = live.get('metadata', {}), live.get('spec', {})
    if meta.get('name') != name or meta.get('namespace') != NS:
        raise ValueError('unexpected workload identity')
    if not isinstance(meta.get('uid'), str) or not meta['uid'] or not re.fullmatch('[0-9]+', str(meta.get('resourceVersion', ''))):
        raise ValueError('UID and resourceVersion fences required')
    replicas = spec.get('replicas')
    if type(replicas) is not int or replicas < 0:
        raise ValueError('explicit nonnegative replica count required')
    pod = spec.get('template', {}).get('spec', {})
    containers = pod.get('containers', [])
    if len(containers) != 1 or containers[0].get('name') != container or pod.get('initContainers'):
        raise ValueError('known single application container required')
    old = containers[0].get('image')
    if not isinstance(old, str) or not old:
        raise ValueError('existing image required')
    return [
        {'op': 'test', 'path': '/metadata/uid', 'value': meta['uid']},
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': meta['resourceVersion']},
        {'op': 'test', 'path': '/spec/replicas', 'value': replicas},
        {'op': 'test', 'path': '/spec/template/spec/containers/0/name', 'value': container},
        {'op': 'test', 'path': '/spec/template/spec/containers/0/image', 'value': old},
        {'op': 'replace', 'path': '/spec/template/spec/containers/0/image', 'value': image},
    ]


def kube(*args, timeout=30):
    return subprocess.run(['kubectl', '--kubeconfig', os.environ['ES4_KUBECONFIG'], '-n', NS, *args],
                          check=True, text=True, capture_output=True, timeout=timeout).stdout


def verify(service, before, after, image):
    patch_for(service, after, image)  # Revalidate identity, replicas and container structure.
    if after['metadata']['uid'] != before['metadata']['uid']:
        raise ValueError('workload was replaced during image update')
    if after['spec']['replicas'] != before['spec']['replicas']:
        raise ValueError('replica hold changed during image update')
    expected = copy.deepcopy(before['spec'])
    expected['template']['spec']['containers'][0]['image'] = image
    if after['spec'] != expected:
        raise ValueError('non-image workload spec changed or image is not pinned')


def main():
    if not os.environ.get('JENKINS_URL') or os.environ.get('PERMITTED_SHA_GUARD') != 'PASSED':
        raise ValueError('guarded Jenkins execution required')
    if not re.fullmatch('[0-9a-f]{40}', os.environ.get('PERMITTED_SHA', '')):
        raise ValueError('exact main source provenance required')
    service = os.environ['SERVICE']
    if os.environ.get('REQUIRED_IMAGE'):
        raise ValueError('image is derived from latest qualification; manual image selectors are refused')
    base = os.environ['JENKINS_URL'].rstrip('/') + '/job/options-edge-processing/'
    tree = 'builds[number,result,building,actions[parameters[name,value]]]{0,64}'
    builds = json.loads(read_url(base + 'api/json?' + urllib.parse.urlencode({'tree': tree}), 2 << 20))['builds']
    image, provenance = qualified_image(service, builds,
        lambda number: read_url(base + str(number) + '/artifact/.jenkins-tmp/options-edge-image-lock.env', 262144))
    if provenance['processingSource'] != processing_main():
        raise ValueError('latest qualification is not current processing main; qualify latest main first')
    repo, digest = image_parts(service, image)
    dry = os.environ['DEPLOY_DRY_RUN']
    if dry not in ['true', 'false']:
        raise ValueError('explicit dry-run state required')
    who = json.loads(kube('auth', 'whoami', '-o', 'json'))['status']['userInfo']['username']
    if who != 'system:serviceaccount:options-edge:jenkins-deployer':
        raise ValueError('Jenkins deployer identity required')
    namespace = json.loads(kube('get', 'namespace', NS, '-o', 'json'))
    if namespace['metadata']['uid'] != os.environ['ES4_NS_UID']:
        raise ValueError('es4 namespace identity mismatch')
    request = urllib.request.Request(REGISTRY_HTTP + '/v2/' + repo + '/manifests/' + digest,
        headers={'Accept': 'application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.docker.distribution.manifest.list.v2+json'})
    with open_direct(request, timeout=10) as response:
        body = response.read((2 << 20) + 1)
        if len(body) > 2 << 20 or 'sha256:' + hashlib.sha256(body).hexdigest() != digest:
            raise ValueError('registry manifest digest mismatch')
    before = json.loads(kube('get', 'deployment/' + SERVICES[service][0], '-o', 'json'))
    patch = patch_for(service, before, image)
    closure = os.environ.get('CVD_READER_CLOSURE_CONFIRMED', 'false')
    if closure not in ['true', 'false']:
        raise ValueError('explicit reader closure acknowledgement required')
    if dry == 'false' and before['spec']['replicas'] > 0 and closure != 'true':
        raise ValueError('active workload requires completed reader/schema/runtime closure acknowledgement')
    build = os.environ['BUILD_NUMBER']
    if not re.fullmatch('[0-9]+', build):
        raise ValueError('invalid Jenkins build number')
    out = Path('.jenkins-tmp/es4-image-' + build)
    out.parent.mkdir(exist_ok=True)
    if out.parent.is_symlink():
        raise ValueError('refusing symlinked image-update directory')
    if out.exists() or out.is_symlink():
        raise ValueError('image-update build directory already exists; a fresh Jenkins run is required')
    out.mkdir()
    file = out / 'patch.json'
    payload = json.dumps(patch)
    file.write_text(payload)  # Review copy only; the write consumes the immutable in-memory payload.
    args = ['patch', 'deployment/' + SERVICES[service][0], '--type=json', '--patch', payload]
    receipt = {'source': os.environ['PERMITTED_SHA'], 'environment': TARGET_ENVIRONMENT, 'deployment': SERVICES[service][0], 'image': image,
               'replicas': before['spec']['replicas'], 'readyReplicasBefore': before.get('status', {}).get('readyReplicas', 0), 'dryRun': dry == 'true', **provenance,
               'readerClosureAcknowledged': closure == 'true',
               'patchSha256': hashlib.sha256(payload.encode()).hexdigest(),
               'phase': 'patch-prepared', 'runtimeReadinessVerified': False,
               'scope': 'selected existing image only; no configuration, state, topic, group or offset changes'}
    def save():
        (out / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    save()
    try:
        admitted = json.loads(kube(*args, '--dry-run=server', '-o', 'json'))
        verify(service, before, admitted, image)
        receipt['phase'] = 'dry-run-validated'
        save()
        print('Validated image-only patch for ' + SERVICES[service][0])
        if dry == 'false':
            # Preserve an honest unknown-outcome receipt if the write's response is lost.
            receipt['phase'] = 'apply-requested'
            save()
            applied = json.loads(kube(*args, '-o', 'json'))
            receipt['phase'] = 'apply-returned'
            save()
            verify(service, before, applied, image)
            print('Updated pinned image for ' + SERVICES[service][0])
            if before['spec']['replicas'] > 0:
                print(kube('rollout', 'status', 'deployment/' + SERVICES[service][0], '--timeout=600s', timeout=620), end='')
            after = json.loads(kube('get', 'deployment/' + SERVICES[service][0], '-o', 'json'))
            verify(service, before, after, image)
            receipt['phase'] = 'verified'
            receipt['runtimeReadinessVerified'] = before['spec']['replicas'] > 0
            save()
    except Exception as error:
        receipt['failureType'] = type(error).__name__
        save()
        raise
    print(json.dumps(receipt))


if __name__ == '__main__':
    main()
