#!/usr/bin/env python3
"""Reuse the image-only CAS rollout for existing dev/prod CVD readers, preserving holds."""
import importlib.util
import os
from pathlib import Path
import re
import subprocess

BINDINGS = {
    'context-tape': ('context-tape-service','context-tape','options-edge-context-tape'),
    'gamma-migration': ('gamma-migration-service','gamma-migration','options-edge-gamma-migration'),
    'close-direction': ('close-direction-service','close-direction','options-edge-close-direction'),
    'early-auction-verdict': ('early-auction-verdict-service','early-auction-verdict','options-edge-early-auction-verdict'),
    'es-compression-expansion': ('es-compression-expansion-service','es-compression-expansion','options-edge-es-compression-expansion'),
    'es-open-direction': ('es-open-direction-service','es-open-direction','options-edge-es-open-direction'),
    'es-open-direction-postgres-writer': ('es-open-direction-postgres-writer','es-open-direction-postgres-writer','options-edge-es-open-direction-postgres-writer'),
    'es-spx-align': ('es-spx-align-service','es-spx-align','options-edge-es-spx-align'),
    'level-absorption': ('level-absorption-service','level-absorption','options-edge-level-absorption'),
    'amt-order-bridge': ('amt-order-bridge','amt-order-bridge','options-edge-amt-order-bridge'),
}
TARGETS = {
    'dev': {'registry':'host.docker.internal:5001','http':'http://127.0.0.1:5001','platform':'linux/arm64',
            'kubeconfig':'/Users/abhinav/.kube/jenkins-deployer-dev.kubeconfig','uid':'55dcf4ed-9a04-40c4-aeba-039e998c6241','tag':''},
    'production': {'registry':'192.168.100.252:5000','http':'http://192.168.100.252:5000','platform':'linux/amd64',
            'kubeconfig':'/Users/abhinav/.kube/jenkins-deployer-prod.kubeconfig','uid':'c62ee606-688d-4373-a889-a52c8b919e73','tag':'prod'},
}


def sdk_main():
    result=subprocess.run(['git','ls-remote','git@github.com:abhinav-jain09/options-edge-contracts.git','refs/heads/main'],
        check=True,text=True,capture_output=True,timeout=30)
    match=re.fullmatch(r'([0-9a-f]{40})\s+refs/heads/main\s*',result.stdout)
    if not match:raise ValueError('One exact SDK main branch required')
    return match[1]


def configure(module,environment,service):
    if environment not in TARGETS or service not in BINDINGS or (environment=='production' and service=='level-absorption'):raise ValueError('Known dev/prod reader and environment required')
    target=TARGETS[environment]
    module.SERVICES=dict(BINDINGS)
    module.REGISTRY=target['registry'];module.REGISTRY_HTTP=target['http']
    module.IMAGE_ENVIRONMENT=environment;module.IMAGE_PLATFORM=target['platform'];module.IMAGE_TAG_PREFIX=target['tag']
    module.TARGET_ENVIRONMENT=environment
    os.environ['ES4_KUBECONFIG']=target['kubeconfig'];os.environ['ES4_NS_UID']=target['uid']
    qualified=module.qualified_image
    def latest_sdk(*args,**kwargs):
        image,proof=qualified(*args,**kwargs)
        if proof['contractsSource']!=sdk_main():raise ValueError('Latest image is not qualified with current SDK main')
        return image,proof
    module.qualified_image=latest_sdk


def main():
    if not os.environ.get('JENKINS_URL') or os.environ.get('PERMITTED_SHA_GUARD')!='PASSED':raise ValueError('Guarded Jenkins required')
    spec=importlib.util.spec_from_file_location('cvd_image_core',Path(__file__).parents[1]/'es4/pinned-image-rollforward.py')
    core=importlib.util.module_from_spec(spec);spec.loader.exec_module(core)
    configure(core,os.environ['ENVIRONMENT'],os.environ['SERVICE'])
    core.main()


if __name__=='__main__':main()
