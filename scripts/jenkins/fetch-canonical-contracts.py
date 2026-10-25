#!/usr/bin/env python3
"""Jenkins-only download of one successful, main-sourced immutable contracts release."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import xml.etree.ElementTree as ET
import zipfile
import io
import urllib.parse
import urllib.request

REPOSITORIES = {"git@github.com:abhinav-jain09/options-edge-contracts.git",
                "https://github.com/abhinav-jain09/options-edge-contracts.git"}
BRANCHES = {"main", "origin/main", "refs/remotes/origin/main", "*/main"}


def read(url, maximum):
    with urllib.request.urlopen(url, timeout=15) as response:
        body = response.read(maximum + 1)
    if len(body) > maximum:
        raise ValueError("Contracts release response exceeds byte budget")
    return body


def fetch(base, build, commit, digest, version, destination):
    parsed = urllib.parse.urlsplit(base)
    if parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("Jenkins base URL required without credentials/query/fragment")
    if not re.fullmatch(r"[1-9][0-9]{0,8}", build) or not re.fullmatch(r"[0-9a-f]{40}", commit) \
            or not re.fullmatch(r"[0-9a-f]{64}", digest) or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise ValueError("Exact release build, source commit, version and SHA-256 required")
    build_url = base.rstrip("/") + "/job/options-edge-contracts/" + build
    query = "number,result,building,actions[remoteUrls,lastBuiltRevision[SHA1,branch[name,SHA1]]],artifacts[relativePath]"
    metadata = json.loads(read(build_url + "/api/json?tree=" + urllib.parse.quote(query, safe=",[]"), 1024 * 1024))
    if metadata.get("building") is not False or metadata.get("result") != "SUCCESS" or metadata.get("number") != int(build):
        raise ValueError("Contracts release build is not completed SUCCESS")
    sources = [action["lastBuiltRevision"] for action in metadata.get("actions", [])
               if REPOSITORIES.intersection(action.get("remoteUrls", [])) and "lastBuiltRevision" in action]
    if len(sources) != 1 or sources[0].get("SHA1") != commit or not sources[0].get("branch") \
            or any(branch.get("name") not in BRANCHES or branch.get("SHA1") != commit for branch in sources[0]["branch"]):
        raise ValueError("Contracts release source is not the permitted repository/main commit")
    artifact = "target/options-edge-contracts-" + version + ".jar"
    if artifact not in [item.get("relativePath") for item in metadata.get("artifacts", [])]:
        raise ValueError("Exact contracts release JAR is not archived by this build")
    body = read(build_url + "/artifact/" + artifact, 16 * 1024 * 1024)
    if hashlib.sha256(body).hexdigest() != digest:
        raise ValueError("Contracts release JAR SHA-256 mismatch")
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise ValueError("Contracts release destination already exists")
    # The hash is verified before any packaged Python code can be imported.
    with destination.open("xb") as output:
        output.write(body)
    try:
        import sys
        sys.path.insert(0, str(destination.resolve()))
        from optionsedge_contracts.artifact import ContractsArtifact
        with ContractsArtifact(destination, version=version, sha256=digest):
            pass
    except Exception:
        destination.unlink()
        raise
    destination.with_suffix(".release.json").write_text(json.dumps({"build": int(build), "source": commit,
        "sha256": digest, "version": version, "url": build_url}, indent=2) + "\n")


def latest(base, source, destination):
    """Resolve main once, require its newest CI build, then freeze this build's bytes."""
    source=Path(source)
    if source.exists():raise ValueError('Fresh contracts source directory required')
    subprocess.run(['git','clone','--depth','1','--single-branch','--branch','main',
                    'git@github.com:abhinav-jain09/options-edge-contracts.git',str(source)],
                   check=True,timeout=120,env={**os.environ,'GIT_TERMINAL_PROMPT':'0',
                                             'GIT_SSH_COMMAND':'ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes'})
    commit=subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],timeout=10,text=True).strip()
    version=ET.parse(source/'pom.xml').getroot().findtext('{http://maven.apache.org/POM/4.0.0}version')
    # Resolve the moving pointer once. An incomplete/failed latest build is a refusal,
    # never a reason to fall back to an older successful image or contracts package.
    query='number,result,building,actions[remoteUrls,lastBuiltRevision[SHA1,branch[name,SHA1]]],artifacts[relativePath]'
    metadata=json.loads(read(base.rstrip('/')+'/job/options-edge-contracts/lastBuild/api/json?tree='+
                             urllib.parse.quote(query,safe=',[]'),1024*1024))
    build=str(metadata['number'])
    artifact='target/options-edge-contracts-'+str(version)+'.jar'
    if metadata.get('building') is not False or metadata.get('result')!='SUCCESS':
        raise ValueError('Latest contracts main build is not completed SUCCESS')
    revisions=[a['lastBuiltRevision'] for a in metadata.get('actions',[])
               if REPOSITORIES.intersection(a.get('remoteUrls',[])) and 'lastBuiltRevision' in a]
    if len(revisions)!=1 or revisions[0].get('SHA1')!=commit:
        raise ValueError('Latest contracts build does not match current main')
    if artifact not in [a.get('relativePath') for a in metadata.get('artifacts',[])]:
        raise ValueError('Latest main artifact not archived')
    body=read(base.rstrip('/')+'/job/options-edge-contracts/'+build+'/artifact/'+artifact,16*1024*1024)
    # CI can retain old target JARs. Prove all packaged Python/schema resources are
    # this main checkout's literal bytes before importing any packaged code.
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        if len(archive.infolist())>16384 or len(set(archive.namelist()))!=len(archive.namelist()):
            raise ValueError('Contracts package entry budget/uniqueness violation')
        resources={p.relative_to(source/'schemas').as_posix():p for p in (source/'schemas').rglob('*.avsc')}
        resources.update({p.relative_to(source/'python').as_posix():p for p in (source/'python/optionsedge_contracts').rglob('*.py')})
        # The producer-authored registry files pom.xml packages under schemas-registry/ (the same include rules as the pom: three indicator
        # studies files, ta/*.avsc and ta/v1/*.avsc, esflow/*.avsc), keyed by their path from the checkout root.
        reg=source/'schemas-registry'
        for rel in [reg/'indicators'/(n+'.avsc') for n in ('StudyBar','StudiesCurrent','StudyEngineImage')]+sorted((reg/'ta').glob('*.avsc'))+sorted((reg/'ta'/'v1').glob('*.avsc'))+sorted((reg/'esflow').glob('*.avsc')):
            if not rel.is_file():raise ValueError('Packaged registry source missing in main: '+rel.relative_to(source).as_posix())
            resources[rel.relative_to(source).as_posix()]=rel
        actual={name for name in archive.namelist() if name.endswith('.avsc') or
                name.startswith('optionsedge_contracts/') and name.endswith('.py')}
        if not resources or actual!=set(resources):raise ValueError('Contracts package resource inventory differs from main')
        metadata_files={'META-INF/MANIFEST.MF',
                        'META-INF/maven/com.optionsedge/options-edge-contracts/pom.xml',
                        'META-INF/maven/com.optionsedge/options-edge-contracts/pom.properties'}
        for name in archive.namelist():
            if name.endswith('/') or name in resources or name in metadata_files:continue
            if name.startswith(('com/optionsedge/','app/')) and name.endswith('.class'):continue
            raise ValueError('Unverified contracts package entry: '+name)
        for name,path in resources.items():
            if archive.getinfo(name).file_size>1024*1024 or archive.read(name)!=path.read_bytes():
                raise ValueError('Contracts package resource differs from main: '+name)
    digest=hashlib.sha256(body).hexdigest()
    # Re-read the immutable numbered artifact in fetch and require exactly the
    # already-verified digest. Its second read also rechecks source/branch metadata.
    fetch(base,build,commit,digest,version,destination)


if __name__ == "__main__":
    if not os.environ.get("JENKINS_URL"):
        raise SystemExit("Release download runs only inside Jenkins")
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--destination", required=True)
    arguments = parser.parse_args()
    latest(os.environ["JENKINS_URL"], arguments.source, arguments.destination)
