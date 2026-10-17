"""Finite isolated adapter/line-write controls. Actual AssertionError required; no broker or runtime."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT=Path(__file__).resolve().parents[2]


def main():
    p=argparse.ArgumentParser();p.add_argument('--classpath',type=Path,required=True);p.add_argument('--contracts-source',type=Path,required=True);p.add_argument('--out',type=Path,required=True);args=p.parse_args()
    args.out.mkdir(parents=True,exist_ok=True);cp=args.classpath.read_text().strip();java=Path(os.environ['JAVA_HOME'])/'bin';rows=[]
    def run(classpath,arg,name):
        q=subprocess.run([str(java/'java'),'-Xmx512m','-cp',classpath,'CanonicalArchiveNegativeProbe',arg],capture_output=True,text=True,timeout=25)
        (args.out/(name+'.stdout')).write_text(q.stdout);(args.out/(name+'.stderr')).write_text(q.stderr)
        values=[json.loads(line) for line in q.stdout.splitlines() if line.startswith('{')]
        assert len(values)==1,(name,q.returncode,'missing structured classifier')
        return q.returncode,values[0]
    for name,code,status in [('known-assertion',1,'ASSERTION_KILLED'),('known-incidental',2,'INCIDENTAL_ERROR'),('missing-method',2,'INCIDENTAL_ERROR')]:
        actual,row=run(cp,'--'+name,name);assert actual==code and row['status']==status,row;rows.append(dict(control=name,**row))
    actual,row=run(cp,str(args.contracts_source),'baseline');assert actual==0 and row['status']=='PASS',row;rows.append(dict(control='baseline',**row))
    adapter=ROOT/'scripts/ops/archive/CanonicalArchiveDecoder.java';reader=ROOT/'scripts/ops/archive/StrikeArchiveReader.java'
    originals={path:path.read_bytes() for path in [adapter,reader]}
    cases=[('unbound-startup',adapter,'Schema schema=BINDINGS.get(topic);','Schema schema=BINDINGS.getOrDefault(topic,CvdSchemas.BAR);'),
           ('topic-binding',adapter,'if(!this.topic.equals(topic))throw new IllegalArgumentException("Archive decoder topic mismatch");',''),
           ('null-policy',adapter,'topic.equals("es.futures.footprint.strike")?Set.of(topic):Set.of()','Set.of(topic)'),
           ('api-projection',adapter,'return values.deserialize(topic,headers,value);','return value;')]
    for name,path,old,new in cases+[('decode-before-prefix',reader,None,None)]:
        text=originals[path].decode()
        if name=='decode-before-prefix':
            old='byte[] value = decoder == null ? r.value() : decoder.deserialize(r.topic(), r.headers(), r.value());';assert text.count(old)==1
            text=text.replace(old,'byte[] value = r.value();')
            old='out.write(head.toString().getBytes(StandardCharsets.US_ASCII));';assert text.count(old)==1
            text=text.replace(old,old+'\n        if (decoder != null) value = decoder.deserialize(r.topic(), r.headers(), r.value());')
        else:assert text.count(old)==1,(name,'mutation anchor');text=text.replace(old,new)
        with tempfile.TemporaryDirectory(prefix='archive-control-') as temp:
            temp=Path(temp);file=temp/path.name;file.write_text(text);classes=temp/'classes';classes.mkdir()
            q=subprocess.run([str(java/'javac'),'-J-Xmx256m','--release','21','-cp',cp,'-d',str(classes),str(file)],capture_output=True,text=True,timeout=30)
            (args.out/(name+'.compile')).write_text(q.stdout+q.stderr);assert q.returncode==0,(name,'compile is not an assertion kill',q.stderr)
            actual,row=run(str(classes)+os.pathsep+cp,str(args.contracts_source),name)
            assert actual==1 and row['status']=='ASSERTION_KILLED',(name,row);rows.append(dict(control=name,**row))
    assert all(path.read_bytes()==data for path,data in originals.items())
    report=dict(controls=rows,assertionKills=5,baselines=1,knownClassifierCalibration=True,
                sourceSha256={str(path.relative_to(ROOT)):hashlib.sha256(data).hexdigest() for path,data in originals.items()})
    (args.out/'report.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k not in ['controls','sourceSha256']}))


if __name__=='__main__':main()
