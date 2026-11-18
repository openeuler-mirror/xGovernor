"""Fit a two-signal fork gate from labeled independent attempts (stdlib only)."""
import argparse
import csv
import hashlib
import json
import math
import re
from pathlib import Path

WORDS = ['perhaps','maybe','unclear','not sure','uncertain','alternative','reconsider',
         'instead','still fails','might be','could be']
MATCH = re.compile(r'\b(?:'+ '|'.join(re.escape(w) for w in WORDS)+r')\b', re.I)


def sigmoid(x):
    return 1/(1+math.exp(-max(-40,min(40,x))))


def fit(buckets):
    total=sum(w for t,u,w,y in buckets)
    prevalence=sum(y for t,u,w,y in buckets)/total
    b=math.log(max(1e-6,prevalence)/max(1e-6,1-prevalence)); a=c=0.
    for _ in range(1500):
        db=da=dc=0.
        for t,u,w,y in buckets:
            error=w*sigmoid(b+a*t+c*u)-y
            db+=error; da+=error*t; dc+=error*u
        b-=.8*db/total
        a=max(0.,a-.8*(da/total+.002*a))
        c=max(0.,c-.8*(dc/total+.002*c))
    return b,a,c


def calibrate(trace_root, labels):
    buckets={}; runs=failures=0; tasks=set(); checkpoints=0
    for row in csv.DictReader(Path(labels).open()):
        task=row['instance_id']
        if 'sphinx' in task: continue
        attempts=[]
        for run in (1,2,3):
            passed=row.get(f'r{run}_resolved')=='True'
            if row.get(f'r{run}_status') not in ('resolved','unresolved') and not passed: continue
            path=Path(trace_root)/f'swebench-run-{run}'/task/'trace.jsonl'
            if path.exists(): attempts.append((path,passed))
        if len(attempts)<2: continue
        fold=int(hashlib.sha256(task.encode()).hexdigest()[:8],16)%3
        for path,passed in attempts:
            output=0; text=''; samples=[]
            for line in path.open():
                m=json.loads(line).get('message',{})
                if m.get('role')!='assistant': continue
                output+=int(m.get('usage',{}).get('output',0))
                parts=m.get('content',[])
                if not isinstance(parts,list): continue
                text+='\n'+'\n'.join(p.get('thinking',p.get('text','')) for p in parts if p.get('type') in ('thinking','text'))
                if output<=0 or not any(p.get('type')=='toolCall' for p in parts): continue
                t=min(1.,output/4000)
                u=min(1.,len(MATCH.findall(text))*1000/output/8)
                samples.append((round(t*40)/40,round(u*40)/40))
            if not samples: continue
            # Failure with successful independent siblings is a proxy, not live success probability.
            target=0. if passed else sum(p for other,p in attempts if other!=path)/(len(attempts)-1)
            weight=1/len(samples)
            for t,u in samples:
                key=(fold,t,u); w,y=buckets.get(key,(0.,0.))
                buckets[key]=(w+weight,y+weight*target)
            runs+=1; failures+=not passed; tasks.add(task); checkpoints+=len(samples)
    if not buckets: raise ValueError('no labeled trace prefixes')
    oof=[]
    for fold in range(3):
        training=[(t,u,w,y) for (f,t,u),(w,y) in buckets.items() if f!=fold]
        b,a,c=fit(training)
        oof.extend((sigmoid(b+a*t+c*u),w,y) for (f,t,u),(w,y) in buckets.items() if f==fold)
    positive=sum(y for p,w,y in oof); negative=sum(w-y for p,w,y in oof)
    if not positive or not negative: raise ValueError('both outcomes are required')
    def quality(threshold):
        tp=sum(y for p,w,y in oof if p>=threshold)
        fp=sum(w-y for p,w,y in oof if p>=threshold)
        return .5*(tp/positive+1-fp/negative)
    threshold=max((i/1000 for i in range(1,1000)),key=lambda q:(quality(q),q))
    b,a,c=fit([(t,u,w,y) for (f,t,u),(w,y) in buckets.items()])
    return dict(token_weight=a,hesitation_weight=c,
                threshold=math.log(threshold/(1-threshold))-b,
                output_scale=4000,hesitation_scale=8,words=WORDS,
                calibration=dict(tasks=len(tasks),attempts=runs,failed_attempts=failures,
                    prefixes=checkpoints,folds=3,group='instance_id',
                    proxy='failed attempt times independent sibling solve fraction',
                    regularization=.002,feature_bin_width=.025,
                    threshold_objective='out-of-fold soft-label balanced accuracy',
                    balanced_accuracy=quality(threshold),
                    prefix_trigger_fraction=sum(w for p,w,y in oof if p>=threshold)/sum(w for p,w,y in oof),
                    sources='swebench-run-1..3; swebench-3runs-per-case.csv; excludes sphinx and unevaluated errors',
                    label_sha256=hashlib.sha256(Path(labels).read_bytes()).hexdigest()))


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--trace-root',required=True)
    parser.add_argument('--labels',required=True)
    args=parser.parse_args()
    print(json.dumps(calibrate(args.trace_root,args.labels),indent=2))
