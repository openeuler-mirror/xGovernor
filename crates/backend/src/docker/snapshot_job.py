"""Host-side commit receipt writer; survives governor crashes, never invokes a shell."""
import fcntl
import json
import os
from pathlib import Path
import resource
import subprocess
import sys

os.umask(0o077)
root=Path(sys.argv[1])
with (root/'lock').open('a') as lock:
    try:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit(0)
    if (root/'done.json').exists():
        sys.exit(0)
    request=json.loads((root/'request.json').read_text())
    # Only the CLI output files inherit this cap; the independent Engine does not.
    resource.setrlimit(resource.RLIMIT_FSIZE,(1024*1024,1024*1024))
    env=os.environ.copy()
    for key in ('DOCKER_CONTEXT','DOCKER_HOST','DOCKER_TLS_VERIFY','DOCKER_CERT_PATH'):
        env.pop(key,None)
    def durable_json(name, value):
        temp=root/(name+'.tmp')
        with temp.open('w') as out:
            json.dump(value,out);out.flush();os.fsync(out.fileno())
        os.replace(temp,root/name)
        fd=os.open(root,os.O_RDONLY);os.fsync(fd);os.close(fd)

    try:
        boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        prior=root/'started.json'
        with (root/'stdout').open('wb') as out, (root/'stderr').open('wb') as err:
            if prior.exists():
                # Query the stable tag before any possible retry. A writer lost
                # during this boot may have left a live Engine commit behind.
                query=subprocess.run(request['argv'][:3]+['image','inspect',request['argv'][-1]],stdin=subprocess.DEVNULL,stdout=out,stderr=err,env=env,check=False,timeout=30)
                if query.returncode==0:
                    receipt={'exit_code':0,'recovered':True}
                elif json.loads(prior.read_text()).get('boot_id')==boot:
                    receipt={'exit_code':-1,'error':'AmbiguousCommitRetained'}
                else:
                    # A previous-boot daemon cannot still be executing. The
                    # failed inspect must explicitly establish image absence.
                    err.flush()
                    missing=b'No such image' in (root/'stderr').read_bytes()
                    if not missing:raise RuntimeError('ImageAbsenceUnconfirmed')
                    durable_json('started.json',{'boot_id':boot})
                    result=subprocess.run(request['argv'],stdin=subprocess.DEVNULL,stdout=out,stderr=err,env=env,check=False)
                    receipt={'exit_code':result.returncode}
            else:
                durable_json('started.json',{'boot_id':boot})
                result=subprocess.run(request['argv'],stdin=subprocess.DEVNULL,stdout=out,stderr=err,env=env,check=False)
                receipt={'exit_code':result.returncode}
    except Exception as error:
        receipt={'exit_code':-1,'error':type(error).__name__}
    with (root/'done.tmp').open('w') as out:
        json.dump(receipt,out);out.flush();os.fsync(out.fileno())
    os.replace(root/'done.tmp',root/'done.json')
    fd=os.open(root,os.O_RDONLY);os.fsync(fd);os.close(fd)
