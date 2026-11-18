import json
import os
import stat
import sys
import tarfile
import tempfile

limit = int(sys.argv[1])
include_vcs = sys.argv[2] == 'true'
fd, path = tempfile.mkstemp(prefix='xgovernor-workspace-', suffix='.tar')
count = 0

class TooLarge(Exception):
    pass

class Bounded:
    def __init__(self, output):
        self.output = output
        self.size = 0

    def write(self, data):
        if self.size + len(data) > limit:
            raise TooLarge()
        self.size += len(data)
        return self.output.write(data)

try:
    with os.fdopen(fd, 'wb') as output:
        with tarfile.open(fileobj=Bounded(output), mode='w|', dereference=False) as archive:
            for parent, dirs, files in os.walk('.', followlinks=False):
                dirs.sort()
                files.sort()
                if not include_vcs:
                    dirs[:] = [d for d in dirs if d != '.git']
                    files = [f for f in files if f != '.git']
                for name in dirs + files:
                    source = os.path.join(parent, name)
                    relative = os.path.relpath(source, '.')
                    mode = os.lstat(source).st_mode
                    if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode) or stat.S_ISLNK(mode)):
                        raise RuntimeError('unsupported workspace file type: ' + relative)
                    archive.add(source, arcname=relative, recursive=False)
                    count += int(stat.S_ISREG(mode))
    print(json.dumps({'path': path, 'files': count, 'size': os.path.getsize(path)}))
except BaseException as error:
    os.unlink(path)
    if isinstance(error, TooLarge):
        sys.exit(42)
    raise
