#!/usr/bin/env python3
"""Private stdin/JSON protocol. Runs exclusively inside the tool container."""
import base64
import glob
import json
import os
import selectors
import signal
import stat
import subprocess
import sys
import tempfile
import time

MAX_BYTES = 4 * 1024 * 1024
PROTOCOL = 2
MAX_REQUEST = 128 * 1024 * 1024 + 1024 * 1024

def limit(q, key, default):
    value = q.get('limits', {}).get(key, default)
    if not isinstance(value, int) or value < 1:
        raise ValueError('invalid limit: ' + key)
    return value



def b64(data):
    return base64.b64encode(data).decode('ascii')


def execute(q):
    timeout = q.get('timeout_ms', limit(q, 'timeout_ms', 60000))
    if not 1 <= timeout <= limit(q, 'max_timeout_ms', 3600000):
        raise ValueError('timeout_ms must be between 1 and 3600000')
    if q.get('shell'):
        if q.get('args'):
            raise ValueError('shell mode does not accept args')
        argv = [q['shell'], '-c', q['command']]
    else:
        argv = [q['command'], *q.get('args', [])]
    env = os.environ.copy()
    env.update(q.get('env') or {})
    p = subprocess.Popen(argv, cwd=q.get('cwd') or '/workspace', env=env,
                         stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, start_new_session=True)
    chunks = {'stdout': bytearray(), 'stderr': bytearray()}
    sel = selectors.DefaultSelector()
    sel.register(p.stdout, selectors.EVENT_READ, 'stdout')
    sel.register(p.stderr, selectors.EVENT_READ, 'stderr')
    deadline = time.monotonic() + timeout / 1000
    timed_out = False
    overflow = False
    try:
        while sel.get_map():
            if time.monotonic() >= deadline:
                timed_out = True
                break
            for key, _ in sel.select(min(0.1, max(0, deadline - time.monotonic()))):
                data = os.read(key.fileobj.fileno(), 65536)
                if not data:
                    sel.unregister(key.fileobj)
                    continue
                chunks[key.data].extend(data)
                if sum(map(len, chunks.values())) > limit(q, 'output_bytes', MAX_BYTES):
                    overflow = True
                    break
            if overflow:
                break
        if not timed_out and not overflow:
            try:
                p.wait(timeout=max(0.001, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                timed_out = True
    finally:
        # Also retire ordinary descendants left behind by a command that exited.
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        p.wait()
        sel.close()
        p.stdout.close()
        p.stderr.close()
    if overflow:
        raise ValueError('combined command output exceeds configured limit')
    return dict(stdout=b64(chunks['stdout']), stderr=b64(chunks['stderr']),
                exit_code=p.returncode, timed_out=timed_out)


def handle(q):
    if q.get('protocol', PROTOCOL) != PROTOCOL:
        raise ValueError('incompatible helper protocol')
    op = q['op']
    if op == 'hello':
        return dict(protocol=PROTOCOL, uid=os.getuid())
    path = q.get('path', '')
    if op == 'exec':
        return execute(q)
    if op == 'resolve':
        return os.path.normpath(os.path.join(q['base'], os.path.expanduser(path)))
    if op == 'stat':
        try:
            s = os.lstat(path)
        except FileNotFoundError:
            return dict(exists=False)
        kind = ('symlink' if stat.S_ISLNK(s.st_mode) else 'directory' if stat.S_ISDIR(s.st_mode)
                else 'file' if stat.S_ISREG(s.st_mode) else 'other')
        return dict(exists=True, kind=kind, size=s.st_size)
    if op == 'read':
        with open(path, 'rb') as f:
            data = f.read(limit(q, 'file_bytes', MAX_BYTES) + 1)
        if len(data) > limit(q, 'file_bytes', MAX_BYTES):
            raise ValueError('file exceeds configured limit')
        return b64(data)
    if op == 'write':
        data = base64.b64decode(q['content'], validate=True)
        if len(data) > limit(q, 'file_bytes', MAX_BYTES):
            raise ValueError('file exceeds configured limit')
        exists = os.path.lexists(path)
        if q['mode'] == 'atomic':
            name = None
            try:
                with tempfile.NamedTemporaryFile(dir=os.path.dirname(path), delete=False) as f:
                    name = f.name
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                if exists:
                    os.chmod(name, stat.S_IMODE(os.stat(path).st_mode))
                os.replace(name, path)
            finally:
                if name and os.path.exists(name):
                    os.unlink(name)
        else:
            with open(path, 'xb' if q['mode'] == 'create' else 'wb') as f:
                f.write(data)
        return dict(created=not exists)
    if op == 'mkdir':
        os.makedirs(path, exist_ok=True)
        return None
    if op == 'temp':
        opts = dict(dir=q.get('parent') or '/tmp', prefix=q.get('prefix') or 'xg-', suffix=q.get('suffix') or '')
        if q['kind'] == 'directory':
            return tempfile.mkdtemp(**opts)
        fd, name = tempfile.mkstemp(**opts)
        os.close(fd)
        return name
    if op == 'glob':
        import itertools
        count = q.get('limit')
        ceiling = limit(q, 'search_entries', 1000)
        if count is None: count = ceiling
        if not isinstance(count, int) or not 0 <= count <= ceiling:
            raise ValueError('search limit outside configured bounds')
        pattern = os.path.join(q['base'], q['pattern'])
        return list(itertools.islice(glob.iglob(pattern, recursive=True, include_hidden=True), count))
    if op == 'grep':
        count = q.get('limit')
        ceiling = limit(q, 'search_entries', 1000)
        if count is None: count = ceiling
        if not isinstance(count, int) or not 0 <= count <= ceiling:
            raise ValueError('search limit outside configured bounds')
        argv = ['rg', '--no-heading', '--color', 'never', '--hidden']
        argv += {'files': ['-l'], 'count': ['-c'], 'content': ['-n']}[q['mode']]
        if q.get('include'):
            argv += ['--glob', q['include']]
        argv += ['--', q['query'], q['base']]
        result = execute(dict(command=argv[0], args=argv[1:], timeout_ms=min(30000, limit(q, 'max_timeout_ms', 3600000)), limits=q.get('limits', {})))
        if result['timed_out'] or result['exit_code'] not in (0, 1):
            raise ValueError('grep failed: ' + base64.b64decode(result['stderr']).decode(errors='replace'))
        return base64.b64decode(result['stdout']).decode(errors='replace').splitlines()[:count]
    raise ValueError('unsupported operation: ' + op)


def main():
    try:
        raw = sys.stdin.buffer.read(MAX_REQUEST + 1)
        if len(raw) > MAX_REQUEST:
            raise ValueError('request too large')
        result = dict(ok=True, value=handle(json.loads(raw)))
    except Exception as e:
        code = {FileNotFoundError: 'not_found', PermissionError: 'permission_denied',
                FileExistsError: 'already_exists', NotADirectoryError: 'not_directory',
                IsADirectoryError: 'not_file'}.get(type(e), 'failed')
        result = dict(ok=False, code=code, message=str(e))
    print(json.dumps(result))


if __name__ == '__main__':
    main()
