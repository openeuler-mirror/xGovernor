"""Supervise one tool process tree; only a clean tree gets an exit status."""
import ctypes
import os
import signal
import subprocess
import sys
import time

stopping = False


def cancel(_signal, _frame):
    global stopping
    stopping = True


signal.signal(signal.SIGTERM, cancel)
# Adopt descendants that detach from the command's initial process group.
if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
    os.kill(os.getpid(), signal.SIGKILL)
os.write(1, bytes.fromhex(sys.argv[1]))
child = None
code = 143
timeout_ms = int(sys.argv[2])
try:
    if not stopping:
        child = subprocess.Popen(sys.argv[3:], start_new_session=True)
        deadline = time.monotonic() + timeout_ms / 1000 if timeout_ms else None
        while not stopping:
            if deadline is not None and time.monotonic() >= deadline:
                code = 124
                break
            result = os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            if result is not None:
                code = result.si_status if result.si_code == os.CLD_EXITED else -result.si_status
                break
            time.sleep(0.01)
except OSError as error:
    print(str(error), file=sys.stderr)
    code = 127
finally:
    # The group was created by this supervisor; adopted children are read from
    # the kernel's own child list, never discovered by name or workspace scans.
    try:
        # Keep the leader unreaped until signalling its group, preventing PID reuse.
        if child is not None:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + 5
        children_path = f"/proc/self/task/{os.getpid()}/children"
        while True:
            children = [int(pid) for pid in open(children_path).read().split()]
            for pid in children:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            while True:
                try:
                    pid, _status = os.waitpid(-1, os.WNOHANG)
                    if pid == 0:
                        break
                except ChildProcessError:
                    break
            if not open(children_path).read().strip():
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("process tree cleanup did not finish")
            time.sleep(0.01)
        if child is not None:
            child.returncode = code
    except BaseException:
        # A killed supervisor has no trustworthy cleanup acknowledgement.
        os.kill(os.getpid(), signal.SIGKILL)
        raise

if code < 0:
    code = 128 - code
sys.exit(code)
