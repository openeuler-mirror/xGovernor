"""Two opt-in scenarios over an installed production E2B/OpenCode distribution."""
import base64
import json
import os
import re
from pathlib import Path
import subprocess
import time
import tempfile
import unittest
import urllib.request


class InstalledScenarios(unittest.TestCase):
    def setUp(self):
        required = ['MCTS_E2E_HOME', 'MCTS_E2E_REQUEST', 'MCTS_E2E_OPENCODE_REQUEST', 'MCTS_E2E_OPENCODE']
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            self.skipTest('real E2B/model installation not configured: ' + ', '.join(missing))
        self.home = Path(os.environ['MCTS_E2E_HOME']).resolve()
        self.env = dict(os.environ, HOME=str(self.home), XDG_CONFIG_HOME=str(self.home / '.config'))
        self.prefix = self.home / '.local/share/mcts'
        self.env['PATH'] = str(self.prefix / 'bin') + ':' + self.env['PATH']
        self.config = json.loads((self.home / '.config/mcts/config.json').read_text())
        self.assertTrue(all(b['driver'] == 'own' for b in self.config['backends']))
        self.assertTrue(self.config.get('swebench_python'), 'official harness Python is required')
        self.timeout = int(os.environ.get('MCTS_E2E_TIMEOUT', '3000'))

    def call(self, *args):
        return json.loads(subprocess.check_output(['mcts', *args], cwd=self.home, env=self.env))

    def terminal(self, run_id):
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            self.call('status', run_id)
            result = self.call('result', run_id)
            if result['run']['status'] in ['completed', 'failed', 'cancelled', 'interrupted']:
                return result
            time.sleep(5)
        self.fail('run did not terminate: ' + run_id)

    def test_command_line_entry(self):
        request = Path(os.environ['MCTS_E2E_REQUEST']).resolve()
        # The start command exits. Its worker lives in an independent persistent
        # unit, so a failed monitor cannot kill execution through its own cgroup.
        unit = 'mcts-e2e-cli-' + str(os.getpid())
        acceptance = self.home / 'acceptance.json'
        acceptance.unlink(missing_ok=True)
        launcher = subprocess.Popen(['systemd-run', '--quiet', '--pipe', '--unit=' + unit,
                        '-p', 'CPUQuota=100%', '-p', 'MemoryMax=3G', '-p', 'MemorySwapMax=0',
                        '-p', 'TasksMax=512', '-p', 'RuntimeMaxSec=' + str(self.timeout),
                        'python3', '-c',
                        'import json,os,subprocess,sys,time; p=json.loads(sys.stdin.readline()); '
                        'os.environ.update(p["env"]); o=subprocess.check_output(p["command"]); '
                        'open(p["acceptance"],"wb").write(o); time.sleep(p["lifetime"])'], stdin=subprocess.PIPE)
        launcher.stdin.write(json.dumps({'env':self.env, 'command':['mcts','run','--request',str(request)],
                                         'acceptance':str(acceptance), 'lifetime':self.timeout + 60}).encode() + b'\n')
        launcher.stdin.close()
        for _ in range(120):
            if acceptance.exists():
                break
            time.sleep(1)
        self.assertTrue(acceptance.exists(), 'launcher did not return durable acceptance')
        run_id = json.loads(acceptance.read_text())['run_id']
        result = self.terminal(run_id)
        subprocess.run(['systemctl','stop',unit],check=True)
        launcher.wait(timeout=30)
        print('CLI_SCENARIO', json.dumps(result), flush=True)
        self.check_delivery(run_id, result)

    def check_delivery(self, run_id, result):
        self.assertEqual(result['run']['status'], 'completed', result)
        self.assertEqual(result['run']['cleanup'], 'complete', result)
        directory = Path(self.config['runs_dir']) / run_id
        for name in ['selected.patch', 'workspace.tar', 'events.jsonl', 'usage.json']:
            self.assertTrue((directory / name).is_file(), name)
        official = list(directory.rglob('official_result.json'))
        self.assertTrue(official, 'missing official evaluation')
        self.assertIn(json.loads(official[0].read_text())['status'], ['passed', 'failed'])
        self.assertEqual(json.loads((directory / 'config.json').read_text())['config']['network_policy'], 'offline')

    def test_opencode_plugin_entry(self):
        subprocess.run(['mcts','install','--target','opencode'],env=self.env,check=True)
        config_dir = self.home / '.config/opencode'
        probe = config_dir / 'plugins/entry-check.js'
        probe.write_text(Path(__file__).with_name('opencode_entry.js').read_text())
        self.addCleanup(probe.unlink)
        evidence = Path(tempfile.mkdtemp(prefix='opencode-entry-', dir=self.home))
        print('OPENCODE_EVIDENCE', str(evidence), flush=True)
        env = dict(self.env, MCTS_ENTRY_DIRECTORY=str(evidence),
                   MCTS_ENTRY_REQUEST=str(Path(os.environ['MCTS_E2E_OPENCODE_REQUEST']).resolve()),
                   OPENCODE_CONFIG_DIR=str(config_dir))
        log = (evidence / 'opencode.log').open('w')
        unit = 'mcts-e2e-opencode-' + str(os.getpid())
        process = subprocess.Popen(['systemd-run', '--quiet', '--pipe', '--unit=' + unit,
                                    '-p','CPUQuota=100%', '-p','MemoryMax=3G', '-p','MemorySwapMax=0',
                                    '-p','TasksMax=512', '-p','RuntimeMaxSec=' + str(self.timeout),
                                    'python3','-c','import json,os,sys; p=json.loads(sys.stdin.readline()); '
                                    'os.environ.update(p["env"]); os.chdir(p["cwd"]); '
                                    'os.execv(p["command"][0],p["command"])'],
                                   stdin=subprocess.PIPE,stdout=log,stderr=log)
        process.stdin.write(json.dumps({'env':env,'cwd':str(self.home),
                                        'command':[os.environ['MCTS_E2E_OPENCODE'],'serve','--hostname','127.0.0.1','--port','18276']}).encode() + b'\n')
        process.stdin.close()
        try:
            deadline = time.monotonic() + self.timeout
            while time.monotonic() < deadline:
                if (evidence / 'failure.txt').exists():
                    self.fail((evidence / 'failure.txt').read_text())
                if (evidence / 'result.json').exists():
                    break
                self.assertIsNone(process.poll(), 'OpenCode exited early')
                match = re.search(r'server password (\S+)', (evidence / 'opencode.log').read_text())
                if match:
                    auth = base64.b64encode(('opencode:' + match[1]).encode()).decode()
                    request = urllib.request.Request('http://127.0.0.1:18276/api/mcp', headers={'Authorization':'Basic ' + auth})
                    try:
                        with urllib.request.urlopen(request, timeout=5) as response:
                            response.read()
                    except OSError:
                        pass
                time.sleep(1)
            self.assertTrue((evidence / 'result.json').exists(), 'OpenCode MCP scenario timed out')
            result = json.loads((evidence / 'result.json').read_text())
            print('PLUGIN_SCENARIO',json.dumps(result),flush=True)
            self.check_delivery(result['run']['run_id'], result)
        finally:
            subprocess.run(["systemctl","stop",unit],check=True)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait()
            log.close()


if __name__ == '__main__':
    unittest.main(verbosity=2)
