import threading
import time
import unittest

from xgovernor_mcts.engine.mcts_protocol import ProtocolError, notification
from xgovernor_mcts.bridge.mock_host import FakeHostCapabilities
from xgovernor_mcts.bridge.host_client import RpcHostCapabilities
from xgovernor_mcts.bridge.job_registry import JobRegistry
from xgovernor_mcts.bridge.lifecycle import RunRegistry
from xgovernor_mcts.bridge.rpc_transport import MockTransport, RpcPeer


def init_params():
    return {"plugin_id": "mcts", "supported_api_versions": ["mcts.rpc.v1"], "host_version": "fake", "available_capabilities": ["snapshot", "fork", "agent_turn", "inspect", "exec", "file_read", "file_write", "diff", "release"]}


class P1MockTests(unittest.TestCase):
    def test_lifecycle_idempotency_and_terminal_wait(self):
        registry = RunRegistry()
        registry.initialize(init_params())
        params = {"plugin_id": "mcts", "run_id": "r1", "session_id": "s", "turn_id": "t", "root_state_id": "state-root", "goal": "fix", "content": "ctx", "budget": {"max_iters": 5, "branching": 3, "max_time_ms": 1, "max_tokens": 1}, "granted_capabilities": ["snapshot", "fork", "agent_turn"]}
        self.assertEqual(registry.start(params)["status"], "accepted")
        self.assertEqual(registry.start(dict(params))["run_id"], "r1")
        registry.emit("plugin.started", {"plugin_id": "mcts", "run_id": "r1", "sequence": 1, "summary": "running"})
        self.assertEqual(registry.wait("r1")["status"], "running")
        with self.assertRaises(ProtocolError): registry.start({**params, "goal": "different"})

    def test_job_ordering_dedup_and_terminal_guard(self):
        jobs = JobRegistry(); jobs.accept("r1", "j1")
        started = {"run_id": "r1", "job_id": "j1", "sequence": 1}
        jobs.apply("host.job.started", started)
        jobs.apply("host.job.started", started)
        jobs.apply("host.job.completed", {"run_id": "r1", "job_id": "j1", "sequence": 2, "output": "ok", "usage": {}})
        self.assertEqual(jobs.get("r1", "j1").status, "completed")
        with self.assertRaises(ProtocolError): jobs.apply("host.job.failed", {"run_id": "r1", "job_id": "j1", "sequence": 3, "failure": {"code": "x", "message": "bad"}, "usage": {}})

    def test_fake_host_capability_and_file_mock(self):
        host = FakeHostCapabilities(granted={"file_write", "file_read"})
        host.file_write("branch-root", "/a", b"abc", timeout_ms=1, idempotency_key="w1")
        self.assertEqual(host.file_read("branch-root", "/a", timeout_ms=1), b"abc")
        with self.assertRaises(ProtocolError): host.exec("branch-root", ["true"], cwd="/", timeout_ms=1, idempotency_key="e1")

    def test_rpc_peer_dispatches_notification_while_waiting(self):
        transport = MockTransport(); seen = []
        peer = RpcPeer(transport, on_notification=seen.append)
        def respond():
            request = transport.take_sent(0.1)
            transport.inject(notification("host.job.started", {"run_id": "r1", "job_id": "j1", "sequence": 1}))
            transport.inject({"jsonrpc": "2.0", "id": request["id"], "result": {"job_id": "j1", "status": "accepted"}})
        import threading
        threading.Thread(target=respond).start()
        response = peer.call("host.agent_turn", {"plugin_id": "mcts", "run_id": "r1", "timeout_ms": 1, "branch_id": "b1", "input": "go", "idempotency_key": "i1"}, timeout=1)
        self.assertEqual(response["result"]["status"], "accepted")
        self.assertEqual(seen[0]["method"], "host.job.started")

    def test_rpc_peer_concurrent_calls_route_reversed_responses(self):
        transport = MockTransport()
        peer = RpcPeer(transport, external_dispatch=True)
        count = 20
        results = [None] * count

        def worker(index):
            response = peer.call(
                "host.agent_turn",
                {
                    "plugin_id": "mcts",
                    "run_id": "r1",
                    "branch_id": f"b{index}",
                    "input": "go",
                    "idempotency_key": f"i{index}",
                    "timeout_ms": 1000,
                },
                timeout=2,
            )
            results[index] = response["result"]["job_id"]

        threads = [
            threading.Thread(target=worker, args=(index,))
            for index in range(count)
        ]
        for thread in threads:
            thread.start()

        requests = []
        for _ in range(count):
            request = transport.take_sent(2.0)
            self.assertIsNotNone(request)
            requests.append(request)
        # Answer in reverse order to prove each waiting call is matched by id.
        for request in reversed(requests):
            peer.dispatch(
                {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": {
                        "job_id": f"job-{request['params']['branch_id']}",
                        "status": "accepted",
                    },
                }
            )

        for thread in threads:
            thread.join()
        self.assertEqual(results, [f"job-b{index}" for index in range(count)])

    def test_rpc_host_capabilities_encode_and_consume_mock_messages(self):
        transport = MockTransport()
        jobs = JobRegistry()
        client = RpcHostCapabilities(transport, plugin_id="mcts", run_id="r1", granted={"file_write", "file_read", "agent_turn"}, jobs=jobs)

        def respond():
            request = transport.take_sent(0.1)
            self.assertEqual(request["method"], "host.file_write")
            transport.inject({"jsonrpc": "2.0", "id": request["id"], "result": {"written_bytes": 3}})

        import threading
        threading.Thread(target=respond).start()
        self.assertEqual(client.file_write("b1", "/a", b"abc", timeout_ms=10, idempotency_key="w1"), 3)
        self.assertEqual(transport.sent[0]["params"]["content_base64"], "YWJj")


if __name__ == "__main__": unittest.main()

class LifecycleServerMockTests(unittest.TestCase):
    def test_stdio_lifecycle_dispatch(self):
        from xgovernor_mcts.bridge.server import LifecycleServer
        from xgovernor_mcts.engine.mcts_protocol import request

        class StubExecutor:
            """Lifecycle-only runner; no benchmark or model work is performed."""
            def __init__(self, params, transport, emit, *, strategy=None):
                import threading
                self.emit = emit
                self.thread = threading.Thread(
                    target=lambda: emit("plugin.started", {"summary": "mock running"}),
                    daemon=True,
                )
                self.host = type("Host", (), {"peer": None})()

            def cancel(self, reason):
                return None

        transport = MockTransport()
        server = LifecycleServer(transport, executor_factory=StubExecutor)
        caps = init_params()['available_capabilities']
        init = server.handle(request(1, 'plugin.initialize', {**init_params()}))
        self.assertEqual(init['result']['api_version'], 'mcts.rpc.v1')
        start_params = {'plugin_id':'mcts','run_id':'r1','session_id':'s','turn_id':'t','root_state_id':'state-root','goal':'g','content':'','budget':{'max_iters':5,'branching':3,'max_time_ms':1,'max_tokens':1},'granted_capabilities':caps}
        self.assertEqual(server.handle(request(2,'plugin.start',start_params))['result']['status'],'accepted')
        self.assertIsNone(server.handle(request(3,'plugin.wait',{'plugin_id':'mcts','run_id':'r1','after_sequence':0,'timeout_ms':0})))
        wait = None
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            message = transport.take_sent(0.1)
            if message is None:
                continue
            if message.get('id') == 3:
                wait = message
                break
        self.assertIsNotNone(wait)
        self.assertIn(wait['result']['status'], {'accepted', 'running'})
        self.assertEqual(server.handle(request(4,'plugin.shutdown',{'plugin_id':'mcts','grace_period_ms':0}))['result']['status'],'stopped')


class MockLifecycleExecutorTests(unittest.TestCase):
    def test_mock_executor_emits_started_and_completed(self):
        from xgovernor_mcts.bridge.mock_lifecycle import MockRunExecutor

        params = {
            'plugin_id': 'mcts',
            'run_id': 'mock-run-1',
            'root_state_id': 'state-root',
            'budget': {'max_iters': 1, 'branching': 1, 'max_time_ms': 1000, 'max_tokens': 1000},
        }
        events: list[tuple[str, dict]] = []
        executor = MockRunExecutor(params, None, lambda method, payload: events.append((method, payload)))
        executor.run()

        self.assertEqual(events[0][0], 'plugin.started')
        self.assertEqual(events[-1][0], 'plugin.completed')
        self.assertEqual(events[-1][1]['result']['status'], 'completed')
