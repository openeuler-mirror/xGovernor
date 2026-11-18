from __future__ import annotations

import json
import time
import unittest

from xgovernor_mcts.memory import MemoryPool
from xgovernor_mcts.memory.merger import SemanticMerger
from xgovernor_mcts.bridge.memory_host import HostMergeClient


class FakeHost:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def fork(self, parent_state_id, *, timeout_ms, idempotency_key, offline=False):
        self.calls.append(("fork", {"parent": parent_state_id, "key": idempotency_key}))
        return {"branch_id": "branch-1"}

    def agent_turn(self, branch_id, text, *, timeout_ms, idempotency_key, profile=None):
        self.calls.append(
            (
                "agent_turn",
                {
                    "branch_id": branch_id,
                    "text": text,
                    "profile": profile,
                    "key": idempotency_key,
                },
            )
        )
        return {"job_id": "job-1"}

    def release(self, branch_id, *, timeout_ms, idempotency_key):
        self.calls.append(("release", {"branch_id": branch_id, "key": idempotency_key}))


class HostMergeClientTests(unittest.TestCase):
    def test_complete_sends_user_message_and_releases_branch(self) -> None:
        host = FakeHost()
        wait_job = lambda receipt: {"output": "{}"}
        client = HostMergeClient(
            host=host,
            wait_job=wait_job,
            run_id="run-1",
            author_state_id=lambda: "author-state",
            profile="memory-merge",
            timeout_ms=lambda: 30000,
        )

        output = client.complete(
            [
                {"role": "system", "content": "system prompt"},
                {"role": "user", "content": "merge payload"},
            ]
        )

        self.assertEqual(output, "{}")
        agent_calls = [call for call in host.calls if call[0] == "agent_turn"]
        self.assertEqual(len(agent_calls), 1)
        self.assertEqual(agent_calls[0][1]["text"], "merge payload")
        self.assertEqual(agent_calls[0][1]["profile"], "memory-merge")
        self.assertEqual(host.calls[-1][0], "release")


def _query(kind: str) -> str:
    return (
        "<query>\n"
        f'<item kind="{kind}">\n'
        "test_name: check_x\n"
        "problem: check is too strict\n"
        "fix_request: require a concrete ASan signature\n"
        "evidence: two false positives\n"
        "</item>\n"
        "</query>"
    )


def _dedup_complete(messages: list[dict[str, str]]) -> str:
    payload = json.loads(messages[-1]["content"])
    inputs = payload["inputs"]
    item = inputs[0]["item"]
    return json.dumps(
        {
            "groups": [
                {
                    "source_ids": [entry["source_id"] for entry in inputs],
                    "item": item,
                }
            ]
        }
    )


class MemoryPoolIntegrationTests(unittest.TestCase):
    def test_merge_exhaustion_preserves_every_finding(self) -> None:
        always_invalid = lambda messages: "completion content is not valid JSON"
        pool = MemoryPool(_merger=SemanticMerger(always_invalid, backoff_secs=0.0))
        try:
            self.assertTrue(pool.add(_query("test_bug")).success)
            self.assertTrue(pool.add(_query("test_bug")).success)
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and pool.is_merge_pending():
                time.sleep(0.01)
            self.assertFalse(pool.is_merge_pending())
            snapshot = json.loads(pool.fetch())
            self.assertEqual(len(snapshot), 1)
            items = list(snapshot.values())[0]
            self.assertEqual(len(items), 2)
            self.assertTrue(all(item["kind"] == "test_bug" for item in items))
        finally:
            pool.close(cancel_pending=True)

    def test_identical_test_bug_findings_merge_to_one_item(self) -> None:
        pool = MemoryPool(_merger=SemanticMerger(_dedup_complete))
        try:
            self.assertTrue(pool.add(_query("test_bug")).success)
            self.assertTrue(pool.add(_query("test_bug")).success)
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and pool.is_merge_pending():
                time.sleep(0.01)
            self.assertFalse(pool.is_merge_pending())
            snapshot = json.loads(pool.fetch())
            self.assertEqual(len(snapshot), 1)
            items = list(snapshot.values())[0]
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["kind"], "test_bug")
        finally:
            pool.close(cancel_pending=True)


if __name__ == "__main__":
    unittest.main()


class _OneShotMemory:
    def __init__(self) -> None:
        self.fetch_called = False

    def has_unfetched(self) -> bool:
        return not self.fetch_called

    def is_merge_pending(self) -> bool:
        return False

    def fetch(self) -> str:
        self.fetch_called = True
        return '{"v1":[]}'

    def close(self, *, cancel_pending: bool) -> None:
        pass


class _OneShotStrategy:
    def __init__(self) -> None:
        self._memory = _OneShotMemory()
        self.consumed = 0

    def _consume_memory(self, executor) -> None:
        self._memory.fetch()
        self.consumed += 1


class _WorkerExecutor:
    def __init__(self) -> None:
        self.context = type("Context", (), {"run_id": "worker-run"})()


class AuthorWorkerTests(unittest.TestCase):
    def test_worker_consumes_settled_memory_snapshot(self) -> None:
        strategy = _OneShotStrategy()
        executor = _WorkerExecutor()
        from xgovernor_mcts.bridge.memory_host import AuthorWorker

        worker = AuthorWorker(strategy, executor, poll_interval=0.01)
        worker.start()
        worker.wake()
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline and strategy.consumed == 0:
            time.sleep(0.01)
        worker.stop(timeout=1.0)
        self.assertEqual(strategy.consumed, 1)


class AuthorSyncDedupTests(unittest.TestCase):
    def test_two_identical_queries_trigger_one_author_sync(self) -> None:
        from xgovernor_mcts.engine.executor import BenchmarkStrategy
        from xgovernor_mcts.bridge.memory_host import AuthorWorker

        strategy = BenchmarkStrategy(task=None, scorer=lambda *a, **k: (0.0, ""))
        pool = MemoryPool(_merger=SemanticMerger(_dedup_complete))
        strategy._memory = pool

        sync_calls: list[str] = []
        strategy._sync_author = lambda executor, query: sync_calls.append(query)

        try:
            self.assertTrue(pool.add(_query("test_bug")).success)
            self.assertTrue(pool.add(_query("test_bug")).success)

            # Wait until merge workers have settled both batches, then let the
            # Author worker fetch exactly one stable version.
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and (
                pool.is_merge_pending() or not pool.has_unfetched()
            ):
                time.sleep(0.01)
            self.assertFalse(pool.is_merge_pending())
            self.assertTrue(pool.has_unfetched())

            executor = _WorkerExecutor()
            worker = AuthorWorker(strategy, executor, poll_interval=0.01)
            worker.start()
            worker.wake()

            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and len(sync_calls) < 1:
                time.sleep(0.01)
            time.sleep(0.1)
            worker.stop(timeout=1.0)

            self.assertEqual(len(sync_calls), 1)

            # The single delta should carry exactly one merged canonical item.
            delta = json.loads(sync_calls[0])
            self.assertEqual(len(delta.get("v1", [])), 1)
        finally:
            pool.close(cancel_pending=True)


if __name__ == "__main__":
    unittest.main()
