from __future__ import annotations

import threading
import time
import unittest
from unittest import mock

from xgovernor_mcts.engine.retest_gate import RetestGate, retest_gate_enabled


class RetestGateTests(unittest.TestCase):
    def test_concurrent_same_key_runs_once_and_coalesces(self) -> None:
        gate = RetestGate()
        calls: list[int] = []
        lock = threading.Lock()
        barrier = threading.Barrier(2)

        def compute() -> tuple[float, str]:
            with lock:
                calls.append(1)
            time.sleep(0.05)
            return (1.0, "evidence")

        results: list[tuple[float, str]] = []

        def worker() -> None:
            barrier.wait()
            results.append(gate.run("state-a", 3, compute))

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(calls, [1])
        self.assertEqual([score for score, _ in results], [1.0, 1.0])
        self.assertEqual([evidence for _, evidence in results], ["evidence", "evidence"])
        self.assertEqual(gate.stats(), {"computes": 1, "coalesced": 1})

    def test_different_keys_are_not_coalesced(self) -> None:
        gate = RetestGate()
        calls: list[tuple[str, int | None]] = []

        def compute() -> tuple[float, str]:
            calls.append(("unused", None))
            return (0.0, "")

        gate.run("state-a", 1, compute)
        gate.run("state-a", 2, compute)
        gate.run("state-b", 1, compute)

        self.assertEqual(len(calls), 3)
        self.assertEqual(gate.stats(), {"computes": 3, "coalesced": 0})

    def test_owner_exception_propagates_to_waiters(self) -> None:
        gate = RetestGate()
        barrier = threading.Barrier(2)
        errors: list[str] = []

        def compute() -> tuple[float, str]:
            time.sleep(0.05)
            raise RuntimeError("boom")

        def worker() -> None:
            barrier.wait()
            try:
                gate.run("state-a", 1, compute)
            except RuntimeError as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, ["boom", "boom"])
        self.assertEqual(gate.stats()["coalesced"], 1)

    @mock.patch.dict("os.environ", {"XIAOO_RETEST_CACHE": "0"})
    def test_env_disables_gate(self) -> None:
        self.assertFalse(retest_gate_enabled())


if __name__ == "__main__":
    unittest.main()
