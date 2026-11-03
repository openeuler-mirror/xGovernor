"""Portable MCTS tree policy; execution is supplied by HostCapabilities."""
from __future__ import annotations
import os
import math
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import Callable
from .host_api import Clock, HostCapabilities, RunContext, SystemClock

@dataclass
class Node:
    node_id: str
    parent: 'Node | None' = None
    children: list['Node'] = field(default_factory=list)
    depth: int = 0
    value: float = 0.0
    visits: int = 0
    solution: str = ''
    feedback: str = ''
    reflection: str = ''
    pytest_output: str = ''
    query: str = ''
    state_id: str | None = None
    branch_id: str | None = None
    job_id: str | None = None
    oracle_version: int | None = None
    score: float = 0.0
    pending_children: int = 0
    def uct(self, c: float = 1.0) -> float:
        if self.visits == 0: return float('inf')
        if not self.parent or self.parent.visits <= 0: return self.value / self.visits
        return self.value / self.visits + c * math.sqrt(math.log(self.parent.visits) / self.visits)
    def update(self, reward: float) -> None:
        self.visits += 1; self.value += reward

def backprop(node: Node, reward: float) -> None:
    while node: node.update(reward); node = node.parent

def iter_tree(node: Node):
    yield node
    for child in node.children: yield from iter_tree(child)

def count_tree(root: Node) -> int: return sum(1 for _ in iter_tree(root))
def best_node(root: Node) -> Node:
    return max(iter_tree(root), key=lambda n: (n.value / n.visits if n.visits else float('-inf'), n.visits))

@dataclass(frozen=True)
class Expansion:
    score: float
    feedback: str
    output: str = ''
    query: str = ''
    state_id: str | None = None
    oracle_version: int | None = None


def max_concurrent_from_env() -> int:
    """Maximum concurrent expansion workers for plugin runs.

    Defaults to 3 to match the pre-plugin ``MAX_CONCURRENT = 3`` coordinator
    behaviour; set it to 1 only for an explicit serial regression comparison.
    """
    raw = os.environ.get("XIAOO_MCTS_MAX_CONCURRENT", "3").strip()
    if not raw:
        return 3
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(
            "XIAOO_MCTS_MAX_CONCURRENT must be a positive integer, got "
            f"{raw!r}"
        ) from exc
    if value <= 0:
        raise RuntimeError(
            "XIAOO_MCTS_MAX_CONCURRENT must be a positive integer, got "
            f"{raw!r}"
        )
    return value


@dataclass
class _Job:
    parent: Node
    child: Node


class Coordinator:
    def __init__(self, root: Node, *, host: HostCapabilities, run_context: RunContext | None = None,
                 step: Callable[[Node, Node, HostCapabilities], Expansion], max_expansions: int,
                 branching: int = 1, clock: Clock | None = None, timeout_ms: int = 30_000,
                 should_stop=None, max_concurrent: int = 1, run: RunContext | None = None):
        run_context = run_context or run
        if run_context is None: raise ValueError("run_context is required")
        self.root, self.host, self.run_context, self.step = root, host, run_context, step
        self.max_expansions, self.branching = max_expansions, branching
        self.clock, self.timeout_ms = clock or SystemClock(), timeout_ms
        if max_concurrent <= 0:
            raise ValueError("max_concurrent must be positive")
        self.max_concurrent = max_concurrent
        self.expansion_count = 0
        self.should_stop = should_stop or (lambda: False)

    def run(self) -> Node:
        self.root.state_id = self.root.state_id or self.run_context.root_state_id
        if self.max_concurrent > 1:
            return self._run_concurrent()
        return self._run_serial()

    def _run_serial(self) -> Node:
        for _ in range(self.max_expansions):
            if self.run_context.cancelled or self.should_stop(): break
            parent = self._select_leaf(self.root)
            if parent is None or not parent.state_id: break
            for i in range(self.branching):
                child = Node(f'{parent.node_id}/c{len(parent.children)}', parent=parent, depth=parent.depth + 1)
                branch = self.host.fork(parent.state_id, timeout_ms=self.timeout_ms, idempotency_key=f'{self.run_context.run_id}:{child.node_id}:fork')
                child.branch_id = branch['branch_id']
                try:
                    exp = self.step(parent, child, self.host)
                    snap = ({'state_id': exp.state_id} if exp.state_id else
                            self.host.snapshot(child.branch_id, timeout_ms=self.timeout_ms, idempotency_key=f'{self.run_context.run_id}:{child.node_id}:snapshot'))
                    child.score = exp.score
                    child.state_id, child.feedback = snap['state_id'], exp.feedback
                    child.solution, child.query = exp.output, exp.query
                    child.oracle_version = exp.oracle_version
                    parent.children.append(child); backprop(child, exp.score)
                finally:
                    self.host.release(child.branch_id, timeout_ms=self.timeout_ms, idempotency_key=f'{self.run_context.run_id}:{child.node_id}:release')
            self.expansion_count += 1
        return best_node(self.root)

    def _run_concurrent(self) -> Node:
        queue: deque[_Job] = deque()
        futures: dict[Future, _Job] = {}
        first_error: BaseException | None = None
        pool = ThreadPoolExecutor(
            max_workers=self.max_concurrent,
            thread_name_prefix="mcts-expand",
        )
        try:
            while True:
                if first_error is None and (self.run_context.cancelled or self.should_stop()):
                    queue.clear()

                self._pump(pool, queue, futures)
                if not futures and not queue:
                    if first_error is None and self._reserve(queue):
                        continue
                    break
                if not futures:
                    if first_error is not None:
                        break
                    continue

                done, _ = wait(tuple(futures), return_when=FIRST_COMPLETED)
                for future in done:
                    job = futures.pop(future)
                    try:
                        result = future.result()
                    except BaseException as exc:
                        if first_error is None:
                            first_error = exc
                        job.parent.pending_children = max(
                            0, job.parent.pending_children - 1
                        )
                        continue
                    self._attach_completed(job, result)
                    job.parent.pending_children = max(
                        0, job.parent.pending_children - 1
                    )
        finally:
            pool.shutdown(wait=True, cancel_futures=True)
        if first_error is not None:
            raise first_error
        return best_node(self.root)

    def _reserve(self, queue: deque[_Job]) -> bool:
        if self.run_context.cancelled or self.should_stop():
            return False
        if self.expansion_count >= self.max_expansions:
            return False
        parent = self._select_leaf(self.root)
        if parent is None or not parent.state_id:
            return False
        parent.pending_children += self.branching
        for i in range(self.branching):
            child = Node(
                f'{parent.node_id}/c{len(parent.children) + i}',
                parent=parent,
                depth=parent.depth + 1,
            )
            queue.append(_Job(parent, child))
        self.expansion_count += 1
        return True

    def _pump(
        self,
        pool: ThreadPoolExecutor,
        queue: deque[_Job],
        futures: dict[Future, _Job],
    ) -> None:
        while len(futures) < self.max_concurrent and queue:
            job = queue.popleft()
            futures[pool.submit(self._execute_job, job)] = job

    def _execute_job(self, job: _Job):
        parent, child = job.parent, job.child
        branch = self.host.fork(
            parent.state_id,
            timeout_ms=self.timeout_ms,
            idempotency_key=f'{self.run_context.run_id}:{child.node_id}:fork',
        )
        child.branch_id = branch['branch_id']
        try:
            exp = self.step(parent, child, self.host)
            snap = (
                {'state_id': exp.state_id}
                if exp.state_id
                else self.host.snapshot(
                    child.branch_id,
                    timeout_ms=self.timeout_ms,
                    idempotency_key=f'{self.run_context.run_id}:{child.node_id}:snapshot',
                )
            )
            return job, exp, snap
        finally:
            self.host.release(
                child.branch_id,
                timeout_ms=self.timeout_ms,
                idempotency_key=f'{self.run_context.run_id}:{child.node_id}:release',
            )

    def _attach_completed(self, job: _Job, result) -> None:
        parent, child = job.parent, job.child
        _, exp, snap = result
        child.score = exp.score
        child.state_id, child.feedback = snap['state_id'], exp.feedback
        child.solution, child.query = exp.output, exp.query
        child.oracle_version = exp.oracle_version
        parent.children.append(child)
        backprop(child, exp.score)

    def _select_leaf(self, node: Node) -> Node | None:
        if node.pending_children > 0:
            return None
        if node.children:
            for child in sorted(node.children, key=lambda item: item.uct(), reverse=True):
                selected = self._select_leaf(child)
                if selected is not None:
                    return selected
            return None
        return node
HostCoordinator = Coordinator
