def run_mock():
    from src.core.host_api import RunContext
    from src.core.mcts_coordinator import Node, Coordinator, Expansion
    from src.core.result_builder import ResultBuilder
    from .mock_host import FakeHostCapabilities
    host=FakeHostCapabilities(); run=RunContext('mcts','mock-run','state-root',set(host.granted)); root=Node('root',state_id='state-root')
    def step(parent,child,h): child.job_id=h.agent_turn(child.branch_id,'expand',timeout_ms=1000,idempotency_key=child.node_id)['job_id']; return Expansion(0,'0')
    best=Coordinator(root,host=host,run_context=run,step=step,max_expansions=1).run()
    return ResultBuilder(root_state_id=run.root_state_id).build(answer=best.solution,summary='completed',stop_reason='search_exhausted')
