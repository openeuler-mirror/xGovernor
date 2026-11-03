"""MCTS plugin process entry point.

Task selection is process configuration because ``plugin.start`` is strict v1
wire data and intentionally has no benchmark-specific mode field.
"""
from __future__ import annotations
import argparse
from src.plugin.stdio_transport import StdioTransport
from src.plugin.server import LifecycleServer
from src.plugin.lifecycle import RunRegistry
from src.plugin.executor import BenchmarkStrategy
from src.res.mode import MODES

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--mock', action='store_true')
    parser.add_argument('--mode', choices=sorted(MODES.all()))
    preliminary, _ = parser.parse_known_args(argv)
    if preliminary.mock:
        from src.plugin.mock_lifecycle import serve_mock_stdio
        serve_mock_stdio()
        return
    if not preliminary.mode:
        parser.error('--mode is required for a production benchmark server')
    spec = MODES.get(preliminary.mode)
    if spec.configure_cli:
        spec.configure_cli(parser)
    args = parser.parse_args(argv)
    task = spec.loader()(spec, **vars(args))
    scorer = spec.scorer_fn()
    def strategy_factory(params):
        return BenchmarkStrategy(task, scorer, oracle=getattr(task, 'oracle_host_path', None), evaluation_version=f'{spec.id}:{spec.scorer}:v1')
    LifecycleServer(StdioTransport(), RunRegistry(), strategy_factory=strategy_factory).serve_forever()
if __name__=='__main__': main()
