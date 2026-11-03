"""JSON-RPC v1 lifecycle server with real background MCTS execution."""
from __future__ import annotations
import threading
import sys
from src.core.mcts_protocol import error, result, ProtocolError
from .lifecycle import RunRegistry
from .executor import RunExecutor

class LifecycleServer:
    def __init__(self, transport, registry=None, executor_factory=None, strategy_factory=None):
        self.transport=transport; self.registry=registry or RunRegistry(); self.running=True
        self.executors={}; self.executor_factory=executor_factory or RunExecutor
        self.strategy_factory = strategy_factory
        self._send_lock=threading.Lock()
    def _emit(self, run_id, method, payload):
        run=self.registry.runs[run_id]
        params={'plugin_id':self.registry.manifest.plugin_id,'run_id':run_id,'sequence':run.sequence+1,**payload}
        event=self.registry.emit(method, params)
        with self._send_lock: self.transport.send(event)
    def _make_executor(self, params):
        emit = lambda method,payload: self._emit(params['run_id'],method,payload)
        if self.strategy_factory is not None:
            strategy = self.strategy_factory(params)
            return self.executor_factory(params, self.transport, emit, strategy=strategy)
        return self.executor_factory(params, self.transport, emit)
    def handle(self, message):
        if 'method' not in message: return None
        method, params, request_id=message['method'], message['params'], message.get('id')
        try:
            if method=='plugin.initialize': value=self.registry.initialize(params)
            elif method=='plugin.start':
                value=self.registry.start(params)
                if params['run_id'] not in self.executors:
                    executor=self._make_executor(params); self.executors[params['run_id']]=executor; executor.thread.start()
            elif method=='plugin.wait': value=self.registry.wait(params['run_id'],params['after_sequence'],params.get('timeout_ms',0))
            elif method=='plugin.cancel':
                value=self.registry.cancel(params['run_id'],params['reason'])
                if params['run_id'] in self.executors: self.executors[params['run_id']].cancel(params['reason'])
            elif method=='plugin.shutdown':
                for executor in self.executors.values(): executor.cancel('plugin shutdown')
                value=self.registry.close(); self.running=False
            else: raise ProtocolError(f'unsupported plugin method: {method}')
            return result(request_id,value,method=method)
        except ProtocolError as exc: return error(request_id,-32602,str(exc))
        except Exception as exc: return error(request_id,-32603,str(exc))
    def serve_forever(self):
        while self.running:
            message=self.transport.recv()
            if message is None:
                print("mcts plugin: stdin closed before plugin.shutdown", file=sys.stderr)
                break
            if 'method' in message and 'id' not in message:
                self._dispatch_notification(message)
                continue
            if 'method' not in message and 'id' in message:
                for executor in self.executors.values():
                    executor.host.peer.dispatch(message)
                continue
            response=self.handle(message)
            if response is not None:
                with self._send_lock: self.transport.send(response)
        self.transport.close()

    def _dispatch_notification(self, message):
        method = message.get('method', '')
        params = message.get('params', {})
        if method.startswith('host.job.'):
            run_id = params.get('run_id')
            executor = self.executors.get(run_id)
            if executor is not None:
                executor.host.peer.dispatch(message)
            return
        for executor in self.executors.values():
            executor.host.peer.dispatch(message)
