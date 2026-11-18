"""JSON-RPC v1 lifecycle server with real background MCTS execution."""
from __future__ import annotations
import threading
import sys
from xgovernor_mcts.engine.mcts_protocol import error, result, ProtocolError
from .lifecycle import RunRegistry
from xgovernor_mcts.engine.executor import RunExecutor

class LifecycleServer:
    def __init__(self, transport, registry=None, executor_factory=None, strategy_factory=None):
        self.transport=transport; self.registry=registry or RunRegistry(); self.running=True
        self.executors={}; self.executor_factory=executor_factory or RunExecutor
        self.strategy_factory = strategy_factory
        self._send_lock=threading.Lock()
    def _emit(self, run_id, method, payload):
        with self._send_lock:
            run=self.registry.runs[run_id]
            params={'plugin_id':self.registry.manifest.plugin_id,'run_id':run_id,'sequence':run.sequence+1,**payload}
            event=self.registry.emit(method, params)
            self.transport.send(event)
    def _make_executor(self, params):
        emit = lambda method,payload: self._emit(params['run_id'],method,payload)
        if self.strategy_factory is not None:
            strategy = self.strategy_factory(params)
            return self.executor_factory(params, self.transport, emit, strategy=strategy)
        return self.executor_factory(params, self.transport, emit)
    def _reply(self, request_id, method, value=None, *, err=None):
        if err is None:
            payload = result(request_id, value, method=method)
        elif isinstance(err, ProtocolError):
            payload = error(request_id, -32602, str(err))
        else:
            payload = error(request_id, -32603, str(err) or type(err).__name__)
        with self._send_lock:
            self.transport.send(payload)

    def _handle_wait_async(self, request_id, params):
        """Run plugin.wait off the serve loop so host.* RPC replies stay readable.

        The serve loop is the only stdin reader. Blocking it inside registry.wait
        (up to the 30s schema cap) leaves host.fork / host.agent_turn responses
        sitting unread, which shows up as a ~30s gap between session create and
        the first prompt even though the OpenCode HTTP calls themselves are fast.
        """
        try:
            value = self.registry.wait(
                params['run_id'],
                params['after_sequence'],
                params.get('timeout_ms', 0),
            )
            self._reply(request_id, 'plugin.wait', value)
        except Exception as exc:  # noqa: BLE001
            self._reply(request_id, 'plugin.wait', err=exc)

    def handle(self, message):
        if 'method' not in message: return None
        method, params, request_id=message['method'], message['params'], message.get('id')
        try:
            if method=='plugin.initialize': value=self.registry.initialize(params)
            elif method=='plugin.start':
                value=self.registry.start(params)
                if params['run_id'] not in self.executors:
                    executor=self._make_executor(params); self.executors[params['run_id']]=executor; executor.thread.start()
            elif method=='plugin.wait':
                # Reply asynchronously; do not block serve_forever.
                threading.Thread(
                    target=self._handle_wait_async,
                    args=(request_id, params),
                    name=f"plugin-wait-{params.get('run_id', 'run')}",
                    daemon=True,
                ).start()
                return None
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
