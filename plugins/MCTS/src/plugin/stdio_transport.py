"""Line-delimited JSON-RPC transport for the real plugin process."""
from __future__ import annotations
import json, sys, threading
from typing import Any, Mapping
from .rpc_transport import MessageTransport
from src.core.mcts_protocol import decode, validate_message
class StdioTransport(MessageTransport):
    def __init__(self, reader=None, writer=None):
        self.reader=reader or sys.stdin; self.writer=writer or sys.stdout; self.closed=False; self._lock=threading.Lock()
    def send(self, message: Mapping[str,Any]) -> None:
        wire=validate_message(message)
        with self._lock:
            if self.closed: raise ConnectionError('transport is closed')
            self.writer.write(json.dumps(wire,ensure_ascii=False,separators=(',',':'))+'\n'); self.writer.flush()
    def recv(self, timeout=None):
        if self.closed: return None
        line=self.reader.readline()
        if not line: self.closed=True; return None
        return decode(line)
    def close(self): self.closed=True
