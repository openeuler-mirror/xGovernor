#!/usr/bin/python3
"""Management v2 transport fixture; supplies no Runtime or Agent behavior."""

import json
import os
import struct
import sys
import time

scenario = os.environ.get("XG_TEST_SCENARIO", "ok")
trace_path = os.environ["XG_TEST_TRACE"]


def record(value):
    with open(trace_path, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(value) + "\n")


def read_exact(length):
    value = sys.stdin.buffer.read(length)
    if len(value) != length:
        raise EOFError
    return value


def respond(request, result=None, error=None):
    response = {"protocolVersion": 2, "id": request["id"], "ok": error is None}
    response["result" if error is None else "error"] = result if error is None else error
    data = json.dumps(response).encode()
    sys.stdout.buffer.write(struct.pack(">I", len(data)) + data)
    sys.stdout.buffer.flush()


record({"pid": os.getpid(), "envKeys": sorted(os.environ)})
while True:
    try:
        prefix = sys.stdin.buffer.read(4)
        if not prefix:
            break
        if len(prefix) != 4:
            raise EOFError
        request = json.loads(read_exact(struct.unpack(">I", prefix)[0]))
    except EOFError:
        sys.exit(1)
    record(request)
    method = request["method"]
    if method == "handshake":
        if scenario == "hang":
            time.sleep(30)
        if scenario == "eof":
            sys.exit(0)
        if scenario == "oversized":
            sys.stdout.buffer.write(struct.pack(">I", 1024 * 1024 + 1))
            sys.stdout.buffer.flush()
            continue
        params = request["params"]
        respond(request, {
            "managementProtocolVersion": 3 if scenario == "bad-version" else 2,
            "contractFingerprint": "0" * 64 if scenario == "bad-fingerprint" else params["contractFingerprint"],
            "hostVersion": "fixture", "hostBuildId": "fixture", "installationId": "fixture",
            "runtimeDefinitionSchemaRange": {"minimum": 1, "maximum": 1},
            "runtimeStateSchemaRange": {"minimum": 1, "maximum": 1},
            "supportedFeatures": [],
        })
    elif method == "context.bind":
        respond(request, {"bound": scenario != "bind-refused"})
    elif method == "message.send":
        if scenario == "drop-send":
            sys.exit(0)
        params = request["params"]
        respond(request, {
            "operation": {"scope": "thinkthread", "operationId": params["operationId"], "operationDigest": params["operationDigest"]},
            "message": {
                "messageId": "fixture-message", "recipientThinkthreadId": params["recipientThinkthreadId"],
                "text": params["text"], "truncated": False, "state": "queued",
                "replyTruncated": False, "createdAtUnixMs": "1",
            },
        })
    elif method == "message.cancel":
        respond(request, error={
            "code": "MessageAlreadyDispatched", "message": "fixture rejection",
            "retryable": False, "delivery": "accepted",
        })
    else:
        sys.exit(2)
