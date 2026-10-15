#!/usr/bin/python3
"""Management v2 lifecycle fixture; never establishes native readiness."""
import fcntl, json, os, socket, struct, sys
scenario = os.environ.get("XG_TEST_SCENARIO", "ok")
trace = os.environ["XG_TEST_TRACE"]
root = "fixture-owned-root"
state = "Ready"
messages = {}
starts = 0
stops = 0
state_file = os.environ.get("XG_TEST_STATE")
if scenario.startswith("owner") and state_file and os.path.exists(state_file):
    with open(state_file) as source:
        stored = json.load(source)
        state = stored["state"]
        messages = stored.get("messages", {})

def view():
    return {"thinkthreadId": root, "rootId": root, "state": state,
            "sessionState": "Processing" if scenario == "owner-message-not-ready" else "Ready", "agent": "fixture", "fs": os.environ.get("XG_TEST_FS", "/fixture"),
            "contentRevision": "1", "needsRecovery": scenario == "needs-recovery"}

def handle(params):
    return {"scope": "thinkthread", "operationId": params["operationId"],
            "operationDigest": params["operationDigest"]}

def reply(req, result):
    data = json.dumps({"protocolVersion": 2, "id": req["id"], "ok": True, "result": result}).encode()
    sys.stdout.buffer.write(struct.pack(">I", len(data)) + data)
    sys.stdout.buffer.flush()

while True:
    prefix = sys.stdin.buffer.read(4)
    if not prefix: break
    size = struct.unpack(">I", prefix)[0]
    req = json.loads(sys.stdin.buffer.read(size))
    method = req["method"]; params = req["params"]
    if scenario == "owner-concurrent" and state_file and os.path.exists(state_file):
        with open(state_file) as source: state = json.load(source)["state"]
    with open(trace, "a") as out: out.write(json.dumps({"method": method, "params": params}) + "\n")
    gate = os.environ.get("XG_TEST_OPERATION_GATE")
    if gate and os.path.exists(gate) and method in ("thinkthread.create", "operation.get"):
        with socket.socket(socket.AF_UNIX) as connection:
            connection.connect(gate); connection.sendall((method + "\n").encode()); connection.recv(1)
    if method == "handshake":
        result = {"managementProtocolVersion": 2, "contractFingerprint": params["contractFingerprint"],
                  "hostVersion": "fixture", "hostBuildId": "fixture", "installationId": "fixture",
                  "runtimeDefinitionSchemaRange": {"minimum": 1, "maximum": 1},
                  "runtimeStateSchemaRange": {"minimum": 1, "maximum": 1}, "supportedFeatures": []}
    elif method == "context.bind":
        gate = os.environ.get("XG_TEST_PREFLIGHT_GATE")
        if gate:
            with open(gate + ".count", "a+") as counter:
                fcntl.flock(counter, fcntl.LOCK_EX)
                counter.seek(0); count = int(counter.read() or "0") + 1
                counter.seek(0); counter.truncate(); counter.write(str(count)); counter.flush()
            if count <= 2:
                with socket.socket(socket.AF_UNIX) as connection:
                    connection.connect(gate); connection.sendall(b"preflight\n"); connection.recv(1)
        gate = os.environ.get("XG_TEST_RESUME_GATE")
        if gate:
            with open(gate + ".count", "a+") as counter:
                fcntl.flock(counter, fcntl.LOCK_EX)
                counter.seek(0); count = int(counter.read() or "0") + 1
                counter.seek(0); counter.truncate(); counter.write(str(count)); counter.flush()
            if count == int(os.environ.get("XG_TEST_OPEN_GATE_AT", "2")):
                with socket.socket(socket.AF_UNIX) as connection:
                    connection.connect(gate); connection.sendall(b"resume\n"); connection.recv(1)
        result = {"bound": True}
    elif method == "thinkthread.create":
        if scenario in ("drop-create","durable-drop-create","owner-create-drop"): sys.exit(0)
        result = {"operation": handle(params), "thinkthreadId": root, "changed": True}
        if scenario == "wrong-operation": result["operation"]["operationId"] = "bad-operation"
        if scenario == "wrong-digest": result["operation"]["operationDigest"] = "0" * 64
    elif method == "operation.get" and scenario == "durable-drop-create":
        with open(trace) as source:
            original = next(json.loads(line) for line in source if json.loads(line)["method"] == "thinkthread.create")
        result = {"operation": handle(original["params"]), "method": "thinkthread.create", "state": "succeeded",
                  "acceptedAt": "2026-01-01T00:00:00Z", "result": {"thinkthreadId": root, "changed": True}}
    elif method == "operation.get" and scenario.startswith("owner"):
        with open(trace) as source:
            original = next(json.loads(line) for line in source if json.loads(line)["params"].get("operationId") == params["operationId"])
        if original["method"] == "message.send":
            admitted = next(v[0] for v in messages.values() if v[0]["text"] == original["params"]["text"])
            result = {"operation": handle(original["params"]), "method": "message.send", "state": "failed" if scenario in ("owner-message-partial", "owner-message-unproven") else "succeeded",
                      "acceptedAt": "2026-01-01T00:00:00Z", "result": {} if scenario == "owner-message-unproven" else {"message": admitted}}
            reply(req,result);continue
        failed = scenario == "owner-start-reject" and original["method"] == "thinkthread.start"
        result = {"operation": handle(original["params"]), "method": original["method"], "state": "failed" if failed else "succeeded",
                  "acceptedAt": "2026-01-01T00:00:00Z", "result": {"thinkthreadId": root, "changed": True}}
    elif method == "thinkthread.get":
        if scenario.startswith("owner") and state == "Absent":
            data = json.dumps({"protocolVersion": 2, "id": req["id"], "ok": False,
                "error": {"code": "ThinkThreadNotFound", "message": "absent", "retryable": False, "delivery": "accepted"}}).encode()
            sys.stdout.buffer.write(struct.pack(">I", len(data)) + data); sys.stdout.buffer.flush(); continue
        result = view()
    elif method == "thinkthread.start":
        if scenario == "owner-start-reject":
            data = json.dumps({"protocolVersion": 2, "id": req["id"], "ok": False,
                "error": {"code": "ManagementFailed", "message": "start rejected", "retryable": False, "delivery": "accepted"}}).encode()
            sys.stdout.buffer.write(struct.pack(">I", len(data)) + data); sys.stdout.buffer.flush(); continue
        starts += 1; state = "Running"
        result = {"operation": handle(params), "thinkthreadId": root, "changed": starts == 1}
    elif method == "message.send":
        mid = "message-1" if scenario == "duplicate-message" else "message-" + str(len(messages) + 1)
        marker = params["text"].split("exactly ", 1)[1].split(".", 1)[0] if "exactly " in params["text"] else "complete reply"
        msg = {"messageId": mid, "recipientThinkthreadId": root,
               "text": params["text"], "truncated": False, "state": "queued",
               "replyTruncated": False, "createdAtUnixMs": "1"}
        messages[mid] = (msg, marker)
        result = {"operation": handle(params), "message": msg}
    elif method in ("message.wait", "message.get"):
        msg, marker = messages[params["messageId"]]
        result = {**msg, "state": "completed", "replyText": marker, "completedAtUnixMs": "2"}
        if scenario == "owner-message-eof":sys.exit(0)
        if scenario == "owner-message-failed":result["state"] = "failed"
        if scenario == "owner-message-truncated":result["replyTruncated"] = True
        if scenario == "owner-message-silent":
            with open(trace) as source: waits = sum(json.loads(line)["method"] == "message.wait" for line in source)
            result["state"] = ("delivering", "awaiting_reply", "queued")[waits % 3]
        if scenario == "queued-timeout": result["state"] = "queued"
        if scenario == "truncated": result["replyTruncated"] = True
        if scenario == "foreign-terminal": result["recipientThinkthreadId"] = "foreign-root"
        if scenario == "secret-reply": result["replyText"] = os.environ["MODEL_API_KEY"]
        if scenario == "changed-terminal" and method == "message.get": result["replyText"] = "changed"
    elif method == "thinkthread.stop":
        stops += 1
        accepted = scenario != "partial-stop" and stops == 1
        changed = accepted
        status = "stopped" if accepted else "already_stopped"
        if scenario == "partial-stop": status = "messages_active"
        if scenario == "misreported-stop":
            accepted = True; status = "messages_active"
        if scenario == "changed-noop-stop" and stops == 2: changed = True
        if scenario != "partial-stop": state = "Stopped"
        result = {"operation": handle(params), "targets": [{"thinkthreadId": root,
            "changed": changed, "accepted": accepted, "status": status}]}
    elif method == "thinkthread.remove":
        state = "Absent"
        result = {"operation": handle(params), "targets": [{"thinkthreadId": root,
            "changed": True, "accepted": True, "status": "removed"}]}
    elif method == "thinkthread.wait_for_state":
        reached = state.lower() == params["target"]
        result = {"reached": reached, "thinkthreads": [] if state == "Absent" else [view()]}
        if scenario == "not-absent" and params["target"] == "absent": result = {"reached": False, "thinkthreads": [view()]}
    else: sys.exit(2)
    if scenario.startswith("owner") and state_file and (scenario != "owner-concurrent" or method in ("thinkthread.create", "thinkthread.start", "thinkthread.stop", "thinkthread.remove")):
        target = state_file + "." + str(os.getpid()) if scenario == "owner-concurrent" else state_file
        with open(target,"w") as sink: json.dump({"state":state,"messages":messages},sink)
        if scenario == "owner-concurrent": os.replace(target, state_file)
    if scenario in ("owner-message-drop", "owner-message-partial", "owner-message-unproven") and method == "message.send":sys.exit(0)
    reply(req, result)
