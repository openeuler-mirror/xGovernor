#!/usr/bin/python3
"""Management v2 lifecycle fixture; never establishes native readiness."""
import json, os, struct, sys
scenario = os.environ.get("XG_TEST_SCENARIO", "ok")
trace = os.environ["XG_TEST_TRACE"]
root = "fixture-owned-root"
state = "Ready"
messages = {}
starts = 0
stops = 0

def view():
    return {"thinkthreadId": root, "rootId": root, "state": state,
            "sessionState": "Ready", "agent": "fixture", "fs": "/fixture",
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
    with open(trace, "a") as out: out.write(json.dumps({"method": method, "params": params}) + "\n")
    if method == "handshake":
        result = {"managementProtocolVersion": 2, "contractFingerprint": params["contractFingerprint"],
                  "hostVersion": "fixture", "hostBuildId": "fixture", "installationId": "fixture",
                  "runtimeDefinitionSchemaRange": {"minimum": 1, "maximum": 1},
                  "runtimeStateSchemaRange": {"minimum": 1, "maximum": 1}, "supportedFeatures": []}
    elif method == "context.bind": result = {"bound": True}
    elif method == "thinkthread.create":
        if scenario == "drop-create": sys.exit(0)
        result = {"operation": handle(params), "thinkthreadId": root, "changed": True}
        if scenario == "wrong-operation": result["operation"]["operationId"] = "bad-operation"
        if scenario == "wrong-digest": result["operation"]["operationDigest"] = "0" * 64
    elif method == "thinkthread.get": result = view()
    elif method == "thinkthread.start":
        starts += 1; state = "Running"
        result = {"operation": handle(params), "thinkthreadId": root, "changed": starts == 1}
    elif method == "message.send":
        mid = "message-1" if scenario == "duplicate-message" else "message-" + str(len(messages) + 1)
        marker = params["text"].split("exactly ", 1)[1].split(".", 1)[0]
        msg = {"messageId": mid, "recipientThinkthreadId": root,
               "text": params["text"], "truncated": False, "state": "queued",
               "replyTruncated": False, "createdAtUnixMs": "1"}
        messages[mid] = (msg, marker)
        result = {"operation": handle(params), "message": msg}
    elif method in ("message.wait", "message.get"):
        msg, marker = messages[params["messageId"]]
        result = {**msg, "state": "completed", "replyText": marker, "completedAtUnixMs": "2"}
        if scenario == "queued-timeout": result["state"] = "queued"
        if scenario == "truncated": result["replyTruncated"] = True
        if scenario == "foreign-terminal": result["recipientThinkthreadId"] = "foreign-root"
        if scenario == "secret-reply": result["replyText"] = os.environ["MODEL_API_KEY"]
        if scenario == "changed-terminal" and method == "message.get": result["replyText"] = "changed"
    elif method == "thinkthread.stop":
        stops += 1
        accepted = scenario != "partial-stop"
        if accepted: state = "Stopped"
        result = {"operation": handle(params), "targets": [{"thinkthreadId": root,
            "changed": stops == 1, "accepted": accepted,
            "status": "stopped" if accepted else "messages_active"}]}
    elif method == "thinkthread.remove":
        state = "Absent"
        result = {"operation": handle(params), "targets": [{"thinkthreadId": root,
            "changed": True, "accepted": True, "status": "removed"}]}
    elif method == "thinkthread.wait_for_state":
        reached = state.lower() == params["target"]
        result = {"reached": reached, "thinkthreads": [] if state == "Absent" else [view()]}
        if scenario == "not-absent" and params["target"] == "absent": result = {"reached": False, "thinkthreads": [view()]}
    else: sys.exit(2)
    reply(req, result)
