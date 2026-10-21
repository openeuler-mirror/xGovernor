#!/usr/bin/python3
"""Management v2 lifecycle fixture; never establishes native readiness."""
import fcntl, json, os, socket, struct, sys, time
scenario = os.environ.get("XG_TEST_SCENARIO", "ok")
trace = os.environ["XG_TEST_TRACE"]
root = "fixture-owned-root"
state = "Ready"
messages = {}
starts = 0
stops = 0
state_file = os.environ.get("XG_TEST_STATE")
if scenario.startswith("owner") and not scenario.startswith("owner-cancel") and state_file and os.path.exists(state_file):
    with open(state_file) as source:
        stored = json.load(source)
        state = stored["state"]
        messages = stored.get("messages", {})

def checkpoint_request(operation_id):
    return {"requestId":"req-"+operation_id,"kind":"create","state":"succeeded","phase":"done",
            "result":{"kind":"created","checkpointId":"checkpoint-"+operation_id},
            "resolvedIfContentRevision":None,"continuation":"none","continuationMessageId":None,
            "executionResume":"succeeded","executionStart":None,"targetStateAtFinish":"running",
            "error":None,"acceptedAt":"1","finishedAt":"2","expiresAt":None}

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
    control = {}
    cancellation = scenario.startswith("owner-cancel")
    if cancellation:
        lock = open(state_file + ".lock", "a+")
        fcntl.flock(lock, fcntl.LOCK_EX)
        if os.path.exists(state_file):
            with open(state_file) as f: stored = json.load(f)
            state = stored["state"]; messages = stored.get("messages", {})
        if os.path.exists(state_file + ".control"):
            with open(state_file + ".control") as f: control = json.load(f)
        state = control.get("root_state", state)

    if scenario == "owner-concurrent" and state_file and os.path.exists(state_file):
        with open(state_file) as source: state = json.load(source)["state"]
    entry = {"method": method, "params": params, "connection":os.getpid(), "at":time.time()}
    if scenario == "owner-explicit-home": entry["host_home"] = os.environ.get("HOME")
    with open(trace, "a") as out: out.write(json.dumps(entry) + "\n")
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
        if cancellation and original["method"] == "message.cancel":
            msg = messages[original["params"]["messageId"]][0]
            result = {"operation":handle(original["params"]),"method":"message.cancel",
                      "state":control.get("operation_state","succeeded"),"acceptedAt":"2026-01-01T00:00:00Z","result":{"message":msg}}
            for field, value in [("operationId","wrong-id"),("operationDigest","0"*64),("scope","context")]:
                if control.get("fault")==field:result["operation"][field]=value
            if control.get("fault")=="method": result["method"]="message.send"
            if control.get("operation_not_found"):
                data=json.dumps({"protocolVersion":2,"id":req["id"],"ok":False,"error":{"code":"OperationNotFound","message":"not found","retryable":False,"delivery":"not_sent"}}).encode()
                sys.stdout.buffer.write(struct.pack(">I",len(data))+data);sys.stdout.buffer.flush();lock.close();continue
            if cancellation: lock.close()
            reply(req,result);continue
        if cancellation and original["method"] == "checkpoint.create":
            failure=control.get("checkpoint_error")
            result={"operation":handle(original["params"]),"method":"checkpoint.create","state":"failed" if failure else control.get("checkpoint_operation_state","succeeded"),"acceptedAt":"2026-01-01T00:00:00Z",
                    "result":None if failure else {"request":checkpoint_request(params["operationId"])}}
            if failure: result["error"]={"code":failure,"message":"checkpoint unavailable","retryable":True,"delivery":"accepted"}
            lock.close();reply(req,result);continue
        if original["method"] == "message.send":
            admitted = next(v[0] for v in messages.values() if v[0]["text"] == original["params"]["text"])
            result = {"operation": handle(original["params"]), "method": "message.send", "state": "failed" if scenario in ("owner-message-partial", "owner-message-unproven") else "succeeded",
                      "acceptedAt": "2026-01-01T00:00:00Z", "result": {} if scenario == "owner-message-unproven" else {"message": admitted}}
            if cancellation: lock.close()
            reply(req,result);continue
        failed = scenario == "owner-start-reject" and original["method"] == "thinkthread.start"
        result = {"operation": handle(original["params"]), "method": original["method"], "state": "failed" if failed else "succeeded",
                  "acceptedAt": "2026-01-01T00:00:00Z", "result": {"thinkthreadId": root, "changed": True}}
    elif method == "message.cancel" and cancellation:
        msg, marker = messages[params["messageId"]]
        if msg["state"] not in ("completed","failed","cancelled"):
            msg["state"]=control.get("cancel_state","cancelled")
        result={"operation":handle(params),"message":msg}
        if control.get("fault")=="messageId":result["message"]={**msg,"messageId":"foreign-message"}
        if control.get("fault")=="recipientThinkthreadId":result["message"]={**msg,"recipientThinkthreadId":"foreign-root"}
    elif method == "checkpoint.create" and cancellation:
        result={"operation":handle(params),"request":checkpoint_request(params["operationId"])}
    elif method == "checkpoint.get" and cancellation:
        result={"id":params["checkpointId"],"sourceThinkthreadId":control.get("checkpoint_source",root),"sourcePresent":True,
                "createdAtUnixMs":"2","state":"available","sourceContentRevision":"1","sourceFsKind":"direct",
                "sessionFormat":{"id":"fixture","version":1},"boundary":"terminal","sessionArtifactDigest":"a"*64,
                "sessionArtifactBytes":"1","fsSnapshotId":"fixture-snapshot","fsLogicalBytes":"1",
                "createdByRequestId":"req-"+params["checkpointId"].removeprefix("checkpoint-"),"label":None}
    elif method == "thinkthread.get":
        if cancellation and control.get("root_get_drop"):sys.exit(0)
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
        if cancellation:
            msg["state"]="completed" if "natural" in params["text"] else "awaiting_reply"
            if control.get("failed_text") and control["failed_text"] in params["text"]: msg["state"]="failed"
        messages[mid] = (msg, marker)
        result = {"operation": handle(params), "message": msg}
    elif method in ("message.wait", "message.get"):
        msg, marker = messages[params["messageId"]]
        result = {**msg, "state": "completed", "replyText": marker, "completedAtUnixMs": "2"}
        if cancellation:
            result["state"]=control.get("states",{}).get(params["messageId"],msg["state"])
            if result["state"]!="completed":result.pop("replyText",None)
            if control.get("wait_fault")=="messageId":result["messageId"]="foreign-message"
            if control.get("wait_fault")=="recipientThinkthreadId":result["recipientThinkthreadId"]="foreign-root"
            if control.get("wait_fault")=="text":result["text"]="foreign-input"
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
    if cancellation:
        lock.close()
        if method=="message.cancel" and control.get("drop_cancel"):sys.exit(0)
        if method=="message.wait" and control.get("wait_gate"):
            with socket.socket(socket.AF_UNIX) as connection:
                connection.connect(control["wait_gate"]);connection.sendall(b"wait\n");connection.recv(1)
    if scenario in ("owner-message-drop", "owner-message-partial", "owner-message-unproven") and method == "message.send":sys.exit(0)
    reply(req, result)
