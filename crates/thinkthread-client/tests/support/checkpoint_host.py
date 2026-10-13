#!/usr/bin/python3
"""Checkpoint/restore wire fixture; no Session or filesystem is captured."""
import json, os, struct, sys
scenario = os.environ.get("XG_TEST_SCENARIO", "ok")
trace = os.environ["XG_TEST_TRACE"]
request_id = "fixture-request"

def result_request(kind, terminal=False):
    main = {"kind": "created", "checkpointId": "fixture-checkpoint"}
    if kind == "restore": main = {"kind": "restored", "checkpointId": "fixture-checkpoint",
                                  "thinkthreadId": "fixture-source", "contentRevision": "42"}
    if kind == "remove": main = {"kind": "removed", "checkpointId": "fixture-checkpoint"}
    failed = scenario == "partial-restore" and kind == "restore"
    if scenario == "failed-create" and kind == "create" and terminal:
        return {"requestId": request_id, "kind": "create", "state": "failed", "phase": "done",
                "result": None, "resolvedIfContentRevision": None, "continuation": "none",
                "continuationMessageId": None, "executionResume": "not_attempted",
                "executionStart": None, "targetStateAtFinish": "running",
                "error": {"code": "NotAtCheckpointBoundary", "message": "current turn has no confirmed native checkpoint"},
                "acceptedAt": "1", "finishedAt": "2", "expiresAt": "3"}

    return {"requestId": request_id, "kind": kind,
            "state": "failed" if failed else ("succeeded" if terminal else "accepted"),
            "phase": "done" if terminal or failed else "accepted", "result": main if terminal or failed else None,
            "resolvedIfContentRevision": "41" if kind == "restore" else None,
            "continuation": "failed" if failed else "none", "continuationMessageId": None,
            "executionResume": "succeeded" if kind == "create" and terminal else None,
            "executionStart": "failed" if failed else None, "targetStateAtFinish": "Stopped" if failed else None,
            "error": {"code":"fixture_start_failed","message":"synthetic-private-value"} if failed else None,
            "acceptedAt": "1", "finishedAt": "2" if terminal or failed else None, "expiresAt": None}

while True:
    prefix = sys.stdin.buffer.read(4)
    if not prefix: break
    req = json.loads(sys.stdin.buffer.read(struct.unpack(">I", prefix)[0]))
    method = req["method"]; p = req["params"]
    with open(trace, "a") as out: out.write(json.dumps(req) + "\n")
    error = None
    if method == "handshake":
        result = {"managementProtocolVersion": 2, "contractFingerprint": p["contractFingerprint"],
                  "hostVersion": "fixture", "hostBuildId": "fixture", "installationId": "fixture",
                  "runtimeDefinitionSchemaRange": {"minimum":1,"maximum":1},
                  "runtimeStateSchemaRange": {"minimum":1,"maximum":1},"supportedFeatures":[]}
    elif method == "context.bind": result = {"bound": True}
    elif method in ("checkpoint.create", "checkpoint.remove", "thinkthread.restore"):
        if scenario == "drop-restore" and method == "thinkthread.restore": sys.exit(0)
        kind = {"checkpoint.create":"create","checkpoint.remove":"remove","thinkthread.restore":"restore"}[method]
        result = {"operation":{"scope":"thinkthread","operationId":p["operationId"],
                               "operationDigest":p["operationDigest"]},"request":result_request(kind)}
        if scenario == "revision-conflict" and method == "thinkthread.restore":
            error = {"code":"ContentRevisionConflict","message":"fixture conflict","retryable":False,"delivery":"accepted"}
        if scenario == "checkpoint-in-use" and method == "checkpoint.remove":
            error = {"code":"CheckpointInUse","message":"Checkpoint is retained by: fixture-branch",
                     "retryable":False,"delivery":"accepted"}
    elif method == "checkpoint.request.get":
        result = {"request":result_request("create",True),"continuationMessage":None,
                  "continuationRecovery":None,"recoveryBlocker":None}
    elif method == "checkpoint.get":
        result = {"id":"fixture-checkpoint","sourceThinkthreadId":"fixture-source","sourcePresent":True,
                  "createdAtUnixMs":"1","state":"available","sourceContentRevision":"41",
                  "sourceFsKind":"private","sessionFormat":{"id":"fixture","version":1},
                  "boundary":"terminal","sessionArtifactDigest":"a"*64,"sessionArtifactBytes":"8192",
                  "fsSnapshotId":"fixture-fs","fsLogicalBytes":"1024","createdByRequestId":request_id,"label":None}
    else: sys.exit(2)
    response = {"protocolVersion":2,"id":req["id"],"ok":error is None}
    response["result" if error is None else "error"] = result if error is None else error
    data = json.dumps(response).encode()
    sys.stdout.buffer.write(struct.pack(">I", len(data)) + data);sys.stdout.buffer.flush()
