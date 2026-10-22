#!/usr/bin/python3
"""Fork transport fixture: identity/error shape only, no native branches."""
import json, os, struct, sys
scenario = os.environ.get("XG_TEST_SCENARIO", "ok")
trace = os.environ["XG_TEST_TRACE"]
operations = {}
while True:
    prefix = sys.stdin.buffer.read(4)
    if not prefix: break
    req = json.loads(sys.stdin.buffer.read(struct.unpack(">I", prefix)[0]))
    p = req["params"]; method = req["method"]
    with open(trace, "a") as out: out.write(json.dumps(req) + "\n")
    error = None
    if method == "handshake":
        result = {"managementProtocolVersion":2,"contractFingerprint":p["contractFingerprint"],
                  "hostVersion":"fixture","hostBuildId":"fixture","installationId":"fixture",
                  "runtimeDefinitionSchemaRange":{"minimum":1,"maximum":1},
                  "runtimeStateSchemaRange":{"minimum":1,"maximum":1},"supportedFeatures":[]}
    elif method == "context.bind": result = {"bound":True}
    elif method == "thinkthread.fork":
        if scenario == "drop-fork": sys.exit(0)
        if scenario in ("source-not-ready","cross-runtime"):
            error = {"code":"ThinkThreadNotReady" if scenario == "source-not-ready" else "ThinkThreadNotFound",
                     "message":"fixture rejection","retryable":False,"delivery":"accepted"}
        previous = operations.get(p["operationId"])
        if scenario == "operation-conflict" and previous is not None and previous != p["operationDigest"]:
            error = {"code":"OperationConflict", "message":"fixture operation digest mismatch",
                     "retryable":False, "delivery":"not_sent"}
        elif error is None:
            operations[p["operationId"]] = p["operationDigest"]
        target = "fixture-child-" + p["operationId"]
        result = {"operation":{"scope":"thinkthread","operationId":p["operationId"],
                               "operationDigest":p["operationDigest"]},"thinkthreadId":target,
                  "sourceThinkthreadId":p["sourceThinkthreadId"],
                  "parentThinkthreadId":p.get("parentThinkthreadId","fixture-owning-root"),
                  "state":"ready","fs":p["fs"],
                  "fsBranchId":"fixture-private-" + p["operationId"] if p["fs"] == "private" else None}
    else: sys.exit(2)
    response={"protocolVersion":2,"id":req["id"],"ok":error is None}
    response["result" if error is None else "error"] = result if error is None else error
    data=json.dumps(response).encode();sys.stdout.buffer.write(struct.pack(">I",len(data))+data);sys.stdout.buffer.flush()
