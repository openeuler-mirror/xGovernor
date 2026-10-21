#!/usr/bin/python3
"""Typed Management fixture with deterministic raw file bodies and per-connection trace."""
import base64
import hashlib
import json
import os
import struct
import sys
import time

trace = os.environ['XG_TEST_TRACE']
state_path = os.environ['XG_TEST_STATE']

def state():
    with open(state_path) as f:
        return json.load(f)

def reply(req, result=None, error=None):
    value = {'protocolVersion': 2, 'id': req['id'], 'ok': error is None}
    value['error' if error else 'result'] = error or result
    data = json.dumps(value).encode()
    sys.stdout.buffer.write(struct.pack('>I', len(data)) + data)
    sys.stdout.buffer.flush()

def reject(req, code, delivery="accepted"):
    reply(req, error={'code': code, 'message': code, 'retryable': False, 'delivery': delivery})

def handle(params):
    return {'scope': 'thinkthread', 'operationId': params['operationId'], 'operationDigest': params['operationDigest']}

def exec_result(params, current):
    reason = current.get('reason', 'exited')
    return {'operation': handle(params), 'thinkthreadId': params['thinkthreadId'], 'reason': reason,
            'code': current.get('code', 0 if reason == 'exited' else None),
            'stdoutBase64': base64.b64encode(b'partial' if reason == 'output_limit' else b'out').decode(),
            'stderrBase64': base64.b64encode(b'err').decode()}

while True:
    prefix = sys.stdin.buffer.read(4)
    if not prefix:
        break
    req = json.loads(sys.stdin.buffer.read(struct.unpack('>I', prefix)[0]))
    method, params = req['method'], req['params']
    with open(trace, 'a') as f:
        f.write(json.dumps({'method': method, 'params': params, 'connection': os.getpid()}) + '\n')
    current = state()
    scenario = current.get('scenario', 'ok')
    if method == 'handshake':
        reply(req, {'managementProtocolVersion': 2, 'contractFingerprint': params['contractFingerprint'],
                    'hostVersion': 'fixture', 'hostBuildId': 'fixture', 'installationId': 'fixture',
                    'runtimeDefinitionSchemaRange': {'minimum': 1, 'maximum': 1},
                    'runtimeStateSchemaRange': {'minimum': 1, 'maximum': 1}, 'supportedFeatures': []})
    elif method == 'context.bind':
        reply(req, {'bound': True})
    elif method == 'thinkthread.get':
        if scenario == 'get-rejected':
            reject(req, 'NotReady'); continue
        if scenario == 'get-hang': time.sleep(20); sys.exit(0)
        reply(req, {'thinkthreadId': params['thinkthreadId'], 'rootId': 'native-root', 'parentId': 'native-root',
                    'state': 'Running', 'sessionState': 'Ready', 'agent': 'fixture', 'fs': os.environ['XG_TEST_FS'],
                    'fsBranchId': 'other-branch' if scenario == 'get-wrong-branch' else 'private-branch', 'contentRevision': '1', 'needsRecovery': False})
    elif method == 'thinkthread.read':
        data = base64.b64decode(current.get('files', {}).get(params['path'], '')) if params['path'] in current.get('files', {}) else bytes(range(256)) * 513 + b'\x00\xfftail'
        if scenario == 'empty':
            data = b''
        if scenario == 'rejected-read':
            reject(req, 'FileReadRejected')
            continue
        reply(req, {'thinkthreadId': params['thinkthreadId'], 'bytesRead': len(data) + (1 if scenario == 'wrong-length' else 0)})
        if scenario == 'large-frame':
            sys.stdout.buffer.write(struct.pack('>I', 65537)); sys.stdout.buffer.flush(); break
        for start in range(0, len(data), 65536):
            chunk = data[start:start + 65536]
            sys.stdout.buffer.write(struct.pack('>I', len(chunk)) + chunk)
            if scenario == 'short-body':
                sys.stdout.buffer.flush(); sys.exit(0)
        if scenario != 'missing-end':
            sys.stdout.buffer.write(struct.pack('>I', 0))
        sys.stdout.buffer.flush()
        if scenario != 'ok' and scenario != 'empty':
            break
    elif method == 'thinkthread.write':
        data = bytearray()
        while True:
            n = struct.unpack('>I', sys.stdin.buffer.read(4))[0]
            if not n:
                break
            data.extend(sys.stdin.buffer.read(n))
        if scenario in ('checksum-reject', 'length-reject', 'rejected-write'):
            reject(req, {'checksum-reject': 'ChecksumMismatch', 'length-reject': 'LengthMismatch', 'rejected-write': 'FileWriteRejected'}[scenario], 'not_sent'); continue
        if scenario == 'mutation-rejected':
            reject(req, current['error_code'], current['delivery']); continue
        assert len(data) == params['byteLength']
        assert hashlib.sha256(data).hexdigest() == params['sha256']
        current.setdefault('files', {})[params['path']] = base64.b64encode(data).decode()
        with open(state_path, 'w') as f:
            json.dump(current, f)
        if scenario == 'drop':
            sys.exit(0)
        result = {'operation': handle(params), 'thinkthreadId': params['thinkthreadId'], 'bytesWritten': len(data)}
        if scenario == 'wrong-handle': result['operation']['operationId'] = 'wrong'
        reply(req, result)
    elif method == 'thinkthread.exec':
        if scenario == 'mutation-rejected':
            reject(req, current['error_code'], current['delivery']); continue
        if scenario == 'drop': sys.exit(0)
        if scenario == 'hang': time.sleep(20); sys.exit(0)
        reply(req, exec_result(params, current))
    elif method == 'operation.get':
        if scenario in ('not-found', 'ttl'):
            reject(req, 'OperationNotFound'); continue
        with open(trace) as f:
            original = next(json.loads(line) for line in f if json.loads(line)['params'].get('operationId') == params['operationId'])
        p = original['params']
        result = exec_result(p, current) if original['method'] == 'thinkthread.exec' else {'thinkthreadId': p['thinkthreadId'], 'bytesWritten': p['byteLength']}
        h = handle(p)
        if scenario == 'wrong-digest': h['operationDigest'] = '0' * 64
        if scenario == 'wrong-handle': h['operationId'] = 'wrong'
        if scenario == 'wrong-target': result['thinkthreadId'] = 'other-child'
        if scenario == 'missing-result': result = None
        reply(req, {'operation': h, 'method': 'wrong.method' if scenario == 'wrong-method' else original['method'],
                    'state': 'needs_recovery' if scenario == 'needs-recovery' else 'succeeded',
                    'acceptedAt': '2026-01-01T00:00:00Z', 'result': result})
    else:
        reject(req, 'UnknownFixtureMethod')
