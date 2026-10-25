import ast

import builtins

import contextlib

import copy

import datetime as dt

import hashlib

import json

import math

from pathlib import Path, PurePosixPath

import re

import subprocess

import sys

import types

SELECT = {'_sha', '_utc', 'startup_action', 'writer_processes', 'assert_ordinary_recovery',
          '_bootstrap', 'start_current', '_bootstrap_existing_writers', 'recovery_writer_result',
          '_launch_bootstrap_version', 'launch_version', 'startup_gate', '_startup_gate', 'startup_requires_bootstrap',
          '_apply_prepared_once', '_apply_prepared', 'complete_apply_claim', 'apply_claim_status',
          'new_apply_claim', 'settle_trial_failure', 'decide_trial_failure', 'persist_switch',
          'rollback_transition', 'wait_for_rollback_ready', 'observe_trial_ready', 'bind_trial',
          'process_matches_trial', 'ready_matches', 'confirm_transition', 'recovery_sha',
          'prepare_transition', 'apply_transition', 'request_rollback', '_RollbackReadyPending'}

SELECT.update({'current_relay_ready', 'wait_current_relay',
               '_bootstrap_relay_readback', '_bootstrap_relay_snapshot'})
SELECT.update({'_initial_ready_lock', '_RelayPreparationUnavailable',
               '_RelayPreparationDeadline', '_relay_preparation_deadline',
               '_relay_prepare_failure', '_relay_prepare_result',
               '_relay_startup_stop_checkpoint'})
SELECT.update({'startup_bootstrap_reason', '_startup_trial_lock',
               'UpdateMutexRecoveryRequired', '_refuse_abandoned_update'})

CACHE = {}
SELECT.add("gui_recovery_state")
SELECT.update({'_retained_confirmation_state', '_retained_confirmation_allowed',
               '_retained_native_created_at', '_retained_trial_binding',
               '_confirm_retained_ready_once', '_bootstrap_retained_candidate',
               '_commit_bootstrap_trial_observation', '_write_ready_receipt', '_RetainedAuditPending'})

class MemoryPath(PurePosixPath):
    def resolve(self):
        return self
    def read_bytes(self):
        return b'memory-claim-identity'
    def stat(self):
        raise AssertionError('unexpected stat access')

class World:
    def __init__(self, label, source, phase='FAILED', writer='relay', claim='NONE'):
        self.label = label
        self.ca = label.startswith('CA')
        self.clock = 20.0
        self.epoch = dt.datetime(2026, 10, 16, tzinfo=dt.timezone.utc)
        self.own_pid = 900
        self.counter = 1000
        self.events, self.launches, self.guidance, self.trace = [], [], [], set()
        self.scheduled = None
        self.pending_system = False
        self.request_mode = 'live'
        self.auto_ready = False
        self.prepared = False
        self.guard = 'ALLOW'
        self.control = MemoryPath('/memory/control')
        self.stage = MemoryPath('/memory/stage')
        self.policy = {'operator_sid': 'synthetic-operator', 'staging_root': str(self.stage),
                       'operator_local_app_data': '/memory/operator'}
        self.previous, self.candidate = 'a'*40, 'b'*40
        self.generation = {'status': 'READ', 'marker_path': '/memory/generation', 'record': {}}
        self.files = {str(self.control/'policy.json'): self.policy,
                      str(self.control/'active.json'): {'sha': self.previous}}
        self.state = {'schema': 'code-update-transition-v1', 'phase': phase,
                      'previous_sha': self.previous, 'candidate_sha': self.candidate,
                      'applied_at': self.at(-300), 'rollback_at': self.at(-200),
                      'nonce': 'c'*32, 'transition_id': 'd'*32, 'trial_pid': None,
                      'generation_before': copy.deepcopy(self.generation),
                      'generation_previous_release': {'version': 'v2.0.111', 'source_commit': self.previous},
                      'generation_candidate_release': {'version': 'v2.0.112', 'source_commit': self.candidate}}
        if phase == 'DEFERRED':
            self.state.update(recovery_pending=True, recovery_phase='FAILED', retained_candidate=False,
                              deferred_at=self.at(0), reason_code='UPDATE_EXISTING_WRITER')
        self.files[str(self.control/'transition.json')] = self.state
        self.rows = [self.row(writer)] if writer else []
        self.set_claim(claim)
        self.time = types.SimpleNamespace(monotonic=lambda: self.clock, time=lambda: self.clock,
                                          sleep=self.sleep)
        self.os = types.SimpleNamespace(environ={}, getpid=lambda: self.own_pid,
                                        path=types.SimpleNamespace(normcase=lambda p:p))
        self.uuid_counter = 0
        self.inventory = {}
        ns = {'__builtins__': {**vars(builtins), '__import__': self.importer},
              'Path': MemoryPath, 'Mapping': dict, 'Any': object, 'datetime': dt.datetime,
              'timezone': dt.timezone, 're': re, 'hashlib': hashlib, 'json': json,
              'os': self.os, 'time': self.time, 'math': math,
              'contextmanager': contextlib.contextmanager,
              'uuid': types.SimpleNamespace(uuid4=self.uuid),
              'WriterFencedError': type('WriterFencedError', (RuntimeError,), {}),
              'WriterFenceError': type('WriterFenceError', (RuntimeError,), {})}
        if label not in CACHE:
            tree = ast.parse(source)
            constants = {}
            for node in tree.body:
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name) and target.id in {'SCHEMA', 'READY_WINDOW_SECONDS', 'READY_CHECKS', 'RELAY_READY_CHECKS', 'STARTUP_WAIT_SECONDS', 'APPLY_PROGRESS_TIMEOUT', 'RELAY_PREPARE_SECONDS', 'RELAY_PARENT_RESULT_RESERVE_SECONDS'}:
                            if target.id == 'RELAY_READY_CHECKS':
                                constants[target.id] = eval(compile(ast.Expression(node.value),
                                    label + '/ready-checks', 'eval'),
                                    {'__builtins__': {}, 'tuple': tuple, **constants})
                            else:
                                constants[target.id] = ast.literal_eval(node.value)
            nodes = []
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in SELECT:
                    self.inventory[node.name] = {'line': node.lineno, 'end_line': node.end_lineno,
                        'sha256': hashlib.sha256(ast.get_source_segment(source, node).encode()).hexdigest()}
                    node.decorator_list = [d for d in node.decorator_list
                                           if isinstance(d, ast.Name) and d.id == 'contextmanager']
                    nodes.append(node)
            module = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0), *nodes], type_ignores=[])
            CACHE[label] = (constants,self.inventory,compile(ast.fix_missing_locations(module), label+'/auto_update.py', 'exec'))
        constants,self.inventory,code = CACHE[label]
        ns.update(constants)
        ns["UPDATE_RELAY_STOP_REQUESTED"] = "UPDATE_RELAY_STOP_REQUESTED"
        self.state['schema'] = ns['SCHEMA']
        exec(code, ns)
        self.ns = ns
        ns.update(relay_stop_requested=lambda *a: False, defer_stopped_relay=lambda *a: False, control_root=lambda p: self.control, ordinary_user_context=lambda *a: True,
                  load_policy=lambda p: copy.deepcopy(self.policy), read_json=self.read,
                  optional_record=self.optional, write_json_durable=self.write,
                  now_utc=lambda: self.at(self.clock), prepared_pending=lambda *a: self.prepared,
                  process_snapshot=self.snapshot, current_generation_status=lambda *a: self.guard,
                  startup_update_lock=self.lock, update_lock=self.lock,
                  support_record=lambda p, **kw: dict(p), emit_support_record=self.support,
                  emit_progress_guidance=lambda: self.guidance.append('UPDATE_IN_PROGRESS'),
                  request_apply_task=self.request, native_process_identity=self.identity,
                  fixed_helper=lambda *a, **kw: self.events.append('fixed-helper:'+str(a[1])),
                  version_bundle=lambda c, s: MemoryPath('/memory/versions')/s,
                  version_manifest=lambda p, **kw: {'source_commit': self.active, 'version': 'v2.0.111'},
                  start_ordinary_process=self.spawn, subprocess=types.SimpleNamespace(Popen=self.spawn, CREATE_NO_WINDOW=0),
                  headless_product_arguments=lambda p,a: ['memory-relay', *a],
                  record_confirmed_policy=lambda c,s: s, record_confirmed_transition=lambda c,s: s,
                  read_trial_generation=lambda p: copy.deepcopy(self.generation),
                  _verify_abandoned_update=lambda *a: None,
                  request_bootstrap=lambda *a, **kw: self.events.append('request-bootstrap'))

    @property
    def state(self):
        return self.files[str(self.control/'transition.json')]
    @state.setter
    def state(self, value):
        self.files[str(self.control/'transition.json')] = value
    @property
    def active(self):
        return self.files[str(self.control/'active.json')]['sha']
    def at(self, seconds):
        return (self.epoch+dt.timedelta(seconds=seconds)).isoformat()
    def uuid(self):
        self.uuid_counter += 1
        return types.SimpleNamespace(hex=format(self.uuid_counter, '032x'))
    def row(self, role, sha=None, created=0, pid=101):
        return dict(pid=pid, observable=True, bootstrap=False, sha=sha or self.previous,
                    sid=self.policy['operator_sid'], ordinary_user=True, role=role, created_at=self.at(created))
    def set_claim(self, status):
        p = str(self.control/'apply-claim.json')
        if status == 'NONE':
            self.files[p] = {'schema':'code-update-apply-claim-v1','status':'SETTLED'}
        else:
            self.files[p] = {'schema':'code-update-apply-claim-v1','status':'RUNNING',
                             'pid': 900, 'creation_identity':'memory-owner'}
        self.owner_status = 'ABSENT' if status == 'ABSENT' else 'LIVE'
    def read(self, path, **kw):
        return copy.deepcopy(self.files[str(path)])
    def optional(self, path):
        if str(path) == '/memory/relay-status.json':
            relay = next((r for r in self.rows if r['role']=='relay'), None)
            if relay:
                return {'status':'RUNNING','source_commit':self.active,'ordinary_user':True,
                        'process_id':relay['pid'],'updated_at':self.at(self.clock),
                        'code_root':str(MemoryPath('/memory/versions')/self.active/'app')}
        return copy.deepcopy(self.files.get(str(path)))
    def write(self, path, value):
        self.files[str(path)] = copy.deepcopy(value)
        self.events.append('write:'+path.name+':'+str(value.get('phase', value.get('status', 'record'))))
    def identity(self, pid):
        if any(row.get('pid') == pid for row in self.rows):
            return {'status': 'LIVE', 'creation_identity': str(pid * 100)}
        return {'status': self.owner_status, 'creation_identity': 'memory-owner'}
    def snapshot(self, control):
        if self.auto_ready and self.state['phase'] == 'ROLLBACK_PENDING_READY':
            self.ready_trial()
        return copy.deepcopy(self.rows)
    @contextlib.contextmanager
    def lock(self, *args, **kwargs):
        yield True
    def sleep(self, seconds):
        self.clock = round(self.clock + seconds, 9)
        if self.pending_system:
            self.pending_system = False
            self.ns['_apply_prepared_once'](self.control)
            self.scheduled = self.clock+1
        if self.scheduled and self.clock >= self.scheduled:
            self.scheduled = None
            self.ns['complete_apply_claim'](self.control)
    def support(self, record, **kwargs):
        self.guidance.append(record.get('reason_code', 'unknown'))
    def spawn(self, argv, **kw):
        self.counter += 1
        environment = kw.get('env', kw.get('environment', {}))
        role = 'relay' if any('relay' in str(a) for a in argv) else 'gui'
        self.launches.append({'role':role, 'phase':self.state['phase'], 'at_seconds':self.clock,
                              'has_boundary':any(n.endswith('DEFERRED_AT') for n in environment)})
        self.rows.append(self.row(role, sha=self.active, created=self.clock, pid=self.counter))
        return types.SimpleNamespace(pid=self.counter)
    def request(self, control, policy):
        self.events.append('request-system')
        if self.request_mode == 'none':
            return
        if self.request_mode == 'refresh':
            self.state = {**self.state, 'deferred_at': self.at(self.clock)}
        # The next bootstrap sleep yields to the requested SYSTEM invocation.
        self.pending_system = True
    def ready_trial(self):
        state = self.state
        if state.get('trial_pid'):
            return
        self.rows = [self.row('gui', sha=self.active, created=self.clock, pid=301),
                     self.row('relay', sha=self.active, created=self.clock, pid=302)]
        self.files[str(self.stage/'trial.json')] = {'pid':301, 'role':'gui', 'nonce':state['nonce'],
                                                  'transition_id':state['transition_id']}
        receipt = {'pid':301, 'sha':self.active, 'nonce':state['nonce'],
                   'transition_id':state['transition_id'], 'ready_at':self.at(self.clock)}
        receipt.update({k:True for k in self.ns['READY_CHECKS']})
        self.files[str(self.stage/'ready.json')] = receipt
    def importer(self, name, *args, **kwargs):
        if name == '__future__': return builtins.__import__(name, *args, **kwargs)
        if name == 'startup_timing': return builtins.__import__(name, *args, **kwargs)
        if name == 'time': return self.time
        if name == 'subprocess': return types.SimpleNamespace(Popen=self.spawn, CREATE_NO_WINDOW=0)
        if name == 'contextlib': return contextlib
        if name in {'update_service','Label_Match'}:
            return types.SimpleNamespace(file_sha256=lambda p: '0'*64, _sha256_file=lambda p: '0'*64)
        if name == 'current_user_onboarding':
            return types.SimpleNamespace(_machine_worker_context=lambda p: None,
                resolve_current_user_onboarding_paths=lambda p:types.SimpleNamespace(direct_sync_root=MemoryPath('/memory')))
        if name == 'user_relay':
            return types.SimpleNamespace(user_relay_status_path=lambda p:MemoryPath('/memory/relay-status.json'))
        if name == 'startup_check_report':
            return types.SimpleNamespace(observe=lambda *a, **kw: None)
        if name == 'kmtech_shared':
            return types.SimpleNamespace(data_generation=types.SimpleNamespace(probe_downgrade=lambda *a, **kw: {'status':'ALLOW'}))
        raise AssertionError('blocked import:'+name)
    def call(self, name, *args, **kwargs):
        entered = self.clock
        def tracer(frame, event, arg):
            if event == 'line' and frame.f_code.co_filename == self.label+'/auto_update.py':
                self.trace.add((frame.f_code.co_name, frame.f_lineno))
            return tracer
        sys.settrace(tracer)
        try:
            rc = self.ns[name](*args, **kwargs)
            return {'rc':rc, 'exception':None, 'operation_seconds':round(self.clock-entered,3)}
        except Exception as error:
            return {'rc':None, 'exception':type(error).__name__, 'operation_seconds':round(self.clock-entered,3)}
        finally:
            sys.settrace(None)
    def bootstrap(self, role='gui'):
        args = [] if role == 'gui' else ['--container-audit-user-relay' if self.ca else '--label-match-user-relay']
        return self.call('_bootstrap', MemoryPath('/memory/current/app'), args, role=role)
    def summary(self, outcome):
        return {**outcome, 'gui_launches':sum(x['role']=='gui' for x in self.launches),
                'relay_launches':sum(x['role']=='relay' for x in self.launches),
                'elapsed_seconds':round(self.clock-20, 3), 'phase':self.state['phase'],
                'recovery_pending':self.state.get('recovery_pending',False),
                'claim_status':self.ns['apply_claim_status'](self.control),
                'guidance':self.guidance, 'events':self.events,
                'lines':[{'function':f,'line':l} for f,l in sorted(self.trace)]}
