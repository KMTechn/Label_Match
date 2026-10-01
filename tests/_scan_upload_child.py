"""Synthetic independent relay process for the native writer-mutex regression."""
import json
import os
from pathlib import Path
import sys
import time

import pytest
from tests.conftest import register_lm_mutex
from tests.lock_remote_guard import LockRemoteGuard
from tests.test_direct_sync_push import make_manifest, write_csv, make_credentials, FakeSession, FakeResponse
from direct_sync_push import build_source_file_plan, upload_source_file
import writer_session_fence as fence

root, delay = Path(sys.argv[1]), float(sys.argv[2])
guard = LockRemoteGuard()
with pytest.MonkeyPatch.context() as patch:
    register_lm_mutex(patch, guard)
    _manifest, path = make_manifest(root)
    credentials = make_credentials()
    plan = build_source_file_plan(source_file_path=write_csv(root), producer_manifest_path=path, credentials=credentials)
    session = FakeSession(FakeResponse(503, {'committed': False, 'retryable': True}))
    original = session.post
    def delayed(*args, **kwargs):
        (root / 'entered').write_text('entered', encoding='utf-8')
        time.sleep(delay)
        return original(*args, **kwargs)
    session.post = guard.wrap_transport(delayed, 'independent relay upload')
    upload_source_file(plan, credentials, session=session, status_dir=root / 'status')
    guard.assert_clean()
    (root / 'done').write_text(json.dumps({'violations': 0, 'pid': os.getpid(),
        'mutex_name': fence.writer_admission_mutex_name(fence.canonical_control_root())}), encoding='utf-8')
