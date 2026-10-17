"""L2 removal admission uses real fixture files and OS observation adapters."""
from pathlib import Path
import json
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


CASES = [
    "ancestor", "outsider", "runtime_ancestor", "wrong_engine", "reused_pid",
    "other_argument", "peer_root", "normalized_root", "pending", "receipt",
    "missing_receipt", "wrong_sid", "wrong_status", "wrong_root", "old_receipt",
    "disabled", "deleted", "domain", "lookup_error", "hive_unloaded", "completed",
    "pending_run", "fallback_receipt", "missing_binding", "binding_differs",
    "profile_absent", "receipt_reparse", "receipt_hidden", "locked", "expired",
    "fallback_no_profile", "unmapped", "array_sid", "array_status", "array_root",
    "pending_run_lower", "completed_lower",
    "canonical_owner", "canonical_packet_differs", "canonical_unknown_argument",
    "parent_ended", "four_shell_chain",
    "entry_missing_receipt", 'never_started','large_marker','large_receipt','kit_bound','kit_missing_marker','kit_invalid_marker','same_birth', 'broken_top', 'newer_top', 'blank_birth', 'blank_parent', 'above_unrecognized', 'syswow64', 'cwd_only', 'entry_delegated', 'entry_raw_switch',
]


@pytest.fixture(scope="session")
def l2_results(tmp_path_factory):
    work = tmp_path_factory.mktemp('l2-proofs')
    request = work / 'cases.json'
    kit=json.loads((ROOT / 'tests/fixtures/l2_kit_removal.json').read_text(encoding='utf-8'))
    assert kit['arguments'] == ['-Uninstall']
    request.write_text(json.dumps({'cases':CASES,'kit_arguments':kit['arguments']}), encoding='utf-8')
    try:
        result = subprocess.run([
            shutil.which("powershell"), "-NoLogo", "-NoProfile", "-NonInteractive",
            "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "tests/l2_removal_batch.ps1"),
            "-Repo", str(ROOT), "-Request", str(request), "-Root", str(work),
        ], capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    except Exception as exc:
        (work/'l2-diagnostic.json').write_text(json.dumps({'error_type':type(exc).__name__}),encoding='utf-8')
        raise
    error = result.stderr.decode('utf-8',errors='replace')
    (work/'l2-diagnostic.json').write_text(json.dumps({'returncode':result.returncode,
        'parse_error':'ParserError' in error,'binding_error':'ParameterBinding' in error,
        'path_error':'PathNotFound' in error,'case_failed':'L2_CASE' in error,
        'fully_qualified_error':[line.strip() for line in error.splitlines() if 'FullyQualifiedErrorId' in line]}),encoding='utf-8')
    assert result.returncode == 0, 'L2 batch did not complete'
    return {case: json.loads((work / case / 'l2-result.json').read_bytes()) for case in CASES}


def test_l2_removal_admission(l2_results):
    failed = [case for case in CASES if l2_results[case].get('passed') is not True]
    assert not failed, 'L2 contract cases failed'


def test_l2_shared_pins_and_builder_copy_in_process(tmp_path, monkeypatch):
    """Reuse the existing builder/pin assertions inside the headless boundary."""
    from contextlib import redirect_stdout, redirect_stderr
    from io import StringIO
    from qualification import check_kmtech_shared as checker
    from tests import test_kmtech_shared as existing

    def check_in_process(root):
        stdout, stderr = StringIO(), StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            rc = checker.main(['--check', '--root', str(root)])
        return subprocess.CompletedProcess([], rc, stdout.getvalue(), stderr.getvalue())

    monkeypatch.setattr(existing, '_check', check_in_process)
    existing.test_pinned_shared_source_and_packaging_inputs()
    marks = existing.test_new_shared_leaves_are_packaged_and_pin_checked.pytestmark
    leaf_cases = next(mark.args[1] for mark in marks
                      if mark.name == 'parametrize' and mark.args[0] == 'leaf')
    for index, leaf in enumerate(leaf_cases):
        case_root = tmp_path / str(index)
        case_root.mkdir()
        existing.test_new_shared_leaves_are_packaged_and_pin_checked(case_root, leaf)
