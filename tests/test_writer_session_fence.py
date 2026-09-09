from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
import hashlib
import json
import ntpath
import os
from pathlib import Path
import shutil
import subprocess
import sys
import unicodedata

import pytest

import writer_session_fence as fence
from writer_sink_inventory import (
    derive_writer_sink_inventory,
    writer_sink_inventory_sha256,
)


ROOT = Path(__file__).resolve().parents[1]
CHILD = ROOT / "tests" / "_writer_fence_child.py"
HELPER = ROOT / "tools" / "label_writer_fence.ps1"
CONTRACT = ROOT / "tools" / "label_writer_fence_contract.json"


def _environment(control_root: Path) -> dict[str, str]:
    values = os.environ.copy()
    values[fence.TEST_MODE_ENV] = "1"
    values[fence.CONTROL_ROOT_OVERRIDE_ENV] = str(control_root)
    for name in (
        fence.DELEGATION_TOKEN_ENV,
        fence.DELEGATION_SESSION_ENV,
        fence.DELEGATION_ATTEMPT_ENV,
        fence.DELEGATION_TRANSACTION_ENV,
    ):
        values.pop(name, None)
    return values


def _payload(*, delegated_sources: list[str], token: str = "") -> dict[str, object]:
    session_id = "1" * 32
    attempt_id = "2" * 32
    transaction_id = "3" * 32
    orchestrator = "4" * 64
    contract = "5" * 64
    now = datetime.now(timezone.utc)
    return {
        "schema": fence.ACTIVE_SCHEMA,
        "status": "INSTALLING" if delegated_sources else "QUIESCING",
        "app_id": fence.APP_ID,
        "session_id": session_id,
        "attempt_id": attempt_id,
        "replacement_transaction_id": transaction_id,
        "session_started_at_utc": now.isoformat(),
        "orchestrator_sha256": orchestrator,
        "writer_contract_sha256": contract,
        "session_authority_mutex_name": fence.session_authority_mutex_name(
            session_id,
            attempt_id,
            orchestrator,
            transaction_id,
            contract,
        ),
        "writer_inventory_sha256": fence.WRITER_INVENTORY_SHA256,
        "owner_kind": "canonical_installer",
        "delegation_sha256": hashlib.sha256(token.encode("utf-8")).hexdigest()
        if delegated_sources
        else "",
        "delegated_sources": sorted(delegated_sources),
        "delegation_expires_at_utc": (now + timedelta(minutes=5)).isoformat()
        if delegated_sources
        else "",
        "activated_at_utc": now.isoformat(),
        "secret_values_recorded": False,
    }


def _write_active(control_root: Path, payload: dict[str, object]) -> None:
    control_root.mkdir(parents=True)
    (control_root / fence.ACTIVE_FILENAME).write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _run_child(
    control_root: Path,
    target: Path,
    mode: str,
    *,
    extra_environment: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    environment = _environment(control_root)
    environment.update(extra_environment or {})
    return subprocess.run(
        [sys.executable, str(CHILD), "--mode", mode, "--target", str(target)],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )


def _powershell() -> str:
    executable = shutil.which("powershell.exe")
    if not executable:
        pytest.skip("Windows PowerShell is required")
    return executable


def _quote(value: str | Path) -> str:
    return str(value).replace("'", "''")


def _run_powershell_harness(
    tmp_path: Path,
    body: str,
) -> subprocess.CompletedProcess[str]:
    harness = tmp_path / "label-writer-fence-harness.ps1"
    harness.write_text(
        "$ErrorActionPreference='Stop'\n" + f". '{_quote(HELPER)}'\n" + body,
        encoding="utf-8",
    )
    return subprocess.run(
        [
            _powershell(),
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(harness),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )


def _independent_normalized_control_root(control_root: str) -> str:
    selected = ntpath.abspath(control_root).replace("/", "\\").rstrip("\\")
    selected = unicodedata.normalize("NFC", selected)
    return "".join(
        chr(ord(character) + 32) if "A" <= character <= "Z" else character
        for character in selected
    )


def _independent_mutex_name(contract: dict[str, object], control_root: str) -> str:
    fence_contract = contract["all_writer_fence"]
    assert isinstance(fence_contract, dict)
    normalized = _independent_normalized_control_root(control_root)
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]
    return str(fence_contract["noncanonical_mutex_prefix"]) + digest


def test_public_mutex_contract_vectors_and_uppercase_nonascii_regression(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    assert set(contract) == {"schema", "app_id", "all_writer_fence"}
    assert contract["schema"] == "label-writer-fence-contract-v1"
    assert contract["app_id"] == fence.APP_ID
    fence_contract = contract["all_writer_fence"]
    assert set(fence_contract) == {
        "admission_mutex_name",
        "noncanonical_mutex_derivation",
        "noncanonical_mutex_prefix",
        "tuple_fields",
        "tuple_separator",
        "tuple_encoding",
        "tuple_normalization",
        "digest_algorithm",
        "digest_projection",
        "name_construction",
        "verification_vectors",
    }
    assert fence_contract["admission_mutex_name"] == fence.WRITER_MUTEX_NAME
    assert fence_contract["noncanonical_mutex_prefix"] == (
        fence.WRITER_MUTEX_NAME + "."
    )
    assert fence_contract["tuple_fields"] == ["control_root"]
    assert fence_contract["tuple_encoding"] == "UTF-8 without BOM"

    vectors = fence_contract["verification_vectors"]
    assert len(vectors) == 3
    expected: list[str] = []
    for vector in vectors:
        assert set(vector) == {"control_root", "expected_mutex_name"}
        independently_derived = _independent_mutex_name(
            contract,
            vector["control_root"],
        )
        assert independently_derived == vector["expected_mutex_name"]
        expected.append(independently_derived)

    uppercase_vectors = [
        vector for vector in vectors if "\u00c9" in vector["control_root"]
    ]
    assert len(uppercase_vectors) == 1
    uppercase_vector = uppercase_vectors[0]
    normalized = _independent_normalized_control_root(uppercase_vector["control_root"])
    assert "\u00c9" in normalized
    full_unicode_lower_name = str(fence_contract["noncanonical_mutex_prefix"]) + (
        hashlib.sha256(normalized.lower().encode("utf-8")).hexdigest()[:16]
    )
    assert full_unicode_lower_name != uppercase_vector["expected_mutex_name"]

    decomposed_vectors = [
        vector for vector in vectors if "\u0301" in vector["control_root"]
    ]
    assert len(decomposed_vectors) == 1
    decomposed_root = decomposed_vectors[0]["control_root"]
    assert ".." in decomposed_root
    assert "/" in decomposed_root and "\\" in decomposed_root

    monkeypatch.setattr(
        fence,
        "canonical_control_root",
        lambda environ=None: Path(r"C:\KMTech\Label_Match\Production-Control"),
    )
    assert [
        fence.writer_admission_mutex_name(vector["control_root"])
        for vector in vectors
    ] == expected

    completed = _run_powershell_harness(
        tmp_path,
        f"""
$contract = Get-Content -LiteralPath '{_quote(CONTRACT)}' -Raw -Encoding UTF8 |
  ConvertFrom-Json
@($contract.all_writer_fence.verification_vectors | ForEach-Object {{
  Get-LabelWriterAdmissionMutexName ([string]$_.control_root)
}}) | ConvertTo-Json -Compress
""",
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert json.loads(completed.stdout.splitlines()[-1]) == expected


def test_powershell_helper_inventory_pin_matches_python_writer_inventory() -> None:
    helper = HELPER.read_text(encoding="utf-8")
    assert (
        "$Script:LabelWriterFenceInventorySha256 = "
        f"'{fence.WRITER_INVENTORY_SHA256}'"
    ) in helper


def test_explicit_guarded_root_uses_derived_name_without_allocating_production_mutex(tmp_path: Path) -> None:
    environment = {
        "LOCALAPPDATA": str(tmp_path / "LocalAppData"),
        fence.TEST_MODE_ENV: "1",
        fence.CONTROL_ROOT_OVERRIDE_ENV: str(tmp_path / "control"),
    }
    root = fence.canonical_control_root(environment)
    expected = fence.WRITER_MUTEX_NAME + "." + hashlib.sha256(
        _independent_normalized_control_root(str(root)).encode("utf-8")
    ).hexdigest()[:16]
    assert fence.writer_admission_mutex_name(root, environ=environment) == expected
    assert expected != fence.WRITER_MUTEX_NAME
    environment.pop(fence.TEST_MODE_ENV)
    with pytest.raises(fence.WriterFenceError, match="test-only"):
        fence.canonical_control_root(environment)


def test_ordinary_production_root_keeps_literal_mutex_name_without_allocating_it(tmp_path: Path) -> None:
    environment = {"LOCALAPPDATA": str(tmp_path / "LocalAppData")}
    root = fence.canonical_control_root(environment)
    assert fence.writer_admission_mutex_name(root, environ=environment) == fence.WRITER_MUTEX_NAME


def test_code_derived_inventory_is_exactly_bound_and_covers_all_sink_families() -> None:
    inventory = derive_writer_sink_inventory(ROOT)
    sources = {row.source for row in inventory}

    assert writer_sink_inventory_sha256(inventory) == fence.WRITER_INVENTORY_SHA256
    assert sources.issuperset(
        {
            "canonical_placement",
            "scheduled_task_remove",
            "persistent_relay_cycle",
            "raw_relay_runner",
            "gui_direct_sync",
            "package_create",
            "package_cancel",
            "phs_exchange_prepare",
            "direct_sync_upload",
        }
    )
    assert len(sources) == len(inventory)


def test_inventory_pin_is_invariant_across_git_checkout_line_endings(
    tmp_path: Path,
) -> None:
    lf_root = tmp_path / "lf"
    crlf_root = tmp_path / "crlf"
    lf_root.mkdir()
    crlf_root.mkdir()
    source = '@writer_sink("sample")\ndef mutate():\n    return None\n'
    (lf_root / "sink.py").write_bytes(source.encode("utf-8"))
    (crlf_root / "sink.py").write_bytes(source.replace("\n", "\r\n").encode("utf-8"))

    lf_inventory = derive_writer_sink_inventory(lf_root)
    crlf_inventory = derive_writer_sink_inventory(crlf_root)

    assert [row.record() for row in lf_inventory] == [
        row.record() for row in crlf_inventory
    ]
    assert writer_sink_inventory_sha256(lf_inventory) == writer_sink_inventory_sha256(
        crlf_inventory
    )


def test_every_literal_package_post_is_guarded_by_derived_inventory() -> None:
    path = ROOT / "package_logistics.py"
    tree = ast.parse(path.read_text(encoding="utf-8-sig"))
    post_methods: set[str] = set()
    for class_node in (node for node in tree.body if isinstance(node, ast.ClassDef)):
        for function in (
            node
            for node in class_node.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ):
            for call in (node for node in ast.walk(function) if isinstance(node, ast.Call)):
                if (
                    isinstance(call.func, ast.Attribute)
                    and call.func.attr == "_request"
                    and call.args
                    and isinstance(call.args[0], ast.Constant)
                    and call.args[0].value == "POST"
                ):
                    post_methods.add(f"{class_node.name}.{function.name}")
    guarded = {
        row.qualified_name
        for row in derive_writer_sink_inventory(ROOT)
        if row.source_path == "package_logistics.py"
    }
    assert post_methods
    assert post_methods <= guarded


@pytest.mark.parametrize("mode", ["persistent", "placement"])
def test_active_fence_denies_real_separate_process_sink_without_mutation(
    tmp_path: Path, mode: str
) -> None:
    control_root = tmp_path / "control"
    target = tmp_path / f"{mode}.sentinel"
    payload = _payload(delegated_sources=[])
    authority = fence._acquire_named_mutex(  # noqa: SLF001 - exact OS boundary probe
        str(payload["session_authority_mutex_name"]), 0
    )
    assert authority is not None
    try:
        _write_active(control_root, payload)
        completed = _run_child(control_root, target, mode)
    finally:
        authority.release()

    assert completed.returncode == 4, completed.stderr
    assert not target.exists()
    assert not target.with_suffix(".status.json").exists()
    assert json.loads(completed.stdout.strip().splitlines()[-1])["status"] == "DENIED"


def test_actual_string_false_in_active_receipt_fails_closed(tmp_path: Path) -> None:
    control_root = tmp_path / "control"
    target = tmp_path / "string-false.sentinel"
    payload = _payload(delegated_sources=[])
    payload["secret_values_recorded"] = "false"
    authority = fence._acquire_named_mutex(  # noqa: SLF001
        str(payload["session_authority_mutex_name"]), 0
    )
    assert authority is not None
    try:
        _write_active(control_root, payload)
        completed = _run_child(control_root, target, "placement")
    finally:
        authority.release()

    assert completed.returncode == 4
    assert not target.exists()
    assert "FENCE_BINDING_INVALID" in completed.stdout


def test_exact_attempt_delegation_allows_only_the_named_placement_sink(
    tmp_path: Path,
) -> None:
    control_root = tmp_path / "control"
    target = tmp_path / "placement.sentinel"
    token = "6" * 64
    payload = _payload(delegated_sources=["canonical_placement"], token=token)
    authority = fence._acquire_named_mutex(  # noqa: SLF001
        str(payload["session_authority_mutex_name"]), 0
    )
    assert authority is not None
    try:
        _write_active(control_root, payload)
        completed = _run_child(
            control_root,
            target,
            "placement",
            extra_environment={
                fence.DELEGATION_TOKEN_ENV: token,
                fence.DELEGATION_SESSION_ENV: str(payload["session_id"]),
                fence.DELEGATION_ATTEMPT_ENV: str(payload["attempt_id"]),
                fence.DELEGATION_TRANSACTION_ENV: str(
                    payload["replacement_transaction_id"]
                ),
            },
        )
    finally:
        authority.release()

    assert completed.returncode == 0, completed.stderr
    assert target.read_text(encoding="utf-8") == "MUTATED"


def test_writer_started_while_placement_holds_admission_is_denied_nonmutating(
    tmp_path: Path,
) -> None:
    control_root = tmp_path / "control"
    target = tmp_path / "concurrent.sentinel"
    environment = _environment(control_root)
    admission = fence._acquire_named_mutex(  # noqa: SLF001
        fence.writer_admission_mutex_name(control_root, environ=environment), 0
    )
    assert admission is not None
    try:
        completed = _run_child(control_root, target, "placement")
    finally:
        admission.release()

    assert completed.returncode == 4
    assert not target.exists()
    assert "WRITER_GATE_TIMEOUT" in completed.stdout


def test_installer_fences_then_quiesces_before_placement_and_restores() -> None:
    source = (ROOT / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8")

    preimage = source.index("$taskBefore = ScheduledTaskSnapshot")
    fence_start = source.index("Start-LabelWriterFence")
    quiesce_delegation = source.index("-DelegatedSources $rollbackSources", fence_start)
    quiesce = source.index("Product $removalRoot '--remove-current-user-setup'")
    placement = source.index("InvokeFrozenPlacementHelper $frozenPlacement $helperParameters")
    restore = source.index("Product $install '--onboard-current-user'")
    fence_stop = source.index("Stop-LabelWriterFence", restore)
    assert (
        preimage
        < fence_start
        < quiesce_delegation
        < quiesce
        < placement
        < restore
        < fence_stop
    )
    assert "WriterFenceFunctionsPreloaded = $true" in source
    assert "-DelegatedSources @('canonical_placement')" in source


def test_negative_admission_depth_fails_closed_instead_of_bypassing_active_fence(
    tmp_path: Path,
) -> None:
    control_root = tmp_path / "control"
    _write_active(control_root, _payload(delegated_sources=[]))
    environment = _environment(control_root)
    admitted: list[str] = []

    fence._WRITER_LOCAL.depth = 0  # noqa: SLF001
    try:
        with pytest.raises(fence.WriterFenceError) as balanced:
            with fence.writer_admission(
                "gui_package_enqueue",
                control_root=control_root,
                environ=environment,
            ):
                admitted.append("balanced")
        # An underflowed depth is truthy, so before the guard it took the nested
        # branch: no admission mutex and no active-fence check.
        fence._WRITER_LOCAL.depth = -1  # noqa: SLF001
        with pytest.raises(fence.WriterFenceError) as underflowed:
            with fence.writer_admission(
                "gui_package_enqueue",
                control_root=control_root,
                environ=environment,
            ):
                admitted.append("underflowed")
    finally:
        fence._WRITER_LOCAL.depth = 0  # noqa: SLF001
        fence._WRITER_LOCAL.allowed_sources = None  # noqa: SLF001

    assert admitted == []
    assert balanced.value.code == "ACTIVE_WRITER_FENCE"
    assert underflowed.value.code == "WRITER_ADMISSION_DEPTH_UNDERFLOW"
