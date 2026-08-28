from __future__ import annotations

import ast
import copy
import importlib.util
import inspect
import json
import os
from pathlib import Path
import re
import subprocess

import pytest

import protected_admin
from protected_admin import (
    MAX_PROTECTED_ADMIN_PROFILE_BYTES,
    PROTECTED_ADMIN_DISPLAY_NAME,
    PROTECTED_ADMIN_MAX_ITERATIONS,
    PROTECTED_ADMIN_MIN_ITERATIONS,
    PROTECTED_ADMIN_OPERATOR_ID,
    PROTECTED_ADMIN_ROLE,
    ProtectedAdminProfileError,
    build_protected_admin_profile,
    is_protected_admin_code,
    load_protected_admin_profile,
    persistent_operator_name,
    redact_authenticated_credential_entry,
    redact_protected_admin_code,
    sanitize_persistent_value,
    validate_protected_admin_profile,
)
from tools import install_protected_admin as installer


REPOSITORY = Path(__file__).resolve().parents[1]
SYNTHETIC_ADMIN_CODE = "000001"
OTHER_SIX_DIGIT_VALUE = "000002"
TEST_READER = r"TESTHOST\label-match-reader"
TEST_READER_SID = "S-1-5-21-10-20-30-1101"


@pytest.fixture
def provisioned_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, dict[str, object]]:
    target = tmp_path / "protected" / "protected_admin.json"
    target.parent.mkdir(parents=True)
    profile = build_protected_admin_profile(SYNTHETIC_ADMIN_CODE)
    target.write_text(json.dumps(profile, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setenv("LABEL_MATCH_PROTECTED_ADMIN_PROFILE", str(target))
    return target, profile


def _load_label_match(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("ProgramData", str(tmp_path / "ProgramData"))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "LocalAppData"))
    monkeypatch.setenv("TEMP", str(tmp_path / "Temp"))
    spec = importlib.util.spec_from_file_location(
        "label_match_protected_admin_test_module",
        REPOSITORY / "Label_Match.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class _Tree:
    def __getitem__(self, key):
        if key == "columns":
            return ()
        raise KeyError(key)

    @staticmethod
    def column(*_args, **_kwargs):
        return 0


class _Variable:
    def __init__(self, value: str):
        self.value = value

    def get(self) -> str:
        return self.value

    def set(self, value: str) -> None:
        self.value = value


class _ClosableManager:
    worker_role = "PACKAGING"

    def __init__(self) -> None:
        self.closed = False

    def close(self, timeout=None) -> bool:
        self.closed = True
        return True


def _acl_payload(
    reader_sid: str = TEST_READER_SID,
    *,
    is_directory: bool = False,
    extra_entries: list[dict[str, object]] | None = None,
) -> str:
    inheritance = 3 if is_directory else 0
    entries = [
        {
            "sid": "S-1-5-18",
            "access_type": "Allow",
            "rights": 2032127,
            "inheritance": inheritance,
            "propagation": 0,
            "inherited": False,
        },
        {
            "sid": "S-1-5-32-544",
            "access_type": "Allow",
            "rights": 2032127,
            "inheritance": inheritance,
            "propagation": 0,
            "inherited": False,
        },
        {
            "sid": reader_sid,
            "access_type": "Allow",
            "rights": installer._READ_AND_EXECUTE_WITH_SYNCHRONIZE,
            "inheritance": inheritance,
            "propagation": 0,
            "inherited": False,
        },
    ]
    entries.extend(extra_entries or [])
    return json.dumps({"protected": True, "entries": entries})


def _mock_acl_hardening(
    monkeypatch: pytest.MonkeyPatch,
    events: list[str] | None = None,
) -> None:
    trace = events if events is not None else []

    def harden_directory(path, *_args):
        Path(path).mkdir(parents=True, exist_ok=True)
        trace.append("directory-acl")

    def harden_file(path, *_args):
        target = Path(path)
        if target.suffix == ".tmp":
            assert target.stat().st_size == 0
            trace.append("temporary-acl-empty")
        else:
            assert target.stat().st_size > 0
            trace.append("final-acl")

    monkeypatch.setattr(installer, "_harden_profile_directory", harden_directory)
    monkeypatch.setattr(installer, "_harden_profile_file", harden_file)


def test_profile_builder_uses_dynamic_salt_and_verifies_only_synthetic_code(
    tmp_path: Path,
) -> None:
    first = build_protected_admin_profile(SYNTHETIC_ADMIN_CODE)
    second = build_protected_admin_profile(SYNTHETIC_ADMIN_CODE)
    first_verifier = first["verifier"]
    second_verifier = second["verifier"]
    assert isinstance(first_verifier, dict)
    assert isinstance(second_verifier, dict)
    assert first_verifier["salt_hex"] != second_verifier["salt_hex"]
    assert first_verifier["digest_hex"] != second_verifier["digest_hex"]

    target = tmp_path / "profile.json"
    target.write_text(json.dumps(first), encoding="utf-8")
    assert is_protected_admin_code(SYNTHETIC_ADMIN_CODE, profile_path=target)
    assert not is_protected_admin_code(OTHER_SIX_DIGIT_VALUE, profile_path=target)


def test_public_helper_surface_is_plan_c_cross_import_compatible() -> None:
    required = {
        "MAX_PROTECTED_ADMIN_ITERATIONS",
        "PROTECTED_ADMIN_DEFAULT_ITERATIONS",
        "PROTECTED_ADMIN_ITERATIONS",
        "redact_protected_admin_identity",
        "validate_protected_admin_profile",
    }
    assert required <= set(protected_admin.__all__)
    assert "profile_path" in inspect.signature(is_protected_admin_code).parameters
    assert (
        inspect.signature(persistent_operator_name)
        .parameters["authenticated_credential_entry"]
        .default
        is False
    )
    assert protected_admin.PROTECTED_ADMIN_MIN_ITERATIONS >= 600_000
    assert (
        protected_admin.PROTECTED_ADMIN_ITERATIONS
        == protected_admin.PROTECTED_ADMIN_DEFAULT_ITERATIONS
    )
    assert installer.__all__ == [
        "build_parser",
        "install_protected_admin_profile",
        "load_installed_profile",
        "main",
    ]
    assert (
        inspect.signature(installer.install_protected_admin_profile)
        .parameters["candidate"]
        .kind
        is inspect.Parameter.POSITIONAL_OR_KEYWORD
    )


def test_profile_size_is_rejected_before_unbounded_read(tmp_path: Path) -> None:
    target = tmp_path / "oversized-protected-admin.json"
    target.write_bytes(b"x" * (MAX_PROTECTED_ADMIN_PROFILE_BYTES + 1))
    with pytest.raises(ProtectedAdminProfileError):
        load_protected_admin_profile(target)
    assert not is_protected_admin_code(
        SYNTHETIC_ADMIN_CODE,
        profile_path=target,
    )


def test_profile_schema_is_exact_and_iteration_work_is_bounded(
    provisioned_profile: tuple[Path, dict[str, object]],
) -> None:
    _target, profile = provisioned_profile
    invalid_profiles = []

    extra_top = copy.deepcopy(profile)
    extra_top["unexpected"] = True
    invalid_profiles.append(extra_top)

    extra_verifier = copy.deepcopy(profile)
    extra_verifier["verifier"]["unexpected"] = True
    invalid_profiles.append(extra_verifier)

    for field, value in (
        ("operator_id", "other"),
        ("display_name", "other"),
        ("role", "PACKAGING"),
    ):
        changed = copy.deepcopy(profile)
        changed[field] = value
        invalid_profiles.append(changed)

    for iterations in (
        PROTECTED_ADMIN_MIN_ITERATIONS - 1,
        PROTECTED_ADMIN_MAX_ITERATIONS + 1,
        True,
    ):
        changed = copy.deepcopy(profile)
        changed["verifier"]["iterations"] = iterations
        invalid_profiles.append(changed)

    wrong_algorithm = copy.deepcopy(profile)
    wrong_algorithm["verifier"]["algorithm"] = "pbkdf2-hmac-sha1"
    invalid_profiles.append(wrong_algorithm)

    uppercase_salt = copy.deepcopy(profile)
    uppercase_salt["verifier"]["salt_hex"] = str(
        uppercase_salt["verifier"]["salt_hex"]
    ).upper()
    invalid_profiles.append(uppercase_salt)

    short_digest = copy.deepcopy(profile)
    short_digest["verifier"]["digest_hex"] = str(
        short_digest["verifier"]["digest_hex"]
    )[:-2]
    invalid_profiles.append(short_digest)

    for invalid in invalid_profiles:
        with pytest.raises(ProtectedAdminProfileError):
            validate_protected_admin_profile(invalid)


def test_profile_rejects_duplicate_fields_and_fails_closed(
    provisioned_profile: tuple[Path, dict[str, object]],
) -> None:
    target, profile = provisioned_profile
    payload = json.dumps(profile, ensure_ascii=False)
    target.write_text(payload[:-1] + ', "role": "ADMIN"}', encoding="utf-8")

    with pytest.raises(ProtectedAdminProfileError):
        load_protected_admin_profile(target)
    assert not is_protected_admin_code(SYNTHETIC_ADMIN_CODE, profile_path=target)


def test_profile_path_inspection_errors_are_mapped_and_gui_check_is_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_path(*_args, **_kwargs):
        raise OSError("injected path inspection failure")

    monkeypatch.setattr(
        protected_admin,
        "assert_path_has_no_reparse_components",
        reject_path,
    )
    with pytest.raises(ProtectedAdminProfileError):
        load_protected_admin_profile("ignored-test-path")
    assert not is_protected_admin_code(
        SYNTHETIC_ADMIN_CODE,
        profile_path="ignored-test-path",
    )


def test_general_sanitizers_preserve_unrelated_six_digit_business_values() -> None:
    business_payload = {
        "item_code": OTHER_SIX_DIGIT_VALUE,
        "nested": {"lot": OTHER_SIX_DIGIT_VALUE},
    }
    assert sanitize_persistent_value(business_payload) == business_payload
    assert sanitize_persistent_value(
        {"worker_name": f"  {OTHER_SIX_DIGIT_VALUE}  "}
    ) == {"worker_name": f"  {OTHER_SIX_DIGIT_VALUE}  "}
    assert persistent_operator_name(OTHER_SIX_DIGIT_VALUE) == OTHER_SIX_DIGIT_VALUE
    assert redact_protected_admin_code(OTHER_SIX_DIGIT_VALUE) == OTHER_SIX_DIGIT_VALUE
    assert SYNTHETIC_ADMIN_CODE not in redact_protected_admin_code(
        SYNTHETIC_ADMIN_CODE,
        authenticated_credential_entry=True,
    )
    assert (
        redact_authenticated_credential_entry(
            SYNTHETIC_ADMIN_CODE,
            authenticated=False,
        )
        == SYNTHETIC_ADMIN_CODE
    )
    assert SYNTHETIC_ADMIN_CODE not in redact_authenticated_credential_entry(
        SYNTHETIC_ADMIN_CODE,
        authenticated=True,
    )
    assert SYNTHETIC_ADMIN_CODE not in redact_authenticated_credential_entry(
        f"  {SYNTHETIC_ADMIN_CODE}  ",
        authenticated=True,
    )
    assert SYNTHETIC_ADMIN_CODE not in redact_authenticated_credential_entry(
        f"credential={SYNTHETIC_ADMIN_CODE}",
        authenticated=True,
    )
    assert (
        protected_admin.redact_protected_admin_code(PROTECTED_ADMIN_DISPLAY_NAME)
        == PROTECTED_ADMIN_DISPLAY_NAME
    )
    assert (
        persistent_operator_name(PROTECTED_ADMIN_OPERATOR_ID)
        == PROTECTED_ADMIN_DISPLAY_NAME
    )
    assert PROTECTED_ADMIN_OPERATOR_ID not in sanitize_persistent_value(
        {"operator": PROTECTED_ADMIN_OPERATOR_ID}
    )["operator"]


def test_data_manager_persists_canonical_admin_label_and_business_number(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    provisioned_profile: tuple[Path, dict[str, object]],
) -> None:
    module = _load_label_match(monkeypatch, tmp_path)
    data_root = tmp_path / "data"
    manager = module.DataManager(
        str(data_root),
        "포장실",
        PROTECTED_ADMIN_OPERATOR_ID,
        "local-test",
        authenticated_admin=True,
    )

    manager.log_event("TEST_EVENT", {"item_code": OTHER_SIX_DIGIT_VALUE})
    manager.flush(timeout=5)
    assert manager.save_current_state(
        {"current_set_info": {"lot": OTHER_SIX_DIGIT_VALUE}}
    )
    manager.close(timeout=5)

    log_text = next(data_root.glob("*.csv")).read_text(encoding="utf-8-sig")
    state_text = (data_root / module.Label_Match.FILES.CURRENT_STATE).read_text(
        encoding="utf-8"
    )
    assert PROTECTED_ADMIN_OPERATOR_ID not in log_text
    assert PROTECTED_ADMIN_OPERATOR_ID not in state_text
    assert PROTECTED_ADMIN_DISPLAY_NAME in log_text
    assert PROTECTED_ADMIN_DISPLAY_NAME in state_text
    assert OTHER_SIX_DIGIT_VALUE in log_text
    assert OTHER_SIX_DIGIT_VALUE in state_text


def test_app_settings_save_removes_authenticated_admin_but_preserves_business_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    provisioned_profile: tuple[Path, dict[str, object]],
) -> None:
    module = _load_label_match(monkeypatch, tmp_path)
    app = module.Label_Match.__new__(module.Label_Match)
    app.initialized_successfully = True
    app.worker_name = PROTECTED_ADMIN_OPERATOR_ID
    app.worker_role = PROTECTED_ADMIN_ROLE
    app._authenticated_protected_admin = True
    app.app_settings = {
        "worker_name": SYNTHETIC_ADMIN_CODE,
        "worker_role": "PACKAGING",
        "worker_history": [
            {"name": SYNTHETIC_ADMIN_CODE},
            {"name": PROTECTED_ADMIN_OPERATOR_ID},
            {"name": OTHER_SIX_DIGIT_VALUE},
            {"name": "작업자A"},
        ],
        "audit": {"approved_by": PROTECTED_ADMIN_OPERATOR_ID},
    }
    app.app_settings_path = str(tmp_path / "config" / "app_settings.json")
    Path(app.app_settings_path).parent.mkdir(parents=True)
    app.scale_factor = 1.0
    app.tree_font_size = 13
    app.summary_tree = _Tree()
    app.history_tree = _Tree()

    module.Label_Match._save_app_settings(app)

    payload = json.loads(Path(app.app_settings_path).read_text(encoding="utf-8"))
    history_names = {entry["name"] for entry in payload["worker_history"]}
    assert "worker_name" not in payload
    assert "worker_role" not in payload
    assert SYNTHETIC_ADMIN_CODE not in history_names
    assert PROTECTED_ADMIN_OPERATOR_ID not in history_names
    assert OTHER_SIX_DIGIT_VALUE in history_names
    assert "작업자A" in history_names
    assert payload["audit"]["approved_by"] == PROTECTED_ADMIN_DISPLAY_NAME


def test_settings_load_scrubs_only_verified_or_canonical_admin_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    provisioned_profile: tuple[Path, dict[str, object]],
) -> None:
    module = _load_label_match(monkeypatch, tmp_path)
    settings_path = tmp_path / "config" / "app_settings.json"
    settings_path.parent.mkdir(parents=True)
    settings_path.write_text(
        json.dumps(
            {
                "worker_name": SYNTHETIC_ADMIN_CODE,
                "worker_role": "PACKAGING",
                "worker_history": [
                    SYNTHETIC_ADMIN_CODE,
                    OTHER_SIX_DIGIT_VALUE,
                    "작업자A",
                ],
                "business_value": OTHER_SIX_DIGIT_VALUE,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    app = module.Label_Match.__new__(module.Label_Match)
    app.app_settings_path = str(settings_path)

    loaded = module.Label_Match._load_app_settings(app)

    assert "worker_name" not in loaded
    assert "worker_role" not in loaded
    assert loaded["worker_history"] == [OTHER_SIX_DIGIT_VALUE, "작업자A"]
    assert loaded["business_value"] == OTHER_SIX_DIGIT_VALUE


def test_worker_facing_title_and_header_hide_host_and_internal_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_label_match(monkeypatch, tmp_path)
    host = "PACKAGING-PC-SECRET"

    title = module._label_match_window_title()
    context = module._label_match_operator_context(PROTECTED_ADMIN_OPERATOR_ID)
    monkeypatch.setattr(module, "_label_match_machine_identity", lambda: host)
    local_log_id = module._label_match_local_log_id()
    source_host_id = module._label_match_direct_sync_source_host_id()

    assert host not in title
    assert host not in context
    assert host.lower() not in local_log_id.lower()
    assert host.lower() not in source_host_id.lower()
    assert PROTECTED_ADMIN_OPERATOR_ID not in title
    assert PROTECTED_ADMIN_OPERATOR_ID not in context
    assert PROTECTED_ADMIN_DISPLAY_NAME in context


def test_startup_trace_marks_credential_field_but_keeps_business_number(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_label_match(monkeypatch, tmp_path)

    module._label_match_startup_trace(
        "protected-test",
        candidate=SYNTHETIC_ADMIN_CODE,
        batch=OTHER_SIX_DIGIT_VALUE,
    )

    trace_files = list(tmp_path.rglob("*.log"))
    assert trace_files
    trace_text = "\n".join(path.read_text(encoding="utf-8") for path in trace_files)
    assert SYNTHETIC_ADMIN_CODE not in trace_text
    assert OTHER_SIX_DIGIT_VALUE in trace_text


def test_startup_trace_rejects_relative_environment_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_label_match(monkeypatch, tmp_path)
    code_root = tmp_path / "readonly-code"
    code_root.mkdir()
    monkeypatch.setattr(module.sys, "executable", str(code_root / "Label_Match.exe"))
    monkeypatch.setattr(module.sys, "argv", ["Label_Match.exe"])
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("ProgramData", "relative-program-data")
    monkeypatch.setenv("LOCALAPPDATA", "relative-local-app-data")
    monkeypatch.setenv("TEMP", "relative-temp")
    monkeypatch.chdir(code_root)

    module._label_match_startup_trace("readonly-root-test")

    assert list(code_root.iterdir()) == []


def test_settings_path_has_no_packaged_code_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_label_match(monkeypatch, tmp_path)
    monkeypatch.delenv("LABEL_MATCH_SETTINGS_PATH", raising=False)
    monkeypatch.delenv("LOCALAPPDATA", raising=False)

    with pytest.raises(RuntimeError, match="LOCALAPPDATA"):
        module._default_label_match_settings_path()


def test_settings_login_rejects_identity_text_clears_failures_and_authenticates_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    provisioned_profile: tuple[Path, dict[str, object]],
) -> None:
    module = _load_label_match(monkeypatch, tmp_path)
    app = module.Label_Match.__new__(module.Label_Match)
    app.current_set_info = {}
    app.is_running_simulation = False
    app.is_generating_test_logs = False
    app.run_tests = True
    app.worker_name = "포장실"
    app.worker_role = "PACKAGING"
    app._authenticated_protected_admin = False
    app.worker_name_var = _Variable("")
    app.save_directory = str(tmp_path / "data")
    app.unique_id = "local-test"
    app.data_manager = _ClosableManager()
    app._has_background_work = lambda: False
    app._block_view_only_action = lambda *_args, **_kwargs: False
    app._block_duplicate_history_load = lambda *_args, **_kwargs: False
    app._save_app_settings = lambda: None
    app._update_save_directory = lambda: None
    app.title = lambda *_args, **_kwargs: None
    app._destroy_modal_and_refocus = lambda *_args, **_kwargs: None
    created = []

    class _Manager:
        def __init__(self, *args, **kwargs):
            created.append((args, kwargs))

    monkeypatch.setattr(module, "DataManager", _Manager)

    module.Label_Match._save_settings_and_close(
        app,
        object(),
        PROTECTED_ADMIN_OPERATOR_ID,
    )
    assert app.worker_name == "포장실"
    assert not created

    app.worker_name_var.set(OTHER_SIX_DIGIT_VALUE)
    module.Label_Match._save_settings_and_close(
        app,
        object(),
        OTHER_SIX_DIGIT_VALUE,
    )
    assert app.worker_name_var.get() == ""
    assert app.worker_name == "포장실"
    assert not created

    app.worker_name_var.set(SYNTHETIC_ADMIN_CODE)
    module.Label_Match._save_settings_and_close(
        app,
        object(),
        SYNTHETIC_ADMIN_CODE,
    )
    assert app.worker_name == PROTECTED_ADMIN_OPERATOR_ID
    assert app.worker_role == PROTECTED_ADMIN_ROLE
    assert app._authenticated_protected_admin is True
    assert app.worker_name_var.get() == PROTECTED_ADMIN_DISPLAY_NAME
    assert created[-1][1]["authenticated_admin"] is True


def test_installer_dry_run_is_write_free_and_report_has_no_verifier(
    tmp_path: Path,
) -> None:
    target = tmp_path / "protected" / "protected_admin.json"
    report = installer.install_protected_admin_profile(
        SYNTHETIC_ADMIN_CODE,
        profile_path=target,
        dry_run=True,
    )

    assert report["status"] == "dry-run"
    assert not target.exists()
    serialized = json.dumps(report, ensure_ascii=False).casefold()
    assert SYNTHETIC_ADMIN_CODE not in serialized
    assert "salt" not in serialized
    assert "digest" not in serialized
    assert "verifier" not in serialized


def test_installer_applies_acl_to_empty_random_temp_before_atomic_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "protected" / "protected_admin.json"
    events: list[str] = []
    _mock_acl_hardening(monkeypatch, events)
    real_fsync = installer.os.fsync
    real_replace = installer.os.replace

    def fsync_with_trace(descriptor):
        events.append("fsync")
        return real_fsync(descriptor)

    def replace_with_trace(source, destination):
        assert Path(source).suffix == ".tmp"
        events.append("replace")
        return real_replace(source, destination)

    monkeypatch.setattr(installer.os, "replace", replace_with_trace)
    monkeypatch.setattr(installer.os, "fsync", fsync_with_trace)

    report = installer.install_protected_admin_profile(
        SYNTHETIC_ADMIN_CODE,
        profile_path=target,
        reader_principal=TEST_READER,
    )

    assert report["role"] == PROTECTED_ADMIN_ROLE
    assert events == [
        "directory-acl",
        "temporary-acl-empty",
        "fsync",
        "replace",
        "final-acl",
    ]
    installed = installer.load_installed_profile(target)
    assert validate_protected_admin_profile(installed) == installed
    assert is_protected_admin_code(SYNTHETIC_ADMIN_CODE, profile_path=target)
    assert not list(target.parent.glob("*.tmp"))


def test_acl_apply_then_readback_accepts_only_exact_allow_list(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "profile.json"
    calls: list[str] = []

    def fake_powershell(script, **_kwargs):
        if script == installer._APPLY_EXACT_ACL_SCRIPT:
            calls.append("apply")
            return TEST_READER_SID
        calls.append("readback")
        return _acl_payload()

    monkeypatch.setattr(installer, "_run_powershell", fake_powershell)
    reader_sid = installer._apply_exact_acl(
        target,
        TEST_READER,
        is_directory=False,
    )
    installer._assert_exact_acl(target, reader_sid, is_directory=False)
    assert calls == ["apply", "readback"]


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell authority is Windows-only")
def test_powershell_acl_child_rejects_poisoned_ps7_module_precedence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        powershell_executable,
        module_root,
        security_manifest,
        windows_root,
    ) = installer._trusted_windows_powershell_authority()
    poisoned_module_root = tmp_path / "hostile-powershell-7-modules"
    poisoned_module_root.mkdir()
    monkeypatch.setenv("PSModulePath", str(poisoned_module_root))
    captured: dict[str, object] = {}

    def fake_run(arguments, **kwargs):
        captured["arguments"] = arguments
        captured["environment"] = kwargs["env"]
        return subprocess.CompletedProcess(arguments, 0, "trusted-readback\n", "")

    monkeypatch.setattr(installer.subprocess, "run", fake_run)
    output = installer._run_powershell(
        "$acl = Get-Acl -LiteralPath $env:KMTECH_PROTECTED_ACL_TARGET",
        path=tmp_path / "profile.json",
    )

    assert output == "trusted-readback"
    arguments = captured["arguments"]
    environment = captured["environment"]
    assert arguments[0] == str(powershell_executable)
    assert environment["SystemRoot"] == str(windows_root)
    assert environment["WINDIR"] == str(windows_root)
    assert environment["PSModulePath"] == str(module_root)
    assert str(poisoned_module_root) not in environment["PSModulePath"]
    assert environment["KMTECH_PROTECTED_POWERSHELL_SECURITY_MANIFEST"] == str(
        security_manifest
    )
    assert environment["KMTECH_PROTECTED_POWERSHELL_MODULE_ROOT"] == str(
        module_root
    )
    assert "$env:PSModulePath = $trustedModuleRoot" in arguments[-1]
    assert "Import-Module -Name $securityManifest" in arguments[-1]
    assert "Get-Command -Name Get-Acl -CommandType Cmdlet" in arguments[-1]


@pytest.mark.parametrize(
    "principal",
    [
        "Everyone",
        r"BUILTIN\Users",
        r"TESTHOST\Users",
        r"TESTHOST\Administrators",
        "unqualified-reader",
        "*S-1-5-11",
    ],
)
def test_installer_rejects_broad_or_ambiguous_reader_principals(principal: str) -> None:
    with pytest.raises(ValueError):
        installer._validate_reader_principal(principal)


def test_acl_readback_rejects_unrelated_explicit_ace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unrelated = {
        "sid": "S-1-5-21-10-20-30-2202",
        "access_type": "Allow",
        "rights": 131241,
        "inheritance": 0,
        "propagation": 0,
        "inherited": False,
    }
    monkeypatch.setattr(
        installer,
        "_run_powershell",
        lambda *_args, **_kwargs: _acl_payload(extra_entries=[unrelated]),
    )
    with pytest.raises(ProtectedAdminProfileError):
        installer._assert_exact_acl(
            tmp_path / "profile.json",
            TEST_READER_SID,
            is_directory=False,
        )


def test_failed_reprovision_restores_existing_valid_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "protected" / "protected_admin.json"
    _mock_acl_hardening(monkeypatch)
    installer.install_protected_admin_profile(
        SYNTHETIC_ADMIN_CODE,
        profile_path=target,
        reader_principal=TEST_READER,
    )
    original = target.read_bytes()
    events: list[str] = []

    def harden_directory(path, *_args):
        Path(path).mkdir(parents=True, exist_ok=True)
        events.append("directory-acl")

    target_acl_calls = 0

    def fail_final_acl(path, *_args):
        nonlocal target_acl_calls
        path = Path(path)
        if path == target:
            target_acl_calls += 1
            if target_acl_calls == 1:
                events.append("existing-acl")
                return
            if target_acl_calls == 2:
                events.append("final-acl-failed")
                raise ProtectedAdminProfileError("injected final ACL failure")
            events.append("restored-acl")
            return
        assert path.stat().st_size == 0
        events.append("temporary-acl-empty")

    monkeypatch.setattr(installer, "_harden_profile_directory", harden_directory)
    monkeypatch.setattr(installer, "_harden_profile_file", fail_final_acl)
    real_replace = installer.os.replace

    def replace_with_trace(source, destination):
        events.append("replace")
        return real_replace(source, destination)

    monkeypatch.setattr(installer.os, "replace", replace_with_trace)

    with pytest.raises(ProtectedAdminProfileError):
        installer.install_protected_admin_profile(
            OTHER_SIX_DIGIT_VALUE,
            profile_path=target,
            reader_principal=TEST_READER,
            replace=True,
        )

    assert target.read_bytes() == original
    assert is_protected_admin_code(SYNTHETIC_ADMIN_CODE, profile_path=target)
    assert not is_protected_admin_code(OTHER_SIX_DIGIT_VALUE, profile_path=target)
    assert events == [
        "directory-acl",
        "existing-acl",
        "temporary-acl-empty",
        "temporary-acl-empty",
        "replace",
        "final-acl-failed",
        "replace",
        "restored-acl",
    ]


def test_failed_first_install_removes_unusable_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "protected" / "protected_admin.json"
    monkeypatch.setattr(
        installer,
        "_harden_profile_directory",
        lambda path, *_args: Path(path).mkdir(parents=True, exist_ok=True),
    )

    def fail_final_acl(path, *_args):
        if Path(path) == target:
            raise ProtectedAdminProfileError("injected final ACL failure")

    monkeypatch.setattr(installer, "_harden_profile_file", fail_final_acl)

    with pytest.raises(ProtectedAdminProfileError):
        installer.install_protected_admin_profile(
            SYNTHETIC_ADMIN_CODE,
            profile_path=target,
            reader_principal=TEST_READER,
        )
    assert not target.exists()


def test_failed_restore_invalidates_target_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "protected" / "protected_admin.json"
    _mock_acl_hardening(monkeypatch)
    installer.install_protected_admin_profile(
        SYNTHETIC_ADMIN_CODE,
        profile_path=target,
        reader_principal=TEST_READER,
    )

    def harden_directory(path, *_args):
        Path(path).mkdir(parents=True, exist_ok=True)

    target_acl_calls = 0

    def fail_new_and_restored_acl(path, *_args):
        nonlocal target_acl_calls
        checked = Path(path)
        if checked == target:
            target_acl_calls += 1
            if target_acl_calls >= 2:
                raise ProtectedAdminProfileError(
                    "injected target ACL readback failure"
                )
        elif checked.suffix == ".tmp":
            assert checked.stat().st_size == 0

    monkeypatch.setattr(installer, "_harden_profile_directory", harden_directory)
    monkeypatch.setattr(installer, "_harden_profile_file", fail_new_and_restored_acl)

    with pytest.raises(ProtectedAdminProfileError):
        installer.install_protected_admin_profile(
            OTHER_SIX_DIGIT_VALUE,
            profile_path=target,
            reader_principal=TEST_READER,
            replace=True,
        )
    assert not target.exists()


def test_cli_prompts_twice_and_never_accepts_credential_via_arguments(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    entries = iter([SYNTHETIC_ADMIN_CODE, SYNTHETIC_ADMIN_CODE])
    prompts: list[str] = []
    captured_candidate: list[str] = []
    compared: list[tuple[bytes, bytes]] = []

    def fake_getpass(prompt: str) -> str:
        prompts.append(prompt)
        return next(entries)

    def fake_compare(first: bytes, second: bytes) -> bool:
        compared.append((first, second))
        return True

    def fake_install(candidate, **kwargs):
        captured_candidate.append(candidate)
        return {
            "status": "installed",
            "schema_version": 1,
            "role": PROTECTED_ADMIN_ROLE,
            "profile_path": kwargs["profile_path"],
        }

    monkeypatch.setattr(installer.getpass, "getpass", fake_getpass)
    monkeypatch.setattr(installer.hmac, "compare_digest", fake_compare)
    monkeypatch.setattr(installer, "install_protected_admin_profile", fake_install)
    target = tmp_path / "profile.json"

    assert installer.main(
        [
            "--profile-path",
            str(target),
            "--reader-principal",
            TEST_READER,
        ]
    ) == 0
    output = capsys.readouterr()
    assert len(prompts) == 2
    assert len(compared) == 1
    assert captured_candidate == [SYNTHETIC_ADMIN_CODE]
    assert SYNTHETIC_ADMIN_CODE not in output.out
    assert SYNTHETIC_ADMIN_CODE not in output.err
    option_strings = {
        option
        for action in installer.build_parser()._actions
        for option in action.option_strings
    }
    assert not {"--code", "--candidate", "--credential"} & option_strings


def test_cli_confirmation_mismatch_never_calls_installer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    entries = iter([SYNTHETIC_ADMIN_CODE, OTHER_SIX_DIGIT_VALUE])
    monkeypatch.setattr(installer.getpass, "getpass", lambda _prompt: next(entries))
    monkeypatch.setattr(
        installer,
        "install_protected_admin_profile",
        lambda *_args, **_kwargs: pytest.fail("installer must not be called"),
    )

    assert installer.main(
        [
            "--profile-path",
            str(tmp_path / "profile.json"),
            "--reader-principal",
            TEST_READER,
        ]
    ) == 2
    output = capsys.readouterr()
    assert SYNTHETIC_ADMIN_CODE not in output.out + output.err
    assert OTHER_SIX_DIGIT_VALUE not in output.out + output.err


def test_cli_rejects_credential_arguments_without_reflecting_them(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        installer.getpass,
        "getpass",
        lambda _prompt: pytest.fail("unknown arguments must be rejected before input"),
    )
    assert installer.main(["--code", SYNTHETIC_ADMIN_CODE]) == 2
    output = capsys.readouterr()
    assert SYNTHETIC_ADMIN_CODE not in output.out + output.err


def test_cli_dry_run_ignores_credential_environment_and_does_not_prompt(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("PROTECTED_ADMIN_CODE", SYNTHETIC_ADMIN_CODE)
    monkeypatch.setattr(
        installer.getpass,
        "getpass",
        lambda _prompt: pytest.fail("dry-run must not request a protected code"),
    )
    assert installer.main(["--dry-run"]) == 0
    output = capsys.readouterr()
    assert SYNTHETIC_ADMIN_CODE not in output.out + output.err


def test_source_contains_no_static_verifier_or_default_profile_helper() -> None:
    production_paths = [
        REPOSITORY / "protected_admin.py",
        REPOSITORY / "tools" / "install_protected_admin.py",
        REPOSITORY / "Label_Match.py",
    ]
    production_sources = [path.read_text(encoding="utf-8") for path in production_paths]
    production_source, installer_source, _app_source = production_sources
    combined = "\n".join(production_sources)

    assert "PROTECTED_ADMIN_SALT_HEX" not in combined
    assert "PROTECTED_ADMIN_DIGEST_HEX" not in combined
    assert "protected_admin_profile_payload" not in combined
    assert not re.search(
        r"(?i)(?:salt|digest)[^\n=]*=\s*['\"][0-9a-f]{32,64}['\"]",
        combined,
    )
    for path, source in zip(production_paths, production_sources):
        six_digit_literals = [
            node.value
            for node in ast.walk(ast.parse(source, filename=str(path)))
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and len(node.value) == 6
            and node.value.isascii()
            and node.value.isdecimal()
        ]
        assert six_digit_literals == []


def _is_elevated_windows_process() -> bool:
    if os.name != "nt":
        return False
    try:
        import ctypes

        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except (AttributeError, OSError):
        return False


@pytest.mark.skipif(os.name != "nt", reason="Windows ACL integration is Windows-only")
@pytest.mark.skipif(
    os.environ.get("GITHUB_ACTIONS", "").lower() == "true",
    reason="GitHub Actions is not the trusted Windows ACL integration target",
)
def test_windows_temp_file_acl_readback_is_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    poisoned_module_root = tmp_path / "hostile-powershell-7-modules"
    poisoned_module_root.mkdir()
    poisoned_module_path = os.pathsep.join(
        filter(None, (str(poisoned_module_root), os.environ.get("PSModulePath", "")))
    )
    monkeypatch.setenv("PSModulePath", poisoned_module_path)
    identity = subprocess.run(
        ["whoami.exe"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    try:
        reader = installer._validate_reader_principal(identity)
    except ValueError:
        pytest.skip("current Windows identity is not a narrow supported reader principal")

    directory = tmp_path / "empty-profile-directory"
    installer._harden_profile_directory(directory, reader)
    target = tmp_path / "empty-profile.tmp"
    target.write_bytes(b"")
    installer._harden_profile_file(target, reader)
    assert target.read_bytes() == b""
    assert os.environ["PSModulePath"] == poisoned_module_path


@pytest.mark.skipif(
    not _is_elevated_windows_process(),
    reason="exact Windows ACL integration requires an elevated temporary-path process",
)
@pytest.mark.skipif(
    os.environ.get("GITHUB_ACTIONS", "").lower() == "true",
    reason="GitHub Actions is not the trusted Windows ACL integration target",
)
def test_windows_temp_profile_has_exact_directory_and_file_acl(tmp_path: Path) -> None:
    identity = subprocess.run(
        ["whoami.exe"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    try:
        reader = installer._validate_reader_principal(identity)
    except ValueError:
        pytest.skip("current Windows identity is not a narrow supported reader principal")

    target = tmp_path / "acl-integration" / "protected_admin.json"
    installer.install_protected_admin_profile(
        SYNTHETIC_ADMIN_CODE,
        profile_path=target,
        reader_principal=reader,
    )

    directory_sid = installer._apply_exact_acl(
        target.parent,
        reader,
        is_directory=True,
    )
    installer._assert_exact_acl(target.parent, directory_sid, is_directory=True)
    file_sid = installer._apply_exact_acl(target, reader, is_directory=False)
    installer._assert_exact_acl(target, file_sid, is_directory=False)
    assert is_protected_admin_code(SYNTHETIC_ADMIN_CODE, profile_path=target)
