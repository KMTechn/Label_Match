"""Generated mutations of every declared recovery field and nested record."""

import copy
import ast
from pathlib import Path

import pytest

from label_data_manager import read_recovery_file
from label_recovery_schema import (
    RECOVERY_SCHEMAS, validate_recovery_record, valid_recovery_archive_path,
    valid_recovery_rendered_path,
)


VERSION = "label-match-phs-label-exchange-v1"


def _sample(rule):
    if rule in RECOVERY_SCHEMAS:
        return _record(rule)
    if rule == "nullable_source_snapshot":
        return _record("source_snapshot")
    if rule == "target_prints":
        return {"LBL-A": _record("target_print_state")}
    if rule in {"action_list", "source_list", "target_list"}:
        return [_record({"action_list": "action_item", "source_list": "action_source",
                         "target_list": "action_target"}[rule])]
    if rule == "rendered_path":
        return str(Path("C:/labels/2026-09-23/phs_label_exchange/LBL-A.png"))
    return {
        "text": "VALUE", "integer": 1, "boolean": True,
        "object": {}, "nullable_object": {}, "text_list": ["VALUE"],
        "object_list": [{}], "time": "2026-09-23T00:00:00Z",
        "nullable_time": "2026-09-23T00:00:00Z",
        "nullable_text": "SET-A", "nullable_string": "VALUE",
        "date": "2026-09-23",
    }[rule]


def _record(kind):
    schema = RECOVERY_SCHEMAS[kind]
    fields = {**schema["required"], **schema["optional"]}
    result = {name: ("2026-09-23T00:00:00Z" if name.endswith("_at") else _sample(rule))
              for name, rule in fields.items()}
    if kind == "journal":
        result["schema_version"] = VERSION
    if kind == "journal_state":
        result["status"] = "PREPARED"
    if kind == "target_print_state":
        result["status"] = "PRINT_REQUESTED"
    return result


FIELDS = [
    (kind, name, rule, name in schema["required"])
    for kind, schema in RECOVERY_SCHEMAS.items()
    for name, rule in {**schema["required"], **schema["optional"]}.items()
]


def _valid(kind, value):
    return validate_recovery_record(kind, value, journal_version=VERSION)


@pytest.mark.parametrize("kind,name,rule,required", FIELDS)
def test_every_declared_field_deletion_has_explicit_result(kind, name, rule, required):
    value = _record(kind)
    assert _valid(kind, value)
    del value[name]
    if kind == "journal_state" and name == "status":
        assert not _valid(kind, value)
    elif kind == "target_print_state" and name == "status":
        assert not _valid(kind, value)
    else:
        assert _valid(kind, value) is not required


BAD_VALUES = {
    "text": (7, [], {}, None, "bad\x00text"),
    "nullable_text": ([], {}, True, "bad\x00text"),
    "nullable_string": (7, [], {}, "bad\x00text"),
    "integer": ("7", -1, True, [], {}, None),
    "boolean": (1, "true", [], {}, None),
    "object": (7, "text", [], None),
    "nullable_object": (7, "text", []),
    "text_list": (7, "text", {}, None, [7]),
    "object_list": (7, "text", {}, None, [7]),
    "time": (7, "not-a-date", "2026-09-23", [], {}, None),
    "nullable_time": (7, "not-a-date", "2026-09-23", [], {}),
    "date": (7, "not-a-date", "2026-99-99", [], {}, None),
    "rendered_path": (7, None, "relative/2026-09-23/phs_label_exchange/LBL-A.png",
                      "C:/labels/2026-09-23/phs_label_exchange/CON.png",
                      "C:/labels/2026-09-23/phs_label_exchange/../LBL-A.png",
                      "C:/labels/2026-09-23/phs_label_exchange/bad|name.png",
                      "C:/labels/2026-09-23/phs_label_exchange/" + "x" * 121 + ".png",
                      "C:/labels/2026-09-23/phs_label_exchange/bad\x00name.png"),
    "nullable_source_snapshot": (7, "text", []),
    "target_prints": (7, "text", [], None, {"LBL-A": 7}),
    "action_list": (7, "text", {}, None, [7]),
    "source_list": (7, "text", {}, None, [7]),
    "target_list": (7, "text", {}, None, [7]),
}


def _bad_values(rule):
    return BAD_VALUES.get(rule, (7, "text", [], None))


MUTATIONS = [
    (kind, name, bad)
    for kind, name, rule, _required in FIELDS
    for bad in _bad_values(rule)
]


@pytest.mark.parametrize("kind,name,mutation", MUTATIONS)
def test_every_declared_field_rejects_type_null_and_format_mutations(kind, name, mutation):
    value = _record(kind)
    value[name] = copy.deepcopy(mutation)
    assert not _valid(kind, value)


@pytest.mark.parametrize("kind", list(RECOVERY_SCHEMAS))
def test_declared_record_rejects_null_list_and_invalid_root(kind):
    assert not _valid(kind, None)
    assert not _valid(kind, [])
    if RECOVERY_SCHEMAS[kind]["required"] or kind in {"journal_state", "target_print_state"}:
        assert not _valid(kind, {"unknown": "VALUE"})


@pytest.mark.parametrize("payload", [
    pytest.param(b"\xff\xfe", id="invalid-utf8"),
    pytest.param(b"[" * 5000 + b"]" * 5000, id="depth-5000"),
    pytest.param(b'{"current_set_info":{"id":"SET-A","raw":[],"start_time":"bad"}}', id="bad-time"),
    pytest.param(b'{"current_set_info":{"id":"SET-A","raw":[],"padding":"' + b"x" * 1_048_576 + b'\\u0000"}}', id="mebibyte-nul"),
])
def test_current_reader_keeps_large_deep_nul_and_bad_utf8_unverified(tmp_path, payload):
    path = tmp_path / "current.json"
    path.write_bytes(payload)
    result = read_recovery_file(path, current_state=True)
    assert result["raw"] == payload
    assert result["verified"] is False


def test_recovery_reader_rejects_link_before_reading_target(tmp_path, monkeypatch):
    path = tmp_path / "current.json"
    path.write_bytes(b'{"current_set_info":{"id":"SET-A","raw":[]}}')
    monkeypatch.setattr(Path, "is_symlink", lambda self: self == path)
    monkeypatch.setattr(Path, "read_bytes", lambda self: (_ for _ in ()).throw(
        AssertionError(f"unexpected linked target read: {self}")))
    result = read_recovery_file(path, current_state=True)
    assert result["verified"] is False
    assert result["reason"] == "ValueError"


def test_nested_current_values_are_checked_before_conversion():
    current = _record("current")
    current["current_set_info"]["start_time"] = "2026-09-23"
    assert not _valid("current", current)
    current["current_set_info"]["start_time"] = "2026-09-23T00:00:00"
    current["current_set_info"]["exact_rescan_target_count"] = {"bad": 1}
    assert not _valid("current", current)
    current["current_set_info"]["exact_rescan_target_count"] = 1
    current["current_set_info"]["canonical_input_tag_qr"] = 7
    assert not _valid("current", current)


@pytest.mark.parametrize("kind,name", [
    ("current_set", "canonical_input_tag_qr"),
    ("draft", "source_canonical_input_tag_qr"),
    ("journal_state", "canonical_input_tag_qr"),
])
@pytest.mark.parametrize("bad", [
    "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-1",
    "PHS=2|SRC=OTHER|ITG=ITG-1|CLC=ITEM|LBL=LBL-A|HSH=aaaaaaaaaaaaaaaa",
    "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG=1|CLC=ITEM|LBL=LBL-A|HSH=aaaaaaaaaaaaaaaa",
])
def test_canonical_phs2_format_is_checked_where_written(kind, name, bad):
    value = _record(kind)
    value[name] = bad
    assert not _valid(kind, value)


def test_schema_covers_all_declared_record_fields_without_duplicate_cases():
    assert len(FIELDS) == len({(kind, name) for kind, name, _rule, _required in FIELDS})
    assert {kind for kind, *_rest in FIELDS} == set(RECOVERY_SCHEMAS)


def test_schema_fields_cover_each_local_current_draft_and_journal_writer():
    root = Path(__file__).parents[1]
    journal_tree = ast.parse((root / "phs_label_workflow.py").read_text(encoding="utf-8"))
    journal_fields = {
        keyword.arg for node in ast.walk(journal_tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_save" for keyword in node.keywords if keyword.arg
    }
    draft_tree = ast.parse((root / "package_command_draft.py").read_text(encoding="utf-8"))
    draft_fields = {
        key.value for node in ast.walk(draft_tree)
        if isinstance(node, ast.FunctionDef) and node.name == "to_dict" and node.lineno > 400
        for mapping in ast.walk(node) if isinstance(mapping, ast.Dict)
        for key in mapping.keys if isinstance(key, ast.Constant) and isinstance(key.value, str)
    }
    app_tree = ast.parse((root / "Label_Match.py").read_text(encoding="utf-8"))
    current_fields = {
        key.value for node in ast.walk(app_tree)
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict)
        and any(isinstance(target, ast.Attribute) and target.attr == "current_set_info"
                for target in node.targets)
        for key in node.value.keys if isinstance(key, ast.Constant) and isinstance(key.value, str)
    }
    for kind, writer_fields in (("journal_state", journal_fields),
                                ("draft", draft_fields), ("current_set", current_fields)):
        assert writer_fields
        schema = RECOVERY_SCHEMAS[kind]
        assert writer_fields <= schema["required"].keys() | schema["optional"].keys()


@pytest.mark.parametrize("basename", ["../outside", "CON", "NUL", "bad|name",
                                      "x" * 171, "bad\x00name"])
def test_archive_name_rejects_unsafe_paths(tmp_path, basename):
    digest = "a" * 64
    path = str(tmp_path / (basename + ".held-" + digest))
    assert not valid_recovery_archive_path(path, digest)
    assert valid_recovery_archive_path(str(tmp_path / ("journal.json.held-" + digest)), digest)


@pytest.mark.parametrize("basename", ["CON", "NUL", "bad|name", "x" * 121,
                                           "bad\x00name", "../outside"])
def test_rendered_path_name_rejects_unsafe_values_without_access(tmp_path, monkeypatch, basename):
    outside = tmp_path.parent / "outside.png"
    monkeypatch.setattr(Path, "read_bytes", lambda self: (_ for _ in ()).throw(
        AssertionError(f"unexpected file access: {self}")))
    path = str(tmp_path / "2026-09-23" / "phs_label_exchange" / (basename + ".png"))
    assert not valid_recovery_rendered_path(path)
    assert not outside.exists()
