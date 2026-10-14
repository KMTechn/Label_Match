import ast
from pathlib import Path

import pytest

import label_data_generation
from kmtech_shared.data_generation import probe_downgrade, record_generation


@pytest.mark.parametrize("previous_version,previous_build", [
    ("v2.0.99", "43e2f7d" + "0" * 33),
    ("v2.0.100", "32a95a6e7d8d533f217d2063da54f056f7767756"),
    ("v2.0.100", "645d910f5ccdc1dc4bb7ec30918ec7ef5c1a1429"),
])
def test_release_101_admits_previous_generations_and_blocks_100_rollback(
        tmp_path, previous_version, previous_build):
    source = Path(__file__).resolve().parents[1] / "Label_Match.py"
    assignments = ast.parse(source.read_text(encoding="utf-8-sig")).body
    app_version = next(node.value.value for node in assignments
                       if isinstance(node, ast.Assign)
                       and any(isinstance(target, ast.Name)
                               and target.id == "APP_VERSION"
                               for target in node.targets))
    assert app_version == label_data_generation.CURRENT_VERSION == "v2.0.101"
    marker = tmp_path / "data-generation.json"
    identity = {"app_id": "Label_Match", "subject_sid": "S-1-5-21-172"}
    current_build = "d8a85220f672e8616deed776100aff0661b8b968"
    record_generation(marker, previous_version, build_id=previous_build,
                      **identity)

    if previous_version == "v2.0.100":
        same_version = probe_downgrade([marker], previous_version,
                                      target_build_id=current_build, **identity)
        assert same_version["status"] == "UNKNOWN"
        assert same_version["allowed"] is False
        assert same_version["message_key"] == "data_generation_unknown"

    upgrade = probe_downgrade([marker], label_data_generation.CURRENT_VERSION,
                              target_build_id=current_build, **identity)
    assert upgrade["status"] == "ALLOW"
    assert upgrade["allowed"] is True
    recorded = record_generation(marker, label_data_generation.CURRENT_VERSION,
                                 build_id=current_build, **identity)
    assert recorded["version"] == "v2.0.101"
    assert recorded["data_compatible_min_version"] == "v2.0.101"
    assert recorded["compatibility_verified"] is False
    rollback = probe_downgrade([marker], "v2.0.100",
                               target_build_id=previous_build, **identity)
    assert rollback["status"] == "REFUSE"
    assert rollback["allowed"] is False
    assert rollback["message_key"] == "data_generation_downgrade_refused"
