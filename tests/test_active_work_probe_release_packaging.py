import ast
import hashlib
import json
from pathlib import Path

import pytest

from tools import build_release_archive as archive_builder
from tools import verify_frozen_release_assets as frozen_verifier


ROOT = Path(__file__).resolve().parents[1]
WRAPPER_PATH = ROOT / "tools" / "active_work_probe.py"
PROBE_FILES = (
    "KMTechActiveWorkProbe.exe",
    "KMTechActiveWorkProbe.independent.build-identity.json",
    "KMTechActiveWorkProbe.integrated.build-identity.json",
)
ALL_APPS = ["Inspection_worker", "Rework_worker", "Defect_Inspection", "Container_Audit", "Label_Match"]


def test_active_work_probe_wrapper_is_only_the_canonical_cli_entrypoint():
    source = WRAPPER_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)

    imports = [node for node in tree.body if isinstance(node, ast.ImportFrom)]
    assert len(imports) == 1
    assert imports[0].module == "kmtech_factory_contracts.active_work_probe.cli"
    assert [(alias.name, alias.asname) for alias in imports[0].names] == [("main", None)]
    assert "sys.path" not in source
    assert "WorkerAnalysisGUI-web" not in source
    assert "copy" not in source.lower()


def test_frozen_release_verifier_proves_both_probe_identity_scopes(tmp_path):
    executable = tmp_path / "KMTechActiveWorkProbe.exe"
    executable.write_bytes(b"probe")
    probe_sha = hashlib.sha256(b"probe").hexdigest()
    expected_commit = "1" * 40
    identities = (
        (PROBE_FILES[1], "independent", ["Label_Match"]),
        (PROBE_FILES[2], "integrated", ALL_APPS),
    )
    for filename, mode, supported_apps in identities:
        payload = {
            "schema_version": archive_builder.PROBE_SCHEMA,
            "probe_name": archive_builder.PROBE_NAME,
            "probe_version": archive_builder.PROBE_VERSION,
            "probe_artifact_sha256": probe_sha,
            "probe_source_commit": expected_commit,
            "workflow_mode": mode,
            "supported_apps": supported_apps,
        }
        (tmp_path / filename).write_bytes(
            (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")
        )

    loaded_validator = frozen_verifier._load_release_archive_validator()
    assert loaded_validator._validate_probe_identities(
        tmp_path, expected_commit=expected_commit
    ) == probe_sha

    integrated_path = tmp_path / PROBE_FILES[2]
    integrated = json.loads(integrated_path.read_bytes())
    integrated["supported_apps"] = ["Label_Match"]
    integrated_path.write_bytes(
        (json.dumps(integrated, sort_keys=True) + "\n").encode("utf-8")
    )
    with pytest.raises(
        loaded_validator.ReleaseArchiveError,
        match="active-work probe identity mismatch",
    ):
        loaded_validator._validate_probe_identities(
            tmp_path, expected_commit=expected_commit
        )


def test_probe_artifacts_are_required_by_frozen_archive_contract():
    loaded_validator = frozen_verifier._load_release_archive_validator()

    assert set(PROBE_FILES) <= frozen_verifier.REQUIRED_MEMBERS
    assert set(PROBE_FILES) <= loaded_validator.REQUIRED_PACKAGE_MEMBERS
    assert callable(loaded_validator.validate_release_evidence)


def test_probe_does_not_expand_the_existing_direct_sync_tool_set():
    builder = (ROOT / "tools" / "build_release_cli_tools.py").read_text(encoding="utf-8")

    assert "KMTechActiveWorkProbe" not in builder
    assert "active_work_probe.py" not in builder
