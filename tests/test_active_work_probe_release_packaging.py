import ast
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "release.yml"
WRAPPER_PATH = ROOT / "tools" / "active_work_probe.py"
PROBE_FILES = (
    "KMTechActiveWorkProbe.exe",
    "KMTechActiveWorkProbe.independent.build-identity.json",
    "KMTechActiveWorkProbe.integrated.build-identity.json",
)
ALL_APPS = "Inspection_worker,Rework_worker,Defect_Inspection,Container_Audit,Label_Match"


def _step(workflow: str, name: str) -> str:
    start = workflow.index(f"- name: {name}")
    end = workflow.find("\n      - name:", start + 1)
    return workflow[start:] if end < 0 else workflow[start:end]


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


def test_release_builds_onefile_probe_and_proves_both_identity_scopes():
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    block = _step(workflow, "Build and verify active-work probe")

    assert '--name "KMTechActiveWorkProbe"' in block
    assert "--onefile `" in block
    assert "--console `" in block
    assert '--distpath dist/Label_Match `' in block
    assert '--add-data "kmtech_factory_contracts/bundle;kmtech_factory_contracts/bundle" `' in block
    assert "--collect-submodules kmtech_factory_contracts.active_work_probe `" in block
    assert "tools/active_work_probe.py" in block

    assert block.count("python -m kmtech_factory_contracts.active_work_probe `") == 2
    assert block.count("& $probeArtifactPath `") == 2
    assert block.count("-Mode build-identity `") == 4
    assert block.count("-WorkflowMode independent `") == 2
    assert block.count("-WorkflowMode integrated `") == 2
    assert block.count('-SupportedApps "Label_Match" `') == 2
    assert f'$allProbeApps = "{ALL_APPS}"' in block
    assert "$packageRoot = (Resolve-Path -LiteralPath \"dist/Label_Match\").Path" in block
    assert block.count("[IO.Path]::GetFullPath") >= 5
    assert "-OutputPath $independentIdentityPath `" in block
    assert "-OutputPath $integratedIdentityPath `" in block
    assert block.index("python -m kmtech_factory_contracts.active_work_probe `") < block.index(
        "& $probeArtifactPath --help"
    ) < block.index("& $probeArtifactPath `")

    for field in (
        "schema_version",
        "probe_source_commit",
        "workflow_mode",
        "probe_name",
        "probe_version",
        "probe_artifact_sha256",
        "supported_apps",
    ):
        assert f"identity.{field}" in block
    assert '$probeName = "KMTechActiveWorkProbe"' in block
    assert '$probeVersion = "v1.0.3.4"' in block
    assert "Get-FileHash -LiteralPath $probeArtifactPath -Algorithm SHA256" in block
    assert "[System.Linq.Enumerable]::SequenceEqual" in block
    assert "Fresh probe comparison directory already exists" in block
    assert "[IO.File]::Delete($smokeIdentityPath)" in block
    assert "[IO.Directory]::Delete($comparisonRoot, $false)" in block
    assert "Probe comparison cleanup failed" in block


def test_probe_artifacts_are_in_manifest_inventory_and_exact_release_zip_contract():
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    manifest = _step(workflow, "Seal and verify exact factory package manifest")
    archive = _step(workflow, "Build and verify internal release archive")
    required_match = re.search(r"required = \{(?P<body>.*?)\n\s*\}", archive, re.DOTALL)
    assert required_match is not None
    required = set(re.findall(r'"([^"]+)"', required_match.group("body")))

    for name in PROBE_FILES:
        assert f"--expected-file {name}" in manifest
        assert name in required
        assert f'"Label_Match/{name}"' in workflow

    assert workflow.index("- name: Build and verify active-work probe") < workflow.index(
        "- name: Seal and verify exact factory package manifest"
    ) < workflow.index("- name: Build and verify internal release archive")
    assert 'expected = {f"Label_Match/{name}" for name in relative}' in archive
    assert "archive membership differs from staged package" in archive
    assert "archive byte parity failed" in archive


def test_probe_does_not_expand_the_existing_direct_sync_tool_set():
    builder = (ROOT / "tools" / "build_release_cli_tools.py").read_text(encoding="utf-8")

    assert "KMTechActiveWorkProbe" not in builder
    assert "active_work_probe.py" not in builder
