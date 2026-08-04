import hashlib
import json
import sys
import zipfile
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

from run_test1_exact_artifact import (  # noqa: E402
    ArtifactIdentityError,
    launch_exact_artifact,
    sha256_file,
)


def test_exact_artifact_launch_records_identity_and_blocks_hash_drift(tmp_path):
    executable = tmp_path / "installed" / "App.exe"
    executable.parent.mkdir()
    executable.write_bytes(b"exact packaged executable")
    archive = tmp_path / "release.zip"
    with zipfile.ZipFile(archive, "w") as package:
        package.writestr("Package/App.exe", executable.read_bytes())

    launches = []

    class FakeProcess:
        pid = 4242

        @staticmethod
        def poll():
            return None

        @staticmethod
        def wait(timeout=None):
            return 0

        @staticmethod
        def terminate():
            return None

        @staticmethod
        def kill():
            return None

    def fake_popen(argv, cwd):
        launches.append((argv, cwd))
        return FakeProcess()

    evidence = tmp_path / "evidence" / "identity.json"
    identity = launch_exact_artifact(
        archive_path=archive,
        expected_archive_sha256=sha256_file(archive),
        executable_path=executable,
        expected_executable_sha256=hashlib.sha256(
            executable.read_bytes()
        ).hexdigest(),
        archive_member="Package/App.exe",
        evidence_json=evidence,
        popen_factory=fake_popen,
        query_process_path=lambda _pid: executable,
    )

    recorded = json.loads(evidence.read_text(encoding="utf-8"))
    assert identity["status"] == recorded["status"] == "PASS"
    assert recorded["archive"]["sha256"] == sha256_file(archive)
    assert recorded["installed_executable"]["sha256"] == sha256_file(executable)
    assert recorded["archive_member"]["matches_installed_executable"] is True
    assert recorded["process"] == {
        "pid": 4242,
        "executable_path": str(executable.resolve()),
        "matches_installed_executable": True,
        "exit_code": 0,
    }

    wrong_process = tmp_path / "installed" / "Other.exe"
    wrong_process.write_bytes(b"other process")
    with pytest.raises(
        ArtifactIdentityError, match="OS-reported process executable path"
    ):
        launch_exact_artifact(
            archive_path=archive,
            expected_archive_sha256=sha256_file(archive),
            executable_path=executable,
            expected_executable_sha256=sha256_file(executable),
            archive_member="Package/App.exe",
            evidence_json=tmp_path / "evidence" / "wrong-process.json",
            popen_factory=fake_popen,
            query_process_path=lambda _pid: wrong_process,
        )

    with pytest.raises(ArtifactIdentityError, match="archive SHA-256 mismatch"):
        launch_exact_artifact(
            archive_path=archive,
            expected_archive_sha256="0" * 64,
            executable_path=executable,
            expected_executable_sha256=sha256_file(executable),
            archive_member="Package/App.exe",
            evidence_json=tmp_path / "evidence" / "blocked.json",
            popen_factory=fake_popen,
            query_process_path=lambda _pid: executable,
        )
    assert len(launches) == 2

    contract = (ROOT / "RELEASE_GATE_CONTRACT.md").read_text(encoding="utf-8")
    assert "`READY_COMPONENT_WRITE` supporting evidence" in contract
    assert "tools/run_test1_exact_artifact.py" in contract
