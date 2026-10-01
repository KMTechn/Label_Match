"""Keep default writer admission isolated across pytest runs and their children."""

import pytest


@pytest.fixture(scope="session", autouse=True)
def _isolated_writer_control_root(tmp_path_factory):
    # Children inherit this root; competing writers still share its real mutex.
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("KMTECH_LABEL_WRITER_TEST_MODE", "1")
        patch.setenv(
            "KMTECH_LABEL_WRITER_CONTROL_ROOT",
            str(tmp_path_factory.getbasetemp() / "writer-control"),
        )
        yield
