"""External test-only OS/credential adapter; never copied into a stock package."""
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
from types import SimpleNamespace

assert os.environ.get("KMTECH_LABEL_WRITER_TEST_MODE") == "1"
if not globals().get("lifecycle_only"):
    app_root = Path(sys.argv[sys.argv.index("--app-root") + 1])
sys.path[:0] = [str(app_root / "app"), str(app_root / "app/site-packages")]
import current_user_onboarding as onboarding


def credential(path):
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if value.get("secret_ref") != "dpapi:fixture-current-user":
        raise ValueError("fixture current-user DPAPI owner mismatch")
    return SimpleNamespace(**value)


onboarding.inspect_current_user_state.__kwdefaults__.update(
    credential_loader=credential,
    profile_loader=lambda path: SimpleNamespace(**json.loads(Path(path).read_text(encoding="utf-8"))),
)
onboarding.load_credentials_from_json = credential
onboarding.CANONICAL_PORTABLE_ROOT = app_root

if not globals().get("lifecycle_only"):
    real_popen = subprocess.Popen

    def start_process(arguments, *args, **kwargs):
        if isinstance(arguments, list) and "--label-match-user-relay" in arguments:
            arguments = list(arguments)
            arguments[arguments.index(str(app_root / "app/main.py"))] = __file__
            arguments += ["--app-root", str(app_root)]
        return real_popen(arguments, *args, **kwargs)

    subprocess.Popen = start_process
    runpy.run_path(str(Path(__file__).with_name("_writer_transition_native.py")))
    runpy.run_path(str(app_root / "app/main.py"), run_name="__main__")
