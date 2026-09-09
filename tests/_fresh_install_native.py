"""Exercise ordinary onboarding/registration with only OS and HTTP boundaries replaced."""
import os
from pathlib import Path
import runpy
import sys

runpy.run_path(str(Path(__file__).with_name("_writer_transition_native.py")))

if "--onboard-current-user" in sys.argv:
    import current_user_onboarding as onboarding
    from tools import register_label_match_worker_pc as registration

    # Only shared key data comes from the checkout; the isolated child uses
    # placed production modules and does not require pytest.
    fixtures = runpy.run_path(str(Path(__file__).with_name("_possession_fixture.py")))
    registration._prepare_possession_key = lambda _report: fixtures["fake_possession_descriptor"]()

    def enroll(_payload, **_kwargs):
        code = os.environ["LM_FRESH_ENROLLMENT_ERROR"]
        raise registration.ProducerEnrollmentHTTPError(
            409 if code == "producer_identity_conflict" else 401, code, ""
        )

    registration._enroll = enroll
    onboarding.onboard_current_user.__wrapped__.__kwdefaults__["legacy_task_quiescence_reader"] = lambda: {
        "schema": "label-match-legacy-task-quiescence-v1", "status": "PASS",
        "required_state": "ABSENT_OR_DISABLED", "read_only": True,
        "task_or_process_mutated": False,
    }
