# Reviewed M7 capture validator

`tools/validate_capture_bundle_v1.py` is the reviewed reconstruction accepted on
2026-09-06, vendored without byte changes. Its SHA-256 is
`86713b77bd004a3577b53221be8667362d67984f915395dec95727ca5b1235e1`.
The accepted 21-file handoff manifest is
`e7b6f69ba1aa220904881eb3f10b8486021871539411806aa09e9ca0f95cf880`.

[SPEC.md](SPEC.md) and [PROVENANCE.md](PROVENANCE.md) preserve the original
reconstruction contract, decisions and historical limitations verbatim.
Their candidate/integration-pending wording records the handoff's original
state; this README records the subsequent Label source integration.
Structural PASS does not prove real GUI capture, organizational authority,
source provenance, a runnable release, or native readiness.

Both the original consumer tests and `tools/publish_outline_user_manual.py`
resolve the validator from this repository's `tools` directory. Publishing
still requires validator exit 0/PASS with zero failures and zero pending
approvals. `M7_CANONICAL_HANDOVER_DIR` and its report-write prohibition retain
the historical path and policy; no old capture or receipt is migrated.

Runtime support is Python 3.11+ and Pillow for read-only PNG decoding. The
validator imports no application modules, starts no subprocess, opens no
network connection and writes no evidence. It is a repository publishing tool;
neither the publisher nor validator is a member of the application's portable
tool closure. This integration does not add Pillow to the zero-PE application
package or alter its native dependency guard.

`tests/capture_validator/fixture_contract.py` preserves the accepted independent
fixture bytes. `test_contract.py` preserves all 135 independent cases and real
subprocess assertions, adapting only its relative fixture import and validator
path for the new nesting. The original nine Label consumer cases now execute
instead of skipping for the missing external file. All historic skipped results
remain historical evidence; only a new authorized complete run can establish
current full-suite counts.

Run focused checks with temporary/cache/profile roots and pytest basetemp under
a named E: task root, `PYTHONDONTWRITEBYTECODE=1`, and no visible host windows.
Tests elsewhere that need mapped Tk/focus remain for a scheduled guest.
