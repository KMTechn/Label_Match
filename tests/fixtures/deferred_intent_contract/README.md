# Vendored deferred-intent test contract

The contract, vectors, generator, CHANGES.md, PROVENANCE.json and SHA256SUMS.txt
are exact copies of Main's 2026-09-06 shared publication revision 2, accepted
for Label consumption in `msg_8f5a3be5b193`. SHA256SUMS.txt pins the four main artifacts;
`tests/test_deferred_intent_capture.py` independently pins the two consumed files.
The lease-clock regression module imports that test module's helpers and uses
the same repository-local fixtures. Tests need no external drive or Rework tree.

The seq292 originals were lost during storage cleanup. These are authorized
reconstructions with new pins, not restored original bytes or new native
measurements. CONTRACT.md retains the upstream source hashes, explicit generation
time and evidence limits; historical MEASURED case IDs denote synthetic scenarios.

The generator is retained unchanged for provenance. Reproduction requires the
exact Rework source inputs recorded in CONTRACT.md: pass their repository root
using `--repo-root`, an independent `--output-dir`, and the recorded source commit
and generation time. It is not a Label runtime tool. Do not regenerate these
fixtures from Label code or update their pins without reviewing a new shared
publication. Git attributes disable text conversion for this directory so a
Windows checkout preserves the published bytes.
