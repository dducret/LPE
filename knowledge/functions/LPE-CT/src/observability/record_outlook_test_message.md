---
type: Rust Function
title: record_outlook_test_message
resource: LPE-CT/src/observability.rs#L154-L161
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
  - functions/LPE-CT/src/submission/handle_submission_session
---

# Signature

`pub fn record_outlook_test_message(path: &str, status: &str)`

# Calls

- [entry](../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [receive_message_with_validator](../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)
- [handle_submission_session](../../../../functions/LPE-CT/src/submission/handle_submission_session.md)