---
type: Rust Function
title: record_smtp_session
resource: LPE-CT/src/observability.rs#L145-L152
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/LPE-CT/src/smtp/run_smtp_listener
  - functions/LPE-CT/src/smtp/session/handle_smtp_session
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
---

# Signature

`pub fn record_smtp_session(result: &str)`

# Calls

- [entry](../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [run_smtp_listener](../../../../functions/LPE-CT/src/smtp/run_smtp_listener.md)
- [handle_smtp_session](../../../../functions/LPE-CT/src/smtp/session/handle_smtp_session.md)
- [receive_message_with_validator](../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)