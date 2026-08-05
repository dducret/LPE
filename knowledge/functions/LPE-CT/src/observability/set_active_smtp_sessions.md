---
type: Rust Function
title: set_active_smtp_sessions
resource: LPE-CT/src/observability.rs#L169-L173
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/run_smtp_listener
---

# Signature

`pub fn set_active_smtp_sessions(value: u32)`

# Called by

- [run_smtp_listener](../../../../functions/LPE-CT/src/smtp/run_smtp_listener.md)