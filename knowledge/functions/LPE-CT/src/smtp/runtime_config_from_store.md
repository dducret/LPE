---
type: Rust Function
title: runtime_config_from_store
resource: LPE-CT/src/smtp.rs#L695-L703
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/runtime_config_from_dashboard
  called_by:
  - functions/LPE-CT/src/smtp/run_smtp_listener
  - functions/LPE-CT/src/smtp/session/handle_smtp_session
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
  - functions/LPE-CT/src/submission/handle_submission_session
---

# Signature

`pub(crate) fn runtime_config_from_store( dashboard_store: &Arc<Mutex<super::DashboardState>>, ) -> Result<RuntimeConfig>`

# Calls

- [runtime_config_from_dashboard](../../../../functions/LPE-CT/src/smtp/runtime_config_from_dashboard.md)

# Called by

- [run_smtp_listener](../../../../functions/LPE-CT/src/smtp/run_smtp_listener.md)
- [handle_smtp_session](../../../../functions/LPE-CT/src/smtp/session/handle_smtp_session.md)
- [handle_smtp_command](../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)
- [handle_submission_session](../../../../functions/LPE-CT/src/submission/handle_submission_session.md)