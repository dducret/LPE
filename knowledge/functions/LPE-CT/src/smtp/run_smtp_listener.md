---
type: Rust Function
title: run_smtp_listener
resource: LPE-CT/src/smtp.rs#L490-L541
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/runtime_config_from_store
  - functions/LPE-CT/src/observability/set_active_smtp_sessions
  - functions/LPE-CT/web/app/load
  - functions/LPE-CT/src/observability/record_smtp_backpressure
  - functions/LPE-CT/src/smtp/protocol/write_smtp
  - functions/LPE-CT/src/smtp/tls/smtp_starttls_acceptor_from_store
  - functions/LPE-CT/src/smtp/session/handle_smtp_session
  - functions/LPE-CT/src/observability/record_smtp_session
  called_by:
  - functions/LPE-CT/src/main
---

# Signature

`pub(crate) async fn run_smtp_listener( bind_address: String, dashboard_store: Arc<Mutex<super::DashboardState>>, spool_dir: PathBuf, ) -> Result<()>`

# Calls

- [runtime_config_from_store](../../../../functions/LPE-CT/src/smtp/runtime_config_from_store.md)
- [set_active_smtp_sessions](../../../../functions/LPE-CT/src/observability/set_active_smtp_sessions.md)
- [load](../../../../functions/LPE-CT/web/app/load.md)
- [record_smtp_backpressure](../../../../functions/LPE-CT/src/observability/record_smtp_backpressure.md)
- [write_smtp](../../../../functions/LPE-CT/src/smtp/protocol/write_smtp.md)
- [smtp_starttls_acceptor_from_store](../../../../functions/LPE-CT/src/smtp/tls/smtp_starttls_acceptor_from_store.md)
- [handle_smtp_session](../../../../functions/LPE-CT/src/smtp/session/handle_smtp_session.md)
- [record_smtp_session](../../../../functions/LPE-CT/src/observability/record_smtp_session.md)

# Called by

- [main](../../../../functions/LPE-CT/src/main.md)