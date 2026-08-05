---
type: Rust Function
title: handle_smtp_session
resource: LPE-CT/src/smtp/session.rs#L37-L119
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/readiness/ha_non_active_role_for_traffic
  - functions/LPE-CT/src/smtp/protocol/write_smtp
  - functions/LPE-CT/src/observability/record_smtp_session
  - functions/LPE-CT/src/smtp/runtime_config_from_store
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
  - functions/LPE-CT/src/smtp/session/run_smtp_command_loop
  - functions/LPE-CT/src/smtp/session/SmtpTransaction/after_starttls
  called_by:
  - functions/LPE-CT/src/smtp/run_smtp_listener
  - functions/LPE-CT/src/smtp/tests/smtp_session_accepts_lpe_domain_and_rejects_external_relay_domain
  - functions/LPE-CT/src/smtp/tests/smtp_starttls_upgrades_to_tls_after_ready_reply
  - functions/LPE-CT/src/smtp/tests/smtp_session_rejects_when_ha_role_is_standby
---

# Signature

`pub(in crate::smtp) async fn handle_smtp_session( stream: TcpStream, peer: SocketAddr, dashboard_store: Arc<Mutex<crate::DashboardState>>, spool_dir: PathBuf, starttls: Option<TlsAcceptor>, ) -> Result<()>`

# Calls

- [ha_non_active_role_for_traffic](../../../../../functions/LPE-CT/src/readiness/ha_non_active_role_for_traffic.md)
- [write_smtp](../../../../../functions/LPE-CT/src/smtp/protocol/write_smtp.md)
- [record_smtp_session](../../../../../functions/LPE-CT/src/observability/record_smtp_session.md)
- [runtime_config_from_store](../../../../../functions/LPE-CT/src/smtp/runtime_config_from_store.md)
- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)
- [run_smtp_command_loop](../../../../../functions/LPE-CT/src/smtp/session/run_smtp_command_loop.md)
- [after_starttls](../../../../../functions/LPE-CT/src/smtp/session/SmtpTransaction/after_starttls.md)

# Called by

- [run_smtp_listener](../../../../../functions/LPE-CT/src/smtp/run_smtp_listener.md)
- [smtp_session_accepts_lpe_domain_and_rejects_external_relay_domain](../../../../../functions/LPE-CT/src/smtp/tests/smtp_session_accepts_lpe_domain_and_rejects_external_relay_domain.md)
- [smtp_starttls_upgrades_to_tls_after_ready_reply](../../../../../functions/LPE-CT/src/smtp/tests/smtp_starttls_upgrades_to_tls_after_ready_reply.md)
- [smtp_session_rejects_when_ha_role_is_standby](../../../../../functions/LPE-CT/src/smtp/tests/smtp_session_rejects_when_ha_role_is_standby.md)