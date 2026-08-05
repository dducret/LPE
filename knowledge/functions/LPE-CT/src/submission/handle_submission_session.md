---
type: Rust Function
title: handle_submission_session
resource: LPE-CT/src/submission.rs#L101-L344
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/readiness/ha_non_active_role_for_traffic
  - functions/LPE-CT/src/submission/write_line
  - functions/LPE-CT/src/submission/authenticate_smtp_client
  - functions/LPE-CT/src/submission/smtp_auth_failure_reply
  - functions/LPE-CT/src/smtp/runtime_config_from_store
  - functions/LPE-CT/src/smtp/protocol/parse_smtp_path
  - functions/LPE-CT/src/smtp/protocol/max_smtp_message_size_bytes
  - functions/LPE-CT/src/smtp/protocol/smtp_path_error_reply
  - functions/LPE-CT/src/transport_policy/evaluate_address_policy_with_config
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/submission/read_data
  - functions/LPE-CT/src/outlook_test_message/classify_smtp_message
  - functions/LPE-CT/src/observability/record_outlook_test_message
  - functions/LPE-CT/src/transport_policy/evaluate_attachment_policy_with_config
  - functions/LPE-CT/src/submission/smtp_submission_failure_reply
  called_by:
  - functions/LPE-CT/src/submission/run_submission_listener
---

# Signature

`async fn handle_submission_session( stream: TcpStream, peer: SocketAddr, tls: TlsAcceptor, client: reqwest::Client, core_base_url: String, dashboard_store: Arc<Mutex<crate::DashboardState>>, ) -> Result<()>`

# Calls

- [ha_non_active_role_for_traffic](../../../../functions/LPE-CT/src/readiness/ha_non_active_role_for_traffic.md)
- [write_line](../../../../functions/LPE-CT/src/submission/write_line.md)
- [authenticate_smtp_client](../../../../functions/LPE-CT/src/submission/authenticate_smtp_client.md)
- [smtp_auth_failure_reply](../../../../functions/LPE-CT/src/submission/smtp_auth_failure_reply.md)
- [runtime_config_from_store](../../../../functions/LPE-CT/src/smtp/runtime_config_from_store.md)
- [parse_smtp_path](../../../../functions/LPE-CT/src/smtp/protocol/parse_smtp_path.md)
- [max_smtp_message_size_bytes](../../../../functions/LPE-CT/src/smtp/protocol/max_smtp_message_size_bytes.md)
- [smtp_path_error_reply](../../../../functions/LPE-CT/src/smtp/protocol/smtp_path_error_reply.md)
- [evaluate_address_policy_with_config](../../../../functions/LPE-CT/src/transport_policy/evaluate_address_policy_with_config.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [read_data](../../../../functions/LPE-CT/src/submission/read_data.md)
- [classify_smtp_message](../../../../functions/LPE-CT/src/outlook_test_message/classify_smtp_message.md)
- [record_outlook_test_message](../../../../functions/LPE-CT/src/observability/record_outlook_test_message.md)
- [evaluate_attachment_policy_with_config](../../../../functions/LPE-CT/src/transport_policy/evaluate_attachment_policy_with_config.md)
- [smtp_submission_failure_reply](../../../../functions/LPE-CT/src/submission/smtp_submission_failure_reply.md)

# Called by

- [run_submission_listener](../../../../functions/LPE-CT/src/submission/run_submission_listener.md)