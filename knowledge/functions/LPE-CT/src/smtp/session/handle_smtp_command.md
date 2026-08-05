---
type: Rust Function
title: handle_smtp_command
resource: LPE-CT/src/smtp/session.rs#L164-L375
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/protocol/write_smtp
  - functions/LPE-CT/src/smtp/runtime_config_from_store
  - functions/LPE-CT/src/smtp/session/SmtpTransaction/requires_greeting
  - functions/LPE-CT/src/smtp/protocol/parse_smtp_path
  - functions/LPE-CT/src/smtp/protocol/max_smtp_message_size_bytes
  - functions/LPE-CT/src/smtp/protocol/smtp_path_error_reply
  - functions/LPE-CT/src/transport_policy/evaluate_address_policy_with_config
  - functions/LPE-CT/src/smtp/policy/recipient_domain_is_accepted
  - functions/LPE-CT/src/smtp/policy/recipient_domain_accepts_null_reverse_path
  - functions/LPE-CT/src/transport_policy/verify_recipient_with_core
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/smtp/protocol/read_smtp_data
  - functions/LPE-CT/src/smtp/session/receive_message
  - functions/LPE-CT/src/smtp/dsn/rejected_smtp_reply
  - functions/LPE-CT/src/smtp/dsn/deferred_smtp_reply
  called_by:
  - functions/LPE-CT/src/smtp/session/handle_smtp_session
  - functions/LPE-CT/src/smtp/session/run_smtp_command_loop
  - functions/LPE-CT/src/smtp/tests/smtp_mail_from_rejects_malformed_paths_unsupported_params_and_size_overflow
  - functions/LPE-CT/src/smtp/tests/smtp_rcpt_to_rejects_malformed_paths_and_unsupported_params
  - functions/LPE-CT/src/smtp/tests/smtp_rcpt_to_enforces_transaction_recipient_limit
  - functions/LPE-CT/src/smtp/tests/smtp_long_command_line_returns_line_length_error
  - functions/LPE-CT/src/smtp/tests/smtp_command_sequence_requires_mail_and_recipient_before_data
  - functions/LPE-CT/src/smtp/tests/smtp_rcpt_accepts_configured_domain_and_rejects_external_relay_domain
  - functions/LPE-CT/src/smtp/tests/smtp_null_reverse_path_is_controlled_per_recipient_domain
  - functions/LPE-CT/src/smtp/tests/smtp_data_accepts_plaintext_for_local_domain_and_hands_to_core
  - functions/LPE-CT/src/smtp/tests/smtp_data_accepts_null_reverse_path_for_dsn_delivery
  - functions/LPE-CT/src/smtp/tests/smtp_data_defers_with_trace_when_core_delivery_is_unavailable
  - functions/LPE-CT/src/smtp/tests/smtp_unknown_local_recipient_core_rejection_defers_without_backscatter_bounce
  - functions/LPE-CT/src/smtp/tests/smtp_data_rejects_with_policy_reason_and_trace
  - functions/LPE-CT/src/smtp/tests/smtp_ehlo_advertises_starttls_when_tls_is_available
  - functions/LPE-CT/src/smtp/tests/smtp_ehlo_does_not_advertise_starttls_without_tls_config
  - functions/LPE-CT/src/smtp/tests/smtp_public_ingress_does_not_advertise_or_accept_auth
  - functions/LPE-CT/src/smtp/tests/smtp_starttls_requires_ehlo_or_helo_first
  - functions/LPE-CT/src/smtp/tests/smtp_ehlo_does_not_advertise_starttls_after_tls_upgrade
---

# Signature

`pub(in crate::smtp) async fn handle_smtp_command<R, W>( client: &reqwest::Client, reader: &mut BufReader<R>, writer: &mut W, dashboard_store: &Arc<Mutex<crate::DashboardState>>, spool_dir: &Path, peer: SocketAddr, transaction: &mut SmtpTransaction, command: &str, starttls_available: bool, ) -> Result<SmtpCommandOutcome> where R: tokio::io::AsyncRead + Unpin, W: AsyncWrite + Unpin,`

# Calls

- [write_smtp](../../../../../functions/LPE-CT/src/smtp/protocol/write_smtp.md)
- [runtime_config_from_store](../../../../../functions/LPE-CT/src/smtp/runtime_config_from_store.md)
- [requires_greeting](../../../../../functions/LPE-CT/src/smtp/session/SmtpTransaction/requires_greeting.md)
- [parse_smtp_path](../../../../../functions/LPE-CT/src/smtp/protocol/parse_smtp_path.md)
- [max_smtp_message_size_bytes](../../../../../functions/LPE-CT/src/smtp/protocol/max_smtp_message_size_bytes.md)
- [smtp_path_error_reply](../../../../../functions/LPE-CT/src/smtp/protocol/smtp_path_error_reply.md)
- [evaluate_address_policy_with_config](../../../../../functions/LPE-CT/src/transport_policy/evaluate_address_policy_with_config.md)
- [recipient_domain_is_accepted](../../../../../functions/LPE-CT/src/smtp/policy/recipient_domain_is_accepted.md)
- [recipient_domain_accepts_null_reverse_path](../../../../../functions/LPE-CT/src/smtp/policy/recipient_domain_accepts_null_reverse_path.md)
- [verify_recipient_with_core](../../../../../functions/LPE-CT/src/transport_policy/verify_recipient_with_core.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [read_smtp_data](../../../../../functions/LPE-CT/src/smtp/protocol/read_smtp_data.md)
- [receive_message](../../../../../functions/LPE-CT/src/smtp/session/receive_message.md)
- [rejected_smtp_reply](../../../../../functions/LPE-CT/src/smtp/dsn/rejected_smtp_reply.md)
- [deferred_smtp_reply](../../../../../functions/LPE-CT/src/smtp/dsn/deferred_smtp_reply.md)

# Called by

- [handle_smtp_session](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_session.md)
- [run_smtp_command_loop](../../../../../functions/LPE-CT/src/smtp/session/run_smtp_command_loop.md)
- [smtp_mail_from_rejects_malformed_paths_unsupported_params_and_size_overflow](../../../../../functions/LPE-CT/src/smtp/tests/smtp_mail_from_rejects_malformed_paths_unsupported_params_and_size_overflow.md)
- [smtp_rcpt_to_rejects_malformed_paths_and_unsupported_params](../../../../../functions/LPE-CT/src/smtp/tests/smtp_rcpt_to_rejects_malformed_paths_and_unsupported_params.md)
- [smtp_rcpt_to_enforces_transaction_recipient_limit](../../../../../functions/LPE-CT/src/smtp/tests/smtp_rcpt_to_enforces_transaction_recipient_limit.md)
- [smtp_long_command_line_returns_line_length_error](../../../../../functions/LPE-CT/src/smtp/tests/smtp_long_command_line_returns_line_length_error.md)
- [smtp_command_sequence_requires_mail_and_recipient_before_data](../../../../../functions/LPE-CT/src/smtp/tests/smtp_command_sequence_requires_mail_and_recipient_before_data.md)
- [smtp_rcpt_accepts_configured_domain_and_rejects_external_relay_domain](../../../../../functions/LPE-CT/src/smtp/tests/smtp_rcpt_accepts_configured_domain_and_rejects_external_relay_domain.md)
- [smtp_null_reverse_path_is_controlled_per_recipient_domain](../../../../../functions/LPE-CT/src/smtp/tests/smtp_null_reverse_path_is_controlled_per_recipient_domain.md)
- [smtp_data_accepts_plaintext_for_local_domain_and_hands_to_core](../../../../../functions/LPE-CT/src/smtp/tests/smtp_data_accepts_plaintext_for_local_domain_and_hands_to_core.md)
- [smtp_data_accepts_null_reverse_path_for_dsn_delivery](../../../../../functions/LPE-CT/src/smtp/tests/smtp_data_accepts_null_reverse_path_for_dsn_delivery.md)
- [smtp_data_defers_with_trace_when_core_delivery_is_unavailable](../../../../../functions/LPE-CT/src/smtp/tests/smtp_data_defers_with_trace_when_core_delivery_is_unavailable.md)
- [smtp_unknown_local_recipient_core_rejection_defers_without_backscatter_bounce](../../../../../functions/LPE-CT/src/smtp/tests/smtp_unknown_local_recipient_core_rejection_defers_without_backscatter_bounce.md)
- [smtp_data_rejects_with_policy_reason_and_trace](../../../../../functions/LPE-CT/src/smtp/tests/smtp_data_rejects_with_policy_reason_and_trace.md)
- [smtp_ehlo_advertises_starttls_when_tls_is_available](../../../../../functions/LPE-CT/src/smtp/tests/smtp_ehlo_advertises_starttls_when_tls_is_available.md)
- [smtp_ehlo_does_not_advertise_starttls_without_tls_config](../../../../../functions/LPE-CT/src/smtp/tests/smtp_ehlo_does_not_advertise_starttls_without_tls_config.md)
- [smtp_public_ingress_does_not_advertise_or_accept_auth](../../../../../functions/LPE-CT/src/smtp/tests/smtp_public_ingress_does_not_advertise_or_accept_auth.md)
- [smtp_starttls_requires_ehlo_or_helo_first](../../../../../functions/LPE-CT/src/smtp/tests/smtp_starttls_requires_ehlo_or_helo_first.md)
- [smtp_ehlo_does_not_advertise_starttls_after_tls_upgrade](../../../../../functions/LPE-CT/src/smtp/tests/smtp_ehlo_does_not_advertise_starttls_after_tls_upgrade.md)