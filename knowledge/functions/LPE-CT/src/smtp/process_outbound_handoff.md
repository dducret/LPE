---
type: Rust Function
title: process_outbound_handoff
resource: LPE-CT/src/smtp.rs#L718-L1168
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/queue_store/find_message
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/smtp/audit/append_transport_audit
  - functions/LPE-CT/src/smtp/outbound_policy/outbound_handoff_response_from_spool
  - functions/LPE-CT/src/smtp/outbound_policy/resolve_outbound_route
  - functions/LPE-CT/src/dkim_signing/maybe_sign_outbound_message
  - functions/LPE-CT/src/smtp/outbound/compose_rfc822_message
  - functions/crates/lpe-domain/src/transport/OutboundMessageHandoffRequest/envelope_recipients
  - functions/LPE-CT/src/smtp/evaluate_outbound_sender_policy
  - functions/LPE-CT/src/smtp/queue_store/persist_message
  - functions/LPE-CT/src/transport_policy/evaluate_address_policy_with_config
  - functions/LPE-CT/src/smtp/bayes/score_bayespam
  - functions/LPE-CT/src/smtp/antivirus/evaluate_antivirus_policy
  - functions/LPE-CT/src/smtp/queue_store/move_message
  - functions/LPE-CT/src/smtp/quarantine/persist_quarantine_metadata_or_warn
  - functions/LPE-CT/src/smtp/should_quarantine
  - functions/LPE-CT/src/smtp/outbound_policy/evaluate_outbound_throttle
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message
  - functions/LPE-CT/src/smtp/outbound_policy/default_queue_for_status
  called_by:
  - functions/LPE-CT/src/http_routes/outbound_handoff
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_relays_message
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay
  - functions/LPE-CT/src/smtp/tests/outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay
  - functions/LPE-CT/src/smtp/tests/terminal_outbound_custody_queues_do_not_regress_after_restart
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_message
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_bounces_on_permanent_rcpt_failure
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_defers_when_local_throttle_hits
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_uses_matching_routing_rule
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_delivers_accepted_domain_locally_without_direct_mx
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_on_bayespam_score
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_rejects_blocked_delegated_sender
  - functions/LPE-CT/src/smtp/tests/benchmark_relay_hot_path
---

# Signature

`pub(crate) async fn process_outbound_handoff( spool_dir: &Path, config: &RuntimeConfig, payload: OutboundMessageHandoffRequest, ) -> Result<OutboundMessageHandoffResponse>`

# Calls

- [find_message](../../../../functions/LPE-CT/src/smtp/queue_store/find_message.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [append_transport_audit](../../../../functions/LPE-CT/src/smtp/audit/append_transport_audit.md)
- [outbound_handoff_response_from_spool](../../../../functions/LPE-CT/src/smtp/outbound_policy/outbound_handoff_response_from_spool.md)
- [resolve_outbound_route](../../../../functions/LPE-CT/src/smtp/outbound_policy/resolve_outbound_route.md)
- [maybe_sign_outbound_message](../../../../functions/LPE-CT/src/dkim_signing/maybe_sign_outbound_message.md)
- [compose_rfc822_message](../../../../functions/LPE-CT/src/smtp/outbound/compose_rfc822_message.md)
- [envelope_recipients](../../../../functions/crates/lpe-domain/src/transport/OutboundMessageHandoffRequest/envelope_recipients.md)
- [evaluate_outbound_sender_policy](../../../../functions/LPE-CT/src/smtp/evaluate_outbound_sender_policy.md)
- [persist_message](../../../../functions/LPE-CT/src/smtp/queue_store/persist_message.md)
- [evaluate_address_policy_with_config](../../../../functions/LPE-CT/src/transport_policy/evaluate_address_policy_with_config.md)
- [score_bayespam](../../../../functions/LPE-CT/src/smtp/bayes/score_bayespam.md)
- [evaluate_antivirus_policy](../../../../functions/LPE-CT/src/smtp/antivirus/evaluate_antivirus_policy.md)
- [move_message](../../../../functions/LPE-CT/src/smtp/queue_store/move_message.md)
- [persist_quarantine_metadata_or_warn](../../../../functions/LPE-CT/src/smtp/quarantine/persist_quarantine_metadata_or_warn.md)
- [should_quarantine](../../../../functions/LPE-CT/src/smtp/should_quarantine.md)
- [evaluate_outbound_throttle](../../../../functions/LPE-CT/src/smtp/outbound_policy/evaluate_outbound_throttle.md)
- [relay_message](../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message.md)
- [default_queue_for_status](../../../../functions/LPE-CT/src/smtp/outbound_policy/default_queue_for_status.md)

# Called by

- [outbound_handoff](../../../../functions/LPE-CT/src/http_routes/outbound_handoff.md)
- [outbound_handoff_relays_message](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_relays_message.md)
- [outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay.md)
- [outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay](../../../../functions/LPE-CT/src/smtp/tests/outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay.md)
- [terminal_outbound_custody_queues_do_not_regress_after_restart](../../../../functions/LPE-CT/src/smtp/tests/terminal_outbound_custody_queues_do_not_regress_after_restart.md)
- [outbound_handoff_quarantines_message](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_message.md)
- [outbound_handoff_bounces_on_permanent_rcpt_failure](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_bounces_on_permanent_rcpt_failure.md)
- [outbound_handoff_defers_when_local_throttle_hits](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_defers_when_local_throttle_hits.md)
- [outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody.md)
- [outbound_handoff_uses_matching_routing_rule](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_uses_matching_routing_rule.md)
- [outbound_handoff_delivers_accepted_domain_locally_without_direct_mx](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_delivers_accepted_domain_locally_without_direct_mx.md)
- [outbound_handoff_quarantines_on_bayespam_score](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_on_bayespam_score.md)
- [outbound_handoff_rejects_blocked_delegated_sender](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_rejects_blocked_delegated_sender.md)
- [benchmark_relay_hot_path](../../../../functions/LPE-CT/src/smtp/tests/benchmark_relay_hot_path.md)