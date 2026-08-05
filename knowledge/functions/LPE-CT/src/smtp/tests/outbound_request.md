---
type: Rust Function
title: outbound_request
resource: LPE-CT/src/smtp/tests.rs#L3459-L3482
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay
  - functions/LPE-CT/src/smtp/tests/outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay
  - functions/LPE-CT/src/smtp/tests/terminal_outbound_custody_queues_do_not_regress_after_restart
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_bounces_on_permanent_rcpt_failure
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_defers_when_local_throttle_hits
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_uses_matching_routing_rule
  - functions/LPE-CT/src/smtp/tests/outbound_route_without_smart_host_uses_direct_mx_default
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_delivers_accepted_domain_locally_without_direct_mx
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_emits_sender_header_for_delegated_sender
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_on_bayespam_score
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_rejects_blocked_delegated_sender
  - functions/LPE-CT/src/smtp/tests/benchmark_relay_hot_path
---

# Signature

`fn outbound_request(subject: &str) -> OutboundMessageHandoffRequest`

# Called by

- [outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay.md)
- [outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay](../../../../../functions/LPE-CT/src/smtp/tests/outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay.md)
- [terminal_outbound_custody_queues_do_not_regress_after_restart](../../../../../functions/LPE-CT/src/smtp/tests/terminal_outbound_custody_queues_do_not_regress_after_restart.md)
- [outbound_handoff_bounces_on_permanent_rcpt_failure](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_bounces_on_permanent_rcpt_failure.md)
- [outbound_handoff_defers_when_local_throttle_hits](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_defers_when_local_throttle_hits.md)
- [outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody.md)
- [outbound_handoff_uses_matching_routing_rule](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_uses_matching_routing_rule.md)
- [outbound_route_without_smart_host_uses_direct_mx_default](../../../../../functions/LPE-CT/src/smtp/tests/outbound_route_without_smart_host_uses_direct_mx_default.md)
- [outbound_handoff_delivers_accepted_domain_locally_without_direct_mx](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_delivers_accepted_domain_locally_without_direct_mx.md)
- [outbound_handoff_emits_sender_header_for_delegated_sender](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_emits_sender_header_for_delegated_sender.md)
- [outbound_handoff_quarantines_on_bayespam_score](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_on_bayespam_score.md)
- [outbound_handoff_rejects_blocked_delegated_sender](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_rejects_blocked_delegated_sender.md)
- [benchmark_relay_hot_path](../../../../../functions/LPE-CT/src/smtp/tests/benchmark_relay_hot_path.md)