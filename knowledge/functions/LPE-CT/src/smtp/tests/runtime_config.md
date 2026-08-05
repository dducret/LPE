---
type: Rust Function
title: runtime_config
resource: LPE-CT/src/smtp/tests.rs#L150-L205
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/antivirus/load_antivirus_providers
  called_by:
  - functions/LPE-CT/src/smtp/tests/recipient_domain_acceptance_is_exact_case_insensitive_and_verified
  - functions/LPE-CT/src/smtp/tests/smtp_ingress_marks_outlook_account_test_message
  - functions/LPE-CT/src/smtp/tests/inbound_delivery_keeps_durable_spool_custody_until_core_accepts
  - functions/LPE-CT/src/smtp/tests/inbound_bridge_failure_keeps_deferred_custody_with_audit
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_relays_message
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay
  - functions/LPE-CT/src/smtp/tests/outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay
  - functions/LPE-CT/src/smtp/tests/terminal_outbound_custody_queues_do_not_regress_after_restart
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_message
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_bounces_on_permanent_rcpt_failure
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_defers_when_local_throttle_hits
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_uses_matching_routing_rule
  - functions/LPE-CT/src/smtp/tests/outbound_route_without_smart_host_uses_direct_mx_default
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_delivers_accepted_domain_locally_without_direct_mx
  - functions/LPE-CT/src/smtp/tests/inbound_message_posts_to_core_delivery_api
  - functions/LPE-CT/src/smtp/tests/inbound_magika_failure_is_quarantined
  - functions/LPE-CT/src/smtp/tests/inbound_message_keeps_non_utf8_raw_bytes
  - functions/LPE-CT/src/smtp/tests/greylisting_defers_first_triplet_then_allows_after_release_window
  - functions/LPE-CT/src/smtp/tests/reputation_score_penalizes_quarantine_and_rejects
  - functions/LPE-CT/src/smtp/tests/bayespam_learns_tokens_and_scores_spammy_message
  - functions/LPE-CT/src/smtp/tests/bayespam_requires_enough_content_evidence_before_contributing
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_on_bayespam_score
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_rejects_blocked_delegated_sender
  - functions/LPE-CT/src/smtp/tests/retry_trace_clears_stale_execution_state_and_appends_audit
  - functions/LPE-CT/src/smtp/tests/release_trace_moves_quarantined_inbound_back_to_incoming_and_appends_audit
  - functions/LPE-CT/src/smtp/tests/rejected_quarantine_trace_recovers_from_spool_until_operator_delete
  - functions/LPE-CT/src/smtp/tests/quarantine_release_reject_delete_recovers_across_node_replacement
  - functions/LPE-CT/src/smtp/tests/delete_trace_removes_held_queue_items
  - functions/LPE-CT/src/smtp/tests/delete_trace_rejects_sent_history_items
  - functions/LPE-CT/src/smtp/tests/auth_policy_config
  - functions/LPE-CT/src/smtp/tests/benchmark_relay_hot_path
---

# Signature

`fn runtime_config(primary_upstream: String, core_delivery_base_url: String) -> RuntimeConfig`

# Calls

- [load_antivirus_providers](../../../../../functions/LPE-CT/src/smtp/antivirus/load_antivirus_providers.md)

# Called by

- [recipient_domain_acceptance_is_exact_case_insensitive_and_verified](../../../../../functions/LPE-CT/src/smtp/tests/recipient_domain_acceptance_is_exact_case_insensitive_and_verified.md)
- [smtp_ingress_marks_outlook_account_test_message](../../../../../functions/LPE-CT/src/smtp/tests/smtp_ingress_marks_outlook_account_test_message.md)
- [inbound_delivery_keeps_durable_spool_custody_until_core_accepts](../../../../../functions/LPE-CT/src/smtp/tests/inbound_delivery_keeps_durable_spool_custody_until_core_accepts.md)
- [inbound_bridge_failure_keeps_deferred_custody_with_audit](../../../../../functions/LPE-CT/src/smtp/tests/inbound_bridge_failure_keeps_deferred_custody_with_audit.md)
- [outbound_handoff_relays_message](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_relays_message.md)
- [outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay.md)
- [outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay](../../../../../functions/LPE-CT/src/smtp/tests/outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay.md)
- [terminal_outbound_custody_queues_do_not_regress_after_restart](../../../../../functions/LPE-CT/src/smtp/tests/terminal_outbound_custody_queues_do_not_regress_after_restart.md)
- [outbound_handoff_quarantines_message](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_message.md)
- [outbound_handoff_bounces_on_permanent_rcpt_failure](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_bounces_on_permanent_rcpt_failure.md)
- [outbound_handoff_defers_when_local_throttle_hits](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_defers_when_local_throttle_hits.md)
- [outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody.md)
- [outbound_handoff_uses_matching_routing_rule](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_uses_matching_routing_rule.md)
- [outbound_route_without_smart_host_uses_direct_mx_default](../../../../../functions/LPE-CT/src/smtp/tests/outbound_route_without_smart_host_uses_direct_mx_default.md)
- [outbound_handoff_delivers_accepted_domain_locally_without_direct_mx](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_delivers_accepted_domain_locally_without_direct_mx.md)
- [inbound_message_posts_to_core_delivery_api](../../../../../functions/LPE-CT/src/smtp/tests/inbound_message_posts_to_core_delivery_api.md)
- [inbound_magika_failure_is_quarantined](../../../../../functions/LPE-CT/src/smtp/tests/inbound_magika_failure_is_quarantined.md)
- [inbound_message_keeps_non_utf8_raw_bytes](../../../../../functions/LPE-CT/src/smtp/tests/inbound_message_keeps_non_utf8_raw_bytes.md)
- [greylisting_defers_first_triplet_then_allows_after_release_window](../../../../../functions/LPE-CT/src/smtp/tests/greylisting_defers_first_triplet_then_allows_after_release_window.md)
- [reputation_score_penalizes_quarantine_and_rejects](../../../../../functions/LPE-CT/src/smtp/tests/reputation_score_penalizes_quarantine_and_rejects.md)
- [bayespam_learns_tokens_and_scores_spammy_message](../../../../../functions/LPE-CT/src/smtp/tests/bayespam_learns_tokens_and_scores_spammy_message.md)
- [bayespam_requires_enough_content_evidence_before_contributing](../../../../../functions/LPE-CT/src/smtp/tests/bayespam_requires_enough_content_evidence_before_contributing.md)
- [outbound_handoff_quarantines_on_bayespam_score](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_on_bayespam_score.md)
- [outbound_handoff_rejects_blocked_delegated_sender](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_rejects_blocked_delegated_sender.md)
- [retry_trace_clears_stale_execution_state_and_appends_audit](../../../../../functions/LPE-CT/src/smtp/tests/retry_trace_clears_stale_execution_state_and_appends_audit.md)
- [release_trace_moves_quarantined_inbound_back_to_incoming_and_appends_audit](../../../../../functions/LPE-CT/src/smtp/tests/release_trace_moves_quarantined_inbound_back_to_incoming_and_appends_audit.md)
- [rejected_quarantine_trace_recovers_from_spool_until_operator_delete](../../../../../functions/LPE-CT/src/smtp/tests/rejected_quarantine_trace_recovers_from_spool_until_operator_delete.md)
- [quarantine_release_reject_delete_recovers_across_node_replacement](../../../../../functions/LPE-CT/src/smtp/tests/quarantine_release_reject_delete_recovers_across_node_replacement.md)
- [delete_trace_removes_held_queue_items](../../../../../functions/LPE-CT/src/smtp/tests/delete_trace_removes_held_queue_items.md)
- [delete_trace_rejects_sent_history_items](../../../../../functions/LPE-CT/src/smtp/tests/delete_trace_rejects_sent_history_items.md)
- [auth_policy_config](../../../../../functions/LPE-CT/src/smtp/tests/auth_policy_config.md)
- [benchmark_relay_hot_path](../../../../../functions/LPE-CT/src/smtp/tests/benchmark_relay_hot_path.md)