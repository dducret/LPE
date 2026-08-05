---
type: Rust Function
title: initialize_spool
resource: LPE-CT/src/smtp.rs#L421-L427
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/main
  - functions/LPE-CT/src/queue_metrics_count_runtime_spool_messages_by_state
  - functions/LPE-CT/src/smtp/tests/smtp_session_accepts_lpe_domain_and_rejects_external_relay_domain
  - functions/LPE-CT/src/smtp/tests/smtp_data_accepts_plaintext_for_local_domain_and_hands_to_core
  - functions/LPE-CT/src/smtp/tests/smtp_ingress_marks_outlook_account_test_message
  - functions/LPE-CT/src/smtp/tests/inbound_delivery_keeps_durable_spool_custody_until_core_accepts
  - functions/LPE-CT/src/smtp/tests/smtp_data_accepts_null_reverse_path_for_dsn_delivery
  - functions/LPE-CT/src/smtp/tests/smtp_data_defers_with_trace_when_core_delivery_is_unavailable
  - functions/LPE-CT/src/smtp/tests/inbound_bridge_failure_keeps_deferred_custody_with_audit
  - functions/LPE-CT/src/smtp/tests/accepted_inbound_spool_custody_survives_restart_before_core_delivery
  - functions/LPE-CT/src/smtp/tests/smtp_unknown_local_recipient_core_rejection_defers_without_backscatter_bounce
  - functions/LPE-CT/src/smtp/tests/smtp_data_rejects_with_policy_reason_and_trace
  - functions/LPE-CT/src/smtp/tests/smtp_starttls_upgrades_to_tls_after_ready_reply
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_relays_message
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay
  - functions/LPE-CT/src/smtp/tests/outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay
  - functions/LPE-CT/src/smtp/tests/terminal_outbound_custody_queues_do_not_regress_after_restart
  - functions/LPE-CT/src/smtp/tests/smtp_session_rejects_when_ha_role_is_standby
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_message
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_bounces_on_permanent_rcpt_failure
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_defers_when_local_throttle_hits
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_uses_matching_routing_rule
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
  - functions/LPE-CT/src/smtp/tests/benchmark_relay_hot_path
---

# Signature

`pub(crate) fn initialize_spool(spool_dir: &Path) -> Result<()>`

# Called by

- [main](../../../../functions/LPE-CT/src/main.md)
- [queue_metrics_count_runtime_spool_messages_by_state](../../../../functions/LPE-CT/src/queue_metrics_count_runtime_spool_messages_by_state.md)
- [smtp_session_accepts_lpe_domain_and_rejects_external_relay_domain](../../../../functions/LPE-CT/src/smtp/tests/smtp_session_accepts_lpe_domain_and_rejects_external_relay_domain.md)
- [smtp_data_accepts_plaintext_for_local_domain_and_hands_to_core](../../../../functions/LPE-CT/src/smtp/tests/smtp_data_accepts_plaintext_for_local_domain_and_hands_to_core.md)
- [smtp_ingress_marks_outlook_account_test_message](../../../../functions/LPE-CT/src/smtp/tests/smtp_ingress_marks_outlook_account_test_message.md)
- [inbound_delivery_keeps_durable_spool_custody_until_core_accepts](../../../../functions/LPE-CT/src/smtp/tests/inbound_delivery_keeps_durable_spool_custody_until_core_accepts.md)
- [smtp_data_accepts_null_reverse_path_for_dsn_delivery](../../../../functions/LPE-CT/src/smtp/tests/smtp_data_accepts_null_reverse_path_for_dsn_delivery.md)
- [smtp_data_defers_with_trace_when_core_delivery_is_unavailable](../../../../functions/LPE-CT/src/smtp/tests/smtp_data_defers_with_trace_when_core_delivery_is_unavailable.md)
- [inbound_bridge_failure_keeps_deferred_custody_with_audit](../../../../functions/LPE-CT/src/smtp/tests/inbound_bridge_failure_keeps_deferred_custody_with_audit.md)
- [accepted_inbound_spool_custody_survives_restart_before_core_delivery](../../../../functions/LPE-CT/src/smtp/tests/accepted_inbound_spool_custody_survives_restart_before_core_delivery.md)
- [smtp_unknown_local_recipient_core_rejection_defers_without_backscatter_bounce](../../../../functions/LPE-CT/src/smtp/tests/smtp_unknown_local_recipient_core_rejection_defers_without_backscatter_bounce.md)
- [smtp_data_rejects_with_policy_reason_and_trace](../../../../functions/LPE-CT/src/smtp/tests/smtp_data_rejects_with_policy_reason_and_trace.md)
- [smtp_starttls_upgrades_to_tls_after_ready_reply](../../../../functions/LPE-CT/src/smtp/tests/smtp_starttls_upgrades_to_tls_after_ready_reply.md)
- [outbound_handoff_relays_message](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_relays_message.md)
- [outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay.md)
- [outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay](../../../../functions/LPE-CT/src/smtp/tests/outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay.md)
- [terminal_outbound_custody_queues_do_not_regress_after_restart](../../../../functions/LPE-CT/src/smtp/tests/terminal_outbound_custody_queues_do_not_regress_after_restart.md)
- [smtp_session_rejects_when_ha_role_is_standby](../../../../functions/LPE-CT/src/smtp/tests/smtp_session_rejects_when_ha_role_is_standby.md)
- [outbound_handoff_quarantines_message](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_message.md)
- [outbound_handoff_bounces_on_permanent_rcpt_failure](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_bounces_on_permanent_rcpt_failure.md)
- [outbound_handoff_defers_when_local_throttle_hits](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_defers_when_local_throttle_hits.md)
- [outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody.md)
- [outbound_handoff_uses_matching_routing_rule](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_uses_matching_routing_rule.md)
- [outbound_handoff_delivers_accepted_domain_locally_without_direct_mx](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_delivers_accepted_domain_locally_without_direct_mx.md)
- [inbound_message_posts_to_core_delivery_api](../../../../functions/LPE-CT/src/smtp/tests/inbound_message_posts_to_core_delivery_api.md)
- [inbound_magika_failure_is_quarantined](../../../../functions/LPE-CT/src/smtp/tests/inbound_magika_failure_is_quarantined.md)
- [inbound_message_keeps_non_utf8_raw_bytes](../../../../functions/LPE-CT/src/smtp/tests/inbound_message_keeps_non_utf8_raw_bytes.md)
- [greylisting_defers_first_triplet_then_allows_after_release_window](../../../../functions/LPE-CT/src/smtp/tests/greylisting_defers_first_triplet_then_allows_after_release_window.md)
- [reputation_score_penalizes_quarantine_and_rejects](../../../../functions/LPE-CT/src/smtp/tests/reputation_score_penalizes_quarantine_and_rejects.md)
- [bayespam_learns_tokens_and_scores_spammy_message](../../../../functions/LPE-CT/src/smtp/tests/bayespam_learns_tokens_and_scores_spammy_message.md)
- [bayespam_requires_enough_content_evidence_before_contributing](../../../../functions/LPE-CT/src/smtp/tests/bayespam_requires_enough_content_evidence_before_contributing.md)
- [outbound_handoff_quarantines_on_bayespam_score](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_on_bayespam_score.md)
- [outbound_handoff_rejects_blocked_delegated_sender](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_rejects_blocked_delegated_sender.md)
- [retry_trace_clears_stale_execution_state_and_appends_audit](../../../../functions/LPE-CT/src/smtp/tests/retry_trace_clears_stale_execution_state_and_appends_audit.md)
- [release_trace_moves_quarantined_inbound_back_to_incoming_and_appends_audit](../../../../functions/LPE-CT/src/smtp/tests/release_trace_moves_quarantined_inbound_back_to_incoming_and_appends_audit.md)
- [rejected_quarantine_trace_recovers_from_spool_until_operator_delete](../../../../functions/LPE-CT/src/smtp/tests/rejected_quarantine_trace_recovers_from_spool_until_operator_delete.md)
- [quarantine_release_reject_delete_recovers_across_node_replacement](../../../../functions/LPE-CT/src/smtp/tests/quarantine_release_reject_delete_recovers_across_node_replacement.md)
- [delete_trace_removes_held_queue_items](../../../../functions/LPE-CT/src/smtp/tests/delete_trace_removes_held_queue_items.md)
- [delete_trace_rejects_sent_history_items](../../../../functions/LPE-CT/src/smtp/tests/delete_trace_rejects_sent_history_items.md)
- [benchmark_relay_hot_path](../../../../functions/LPE-CT/src/smtp/tests/benchmark_relay_hot_path.md)