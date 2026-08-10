---
type: Rust Module
title: tests
resource: LPE-CT/src/smtp/tests.rs#L1-L3631
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-apply-authentication-scores-classify-inbound-message-delete-trace-dkim-disposition-dnsbl-query-name-evaluate-greylisting-finalize-policy-decision-handle-smtp-command-handle-smtp-session-initialize-spool-load-antivirus-providers-load-bayespam-corpus-load-reputation-score-load-trace-details-parse-antivirus-output-parse-peer-ip-persist-message-postfix-style-mail-log-line-process-outbound-handoff-receive-message-receive-message-with-validator-release-trace-resolve-outbound-route-retry-after-seconds-retry-trace-score-bayespam-smtp-starttls-acceptor-for-paths-spf-disposition-stable-key-id-summarize-dkim-summarize-dmarc-summarize-spf-train-bayespam-unix-now-update-reputation-write-smtp-accepteddomainconfig-antivirusproviderconfig-antivirusproviderdecision-authsummary-authenticationassessment-bayeslabel-decisiontraceentry-dkimdisposition-filteraction-greylistentry-outboundroutingrule-outboundthrottlerule-parsedsmtppath-queuedmessage-runtimeconfig-smtpcommandoutcome-smtppatherror-smtppathkind-smtptransaction-spfdisposition-transportauditevent-transportdsnreport-transportroutedecision-transporttechnicalstatus-transportthrottlestatus-bayespam-min-scoring-tokens-default-greylist-delay-seconds-max-smtp-command-line-len-max-smtp-rcpt-per-transaction
  - external/crate-env-test-lock
  - external/axum-routing-post-json-router
  - external/email-auth-dkim-dkimresult-dmarc-disposition-as-dmarcdisposition-spf-spfresult
  - external/lpe-domain-inbounddeliveryrequest-inbounddeliveryresponse-outboundmessagehandoffrequest-transportdeliverystatus-transportrecipient
  - external/lpe-magika-detectionsource-detector-magikadetection-validator
  - external/serde-json-json
  - external/std-io-bufreader-as-stdiobufreader-cursor-net-ipaddr-net-socketaddr-path-path-pathbuf-pin-pin-sync-arc-mutex-task-context-as-taskcontext-poll-time-instant-systemtime-unix-epoch
  - external/tokio-io-asyncbufreadext-asyncwriteext-bufreader-net-tcplistener-tcpstream
  - external/tokio-rustls-rustls-pki-types-servername-clientconfig-rootcertstore-tlsconnector
  - external/uuid-uuid
  member_of:
  - packages/LPE-CT
---

# Contains

- [postfix_style_mail_log_line_keeps_operator_correlation_fields](../../../../functions/LPE-CT/src/smtp/tests/postfix_style_mail_log_line_keeps_operator_correlation_fields.md)
- [temp_dir](../../../../functions/LPE-CT/src/smtp/tests/temp_dir.md)
- [runtime_config](../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [plaintext_inbound_store](../../../../functions/LPE-CT/src/smtp/tests/plaintext_inbound_store.md)
- [runtime_store_with_accepted_domains](../../../../functions/LPE-CT/src/smtp/tests/runtime_store_with_accepted_domains.md)
- [recipient_domain_acceptance_is_exact_case_insensitive_and_verified](../../../../functions/LPE-CT/src/smtp/tests/recipient_domain_acceptance_is_exact_case_insensitive_and_verified.md)
- [smtp_path_parser_ignores_mail_parameters](../../../../functions/LPE-CT/src/smtp/tests/smtp_path_parser_ignores_mail_parameters.md)
- [smtp_mail_from_rejects_malformed_paths_unsupported_params_and_size_overflow](../../../../functions/LPE-CT/src/smtp/tests/smtp_mail_from_rejects_malformed_paths_unsupported_params_and_size_overflow.md)
- [smtp_rcpt_to_rejects_malformed_paths_and_unsupported_params](../../../../functions/LPE-CT/src/smtp/tests/smtp_rcpt_to_rejects_malformed_paths_and_unsupported_params.md)
- [smtp_rcpt_to_enforces_transaction_recipient_limit](../../../../functions/LPE-CT/src/smtp/tests/smtp_rcpt_to_enforces_transaction_recipient_limit.md)
- [smtp_long_command_line_returns_line_length_error](../../../../functions/LPE-CT/src/smtp/tests/smtp_long_command_line_returns_line_length_error.md)
- [smtp_command_sequence_requires_mail_and_recipient_before_data](../../../../functions/LPE-CT/src/smtp/tests/smtp_command_sequence_requires_mail_and_recipient_before_data.md)
- [smtp_rcpt_accepts_configured_domain_and_rejects_external_relay_domain](../../../../functions/LPE-CT/src/smtp/tests/smtp_rcpt_accepts_configured_domain_and_rejects_external_relay_domain.md)
- [smtp_null_reverse_path_is_controlled_per_recipient_domain](../../../../functions/LPE-CT/src/smtp/tests/smtp_null_reverse_path_is_controlled_per_recipient_domain.md)
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
- [training_message](../../../../functions/LPE-CT/src/smtp/tests/training_message.md)
- [CountingWriter](../../../../classes/LPE-CT/src/smtp/tests/CountingWriter.md)
- [poll_write](../../../../functions/LPE-CT/src/smtp/tests/CountingWriter/tokio-io-asyncwrite/poll_write.md)
- [poll_flush](../../../../functions/LPE-CT/src/smtp/tests/CountingWriter/tokio-io-asyncwrite/poll_flush.md)
- [poll_shutdown](../../../../functions/LPE-CT/src/smtp/tests/CountingWriter/tokio-io-asyncwrite/poll_shutdown.md)
- [smtp_write_emits_reply_and_crlf_in_one_write](../../../../functions/LPE-CT/src/smtp/tests/smtp_write_emits_reply_and_crlf_in_one_write.md)
- [smtp_ehlo_advertises_starttls_when_tls_is_available](../../../../functions/LPE-CT/src/smtp/tests/smtp_ehlo_advertises_starttls_when_tls_is_available.md)
- [smtp_ehlo_does_not_advertise_starttls_without_tls_config](../../../../functions/LPE-CT/src/smtp/tests/smtp_ehlo_does_not_advertise_starttls_without_tls_config.md)
- [smtp_public_ingress_does_not_advertise_or_accept_auth](../../../../functions/LPE-CT/src/smtp/tests/smtp_public_ingress_does_not_advertise_or_accept_auth.md)
- [smtp_starttls_acceptor_rejects_invalid_tls_config](../../../../functions/LPE-CT/src/smtp/tests/smtp_starttls_acceptor_rejects_invalid_tls_config.md)
- [smtp_starttls_requires_ehlo_or_helo_first](../../../../functions/LPE-CT/src/smtp/tests/smtp_starttls_requires_ehlo_or_helo_first.md)
- [smtp_ehlo_does_not_advertise_starttls_after_tls_upgrade](../../../../functions/LPE-CT/src/smtp/tests/smtp_ehlo_does_not_advertise_starttls_after_tls_upgrade.md)
- [read_test_smtp_reply](../../../../functions/LPE-CT/src/smtp/tests/read_test_smtp_reply.md)
- [smtp_starttls_upgrades_to_tls_after_ready_reply](../../../../functions/LPE-CT/src/smtp/tests/smtp_starttls_upgrades_to_tls_after_ready_reply.md)
- [FakeDetector](../../../../classes/LPE-CT/src/smtp/tests/FakeDetector.md)
- [detect](../../../../functions/LPE-CT/src/smtp/tests/FakeDetector/detector/detect.md)
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
- [outbound_route_without_smart_host_uses_direct_mx_default](../../../../functions/LPE-CT/src/smtp/tests/outbound_route_without_smart_host_uses_direct_mx_default.md)
- [outbound_handoff_delivers_accepted_domain_locally_without_direct_mx](../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_delivers_accepted_domain_locally_without_direct_mx.md)
- [inbound_message_posts_to_core_delivery_api](../../../../functions/LPE-CT/src/smtp/tests/inbound_message_posts_to_core_delivery_api.md)
- [inbound_mismatch_is_rejected_before_delivery](../../../../functions/LPE-CT/src/smtp/tests/inbound_mismatch_is_rejected_before_delivery.md)
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
- [takeri_provider_loads_with_default_command_and_args](../../../../functions/LPE-CT/src/smtp/tests/takeri_provider_loads_with_default_command_and_args.md)
- [antivirus_output_parser_detects_takeri_infections_and_suspicious_files](../../../../functions/LPE-CT/src/smtp/tests/antivirus_output_parser_detects_takeri_infections_and_suspicious_files.md)
- [antivirus_output_parser_ignores_negative_takeri_markers](../../../../functions/LPE-CT/src/smtp/tests/antivirus_output_parser_ignores_negative_takeri_markers.md)
- [auth_summary_uses_structured_outcomes](../../../../functions/LPE-CT/src/smtp/tests/auth_summary_uses_structured_outcomes.md)
- [auth_tempfail_is_detected_for_defer_logic](../../../../functions/LPE-CT/src/smtp/tests/auth_tempfail_is_detected_for_defer_logic.md)
- [auth_score_application_penalizes_failures_and_alignment_gaps](../../../../functions/LPE-CT/src/smtp/tests/auth_score_application_penalizes_failures_and_alignment_gaps.md)
- [auth_policy_config](../../../../functions/LPE-CT/src/smtp/tests/auth_policy_config.md)
- [decide_auth_policy](../../../../functions/LPE-CT/src/smtp/tests/decide_auth_policy.md)
- [strict_dmarc_rejects_spoofed_local_from_without_aligned_auth](../../../../functions/LPE-CT/src/smtp/tests/strict_dmarc_rejects_spoofed_local_from_without_aligned_auth.md)
- [external_domain_without_rejecting_dmarc_is_accepted_by_auth_policy](../../../../functions/LPE-CT/src/smtp/tests/external_domain_without_rejecting_dmarc_is_accepted_by_auth_policy.md)
- [aligned_spf_pass_accepts_message_under_dmarc](../../../../functions/LPE-CT/src/smtp/tests/aligned_spf_pass_accepts_message_under_dmarc.md)
- [aligned_dkim_pass_compensates_for_spf_fail](../../../../functions/LPE-CT/src/smtp/tests/aligned_dkim_pass_compensates_for_spf_fail.md)
- [retry_backoff_grows_with_attempt_count_and_caps](../../../../functions/LPE-CT/src/smtp/tests/retry_backoff_grows_with_attempt_count_and_caps.md)
- [dnsbl_query_name_reverses_ipv4_and_ipv6_addresses](../../../../functions/LPE-CT/src/smtp/tests/dnsbl_query_name_reverses_ipv4_and_ipv6_addresses.md)
- [spawn_dummy_smtp](../../../../functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp.md)
- [DummySmtpProfile](../../../../classes/LPE-CT/src/smtp/tests/DummySmtpProfile.md)
- [spawn_dummy_smtp_with_profile](../../../../functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp_with_profile.md)
- [handle_dummy_smtp](../../../../functions/LPE-CT/src/smtp/tests/handle_dummy_smtp.md)
- [outbound_request](../../../../functions/LPE-CT/src/smtp/tests/outbound_request.md)
- [inbound_test_message](../../../../functions/LPE-CT/src/smtp/tests/inbound_test_message.md)
- [outbound_terminal_test_message](../../../../functions/LPE-CT/src/smtp/tests/outbound_terminal_test_message.md)
- [spawn_dummy_core](../../../../functions/LPE-CT/src/smtp/tests/spawn_dummy_core.md)
- [accept](../../../../functions/LPE-CT/src/smtp/tests/accept.md)
- [spawn_custody_asserting_core](../../../../functions/LPE-CT/src/smtp/tests/spawn_custody_asserting_core.md)
- [accept](../../../../functions/LPE-CT/src/smtp/tests/accept-2.md)
- [count_queue_json_files](../../../../functions/LPE-CT/src/smtp/tests/count_queue_json_files.md)
- [benchmark_relay_hot_path](../../../../functions/LPE-CT/src/smtp/tests/benchmark_relay_hot_path.md)

# Imports

- `super::{
    apply_authentication_scores, classify_inbound_message, delete_trace, dkim_disposition,
    dnsbl_query_name, evaluate_greylisting,
    finalize_policy_decision, handle_smtp_command, handle_smtp_session, initialize_spool,
    load_antivirus_providers, load_bayespam_corpus, load_reputation_score, load_trace_details,
    parse_antivirus_output, parse_peer_ip, persist_message, postfix_style_mail_log_line,
    process_outbound_handoff, receive_message, receive_message_with_validator, release_trace,
    resolve_outbound_route, retry_after_seconds, retry_trace, score_bayespam,
    smtp_starttls_acceptor_for_paths, spf_disposition, stable_key_id, summarize_dkim,
    summarize_dmarc, summarize_spf, train_bayespam, unix_now, update_reputation, write_smtp,
    AcceptedDomainConfig, AntivirusProviderConfig, AntivirusProviderDecision, AuthSummary,
    AuthenticationAssessment, BayesLabel, DecisionTraceEntry, DkimDisposition, FilterAction,
    GreylistEntry, OutboundRoutingRule, OutboundThrottleRule, ParsedSmtpPath, QueuedMessage,
    RuntimeConfig, SmtpCommandOutcome, SmtpPathError, SmtpPathKind, SmtpTransaction,
    SpfDisposition, TransportAuditEvent, TransportDsnReport, TransportRouteDecision,
    TransportTechnicalStatus, TransportThrottleStatus, BAYESPAM_MIN_SCORING_TOKENS,
    DEFAULT_GREYLIST_DELAY_SECONDS, MAX_SMTP_COMMAND_LINE_LEN, MAX_SMTP_RCPT_PER_TRANSACTION,
}`
- `crate::env_test_lock`
- `axum::{routing::post, Json, Router}`
- `email_auth::{dkim::DkimResult, dmarc::Disposition as DmarcDisposition, spf::SpfResult}`
- `lpe_domain::{
    InboundDeliveryRequest, InboundDeliveryResponse, OutboundMessageHandoffRequest,
    TransportDeliveryStatus, TransportRecipient,
}`
- `lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator}`
- `serde_json::json`
- `std::{
    io::{BufReader as StdIoBufReader, Cursor},
    net::IpAddr,
    net::SocketAddr,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll},
    time::{Instant, SystemTime, UNIX_EPOCH},
}`
- `tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
}`
- `tokio_rustls::{
    rustls::{pki_types::ServerName, ClientConfig, RootCertStore},
    TlsConnector,
}`
- `uuid::Uuid`

# Member of

- [lpe-ct](../../../../packages/LPE-CT.md)