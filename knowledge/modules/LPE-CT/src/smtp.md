---
type: Rust Module
title: smtp
resource: LPE-CT/src/smtp.rs#L1-L1284
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-context-result
  - external/base64-engine-general-purpose-standard-as-base64
  - external/email-auth-common-dns-dnserror-dnsresolver-dmarc-disposition-as-dmarcdisposition
  - external/lpe-domain-inbounddeliveryrequest-inbounddeliveryresponse-outboundmessagehandoffrequest-outboundmessagehandoffresponse-signedintegrationheaders-transportdeliverystatus-transportdsnreport-transportretryadvice-transportroutedecision-transporttechnicalstatus-transportthrottlestatus-integration-key-header-integration-nonce-header-integration-signature-header-integration-timestamp-header
  - external/lpe-magika-collect-mime-attachment-parts-extract-visible-text-parse-rfc822-header-value-detector-expectedkind-ingresscontext-policydecision-validationrequest-validator
  - external/serde-deserialize-serialize
  - external/serde-json-value
  - external/sqlx-types-json-pgpool-row
  - external/std-collections-hash-map-defaulthasher-btreemap-hashmap-hashset-env-fs-hash-hash-hasher-net-ipaddr-socketaddr-path-path-pathbuf-sync-atomic-atomicu32-ordering-arc-mutex-time-systemtime-unix-epoch
  - external/tokio-io-asyncbufread-asyncbufreadext-asyncwrite-asyncwriteext-bufreader-net-tcp-ownedreadhalf-tcp-ownedwritehalf-tcplistener-tcpstream-process-command
  - external/tokio-rustls-tlsacceptor
  - external/tracing-info-warn
  - external/crate-dkim-signing-integration-shared-secret-observability-storage-transport-policy
  - external/anti-abuse-evaluate-greylisting-query-dnsbl-dnsbloutcome
  - external/anti-abuse-dnsbl-query-name-greylistentry
  - external/antivirus-classify-inbound-message-evaluate-antivirus-policy-load-antivirus-providers-antivirusproviderconfig-inboundmagikaoutcome
  - external/antivirus-parse-antivirus-output-antivirusproviderdecision
  - external/audit-append-transport-audit-quarantine-search-text
  - external/audit-postfix-style-mail-log-line-transportauditevent
  - external/auth-apply-authentication-scores-authenticate-message-authsummary-authenticationassessment-spfdisposition
  - external/auth-dkim-disposition-spf-disposition-summarize-dkim-summarize-dmarc-summarize-spf-dkimdisposition
  - external/bayes-train-bayespam
  - external/pub-crate-use-bayes-score-bayespam-bayeslabel
  - external/pub-crate-use-bayes-load-bayespam-corpus-bayespam-min-scoring-tokens
  - external/inbound-policy-apply-filter-verdict-evaluate-inbound-policy
  - external/inbound-policy-finalize-policy-decision
  - external/delivery-bridge-deliver-inbound-message
  - external/dns-systemdnsresolver
  - external/dsn-deferred-smtp-reply-direct-mx-failure-is-permanent-direct-mx-error-is-permanent-relay-error-parse-enhanced-status-rejected-smtp-reply
  - external/policy-accepted-domain-is-verified-domain-part-inbound-domain-policy-matches-any-domain-matches-domain-normalized-recipient-domain-accepts-null-reverse-path-recipient-domain-is-accepted
  - external/pub-crate-use-quarantine-list-quarantine-items-list-quarantine-items-from-spool
  - external/quarantine-persist-quarantine-metadata-persist-quarantine-metadata-or-warn-remove-quarantine-metadata-or-warn
  - external/queue-store-find-message-inspect-queue-load-message-from-path-move-message-persist-message-spool-path
  - external/protocol-expect-smtp-read-smtp-data-read-smtp-reply-smtp-command-smtp-command-reply-write-smtp
  - external/pub-crate-use-protocol-max-smtp-message-size-bytes-parse-smtp-path-smtp-path-error-reply-smtppathkind
  - external/pub-crate-use-protocol-parsedsmtppath-smtppatherror
  - external/reputation-load-reputation-score-update-reputation
  - external/pub-crate-use-outbound-compose-rfc822-message
  - external/pub-crate-use-outbound-encode-quoted-printable
  - external/outbound-delivery-relay-message-sanitize-outbound-ehlo-name
  - external/outbound-policy-default-queue-for-status-evaluate-outbound-throttle-outbound-handoff-response-from-spool-resolve-outbound-route
  - external/session-handle-smtp-session
  - external/session-handle-smtp-command-receive-message-receive-message-with-validator-smtpcommandoutcome-smtptransaction
  - external/pub-crate-use-tls-smtp-starttls-acceptor-for-paths
  - external/tls-smtp-starttls-acceptor-from-store-starttlsstream
  - external/trace-latest-decision-summary-quarantine-matches-quarantine-summary-from-message-trace-details-from-message
  - external/pub-crate-use-trace-actions-delete-trace-load-trace-details-release-trace-retry-trace
  - external/super-base64
  - external/base64-engine
  - external/serde-deserialize-deserializer-serializer
  member_of:
  - packages/LPE-CT
---

# Contains

- [RuntimeConfig](../../../classes/LPE-CT/src/smtp/RuntimeConfig.md)
- [AcceptedDomainConfig](../../../classes/LPE-CT/src/smtp/AcceptedDomainConfig.md)
- [OutboundRoutingRule](../../../classes/LPE-CT/src/smtp/OutboundRoutingRule.md)
- [OutboundThrottleRule](../../../classes/LPE-CT/src/smtp/OutboundThrottleRule.md)
- [DecisionTraceEntry](../../../classes/LPE-CT/src/smtp/DecisionTraceEntry.md)
- [QuarantineSummary](../../../classes/LPE-CT/src/smtp/QuarantineSummary.md)
- [QuarantineQuery](../../../classes/LPE-CT/src/smtp/QuarantineQuery.md)
- [TraceAttachmentSummary](../../../classes/LPE-CT/src/smtp/TraceAttachmentSummary.md)
- [TraceDetails](../../../classes/LPE-CT/src/smtp/TraceDetails.md)
- [TraceActionResult](../../../classes/LPE-CT/src/smtp/TraceActionResult.md)
- [QueuedMessage](../../../classes/LPE-CT/src/smtp/QueuedMessage.md)
- [OutboundExecution](../../../classes/LPE-CT/src/smtp/OutboundExecution.md)
- [FilterAction](../../../classes/LPE-CT/src/smtp/FilterAction.md)
- [FilterVerdict](../../../classes/LPE-CT/src/smtp/FilterVerdict.md)
- [initialize_spool](../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [prepare_local_store](../../../functions/LPE-CT/src/smtp/prepare_local_store.md)
- [ensure_local_db_schema](../../../functions/LPE-CT/src/smtp/ensure_local_db_schema.md)
- [reindex_quarantine_spool](../../../functions/LPE-CT/src/smtp/reindex_quarantine_spool.md)
- [queue_metrics](../../../functions/LPE-CT/src/smtp/queue_metrics.md)
- [run_smtp_listener](../../../functions/LPE-CT/src/smtp/run_smtp_listener.md)
- [runtime_config_from_dashboard](../../../functions/LPE-CT/src/smtp/runtime_config_from_dashboard.md)
- [runtime_config_from_store](../../../functions/LPE-CT/src/smtp/runtime_config_from_store.md)
- [parse_csv_env](../../../functions/LPE-CT/src/smtp/parse_csv_env.md)
- [process_outbound_handoff](../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [parse_peer_ip](../../../functions/LPE-CT/src/smtp/parse_peer_ip.md)
- [stable_key_id](../../../functions/LPE-CT/src/smtp/stable_key_id.md)
- [unix_now](../../../functions/LPE-CT/src/smtp/unix_now.md)
- [evaluate_outbound_sender_policy](../../../functions/LPE-CT/src/smtp/evaluate_outbound_sender_policy.md)
- [outbound_sender_policy_addresses](../../../functions/LPE-CT/src/smtp/outbound_sender_policy_addresses.md)
- [retry_after_seconds](../../../functions/LPE-CT/src/smtp/retry_after_seconds.md)
- [should_quarantine](../../../functions/LPE-CT/src/smtp/should_quarantine.md)
- [normalize_smtp_target](../../../functions/LPE-CT/src/smtp/normalize_smtp_target.md)
- [message_id](../../../functions/LPE-CT/src/smtp/message_id.md)
- [current_timestamp](../../../functions/LPE-CT/src/smtp/current_timestamp.md)
- [parse_unix_timestamp](../../../functions/LPE-CT/src/smtp/parse_unix_timestamp.md)
- [serialize](../../../functions/LPE-CT/src/smtp/serialize.md)
- [deserialize](../../../functions/LPE-CT/src/smtp/deserialize.md)

# Imports

- `anyhow::{anyhow, Context, Result}`
- `base64::engine::general_purpose::STANDARD as BASE64`
- `email_auth::{
    common::dns::{DnsError, DnsResolver},
    dmarc::Disposition as DmarcDisposition,
}`
- `lpe_domain::{
    InboundDeliveryRequest, InboundDeliveryResponse, OutboundMessageHandoffRequest,
    OutboundMessageHandoffResponse, SignedIntegrationHeaders, TransportDeliveryStatus,
    TransportDsnReport, TransportRetryAdvice, TransportRouteDecision, TransportTechnicalStatus,
    TransportThrottleStatus, INTEGRATION_KEY_HEADER, INTEGRATION_NONCE_HEADER,
    INTEGRATION_SIGNATURE_HEADER, INTEGRATION_TIMESTAMP_HEADER,
}`
- `lpe_magika::{
    collect_mime_attachment_parts, extract_visible_text, parse_rfc822_header_value, Detector,
    ExpectedKind, IngressContext, PolicyDecision, ValidationRequest, Validator,
}`
- `serde::{Deserialize, Serialize}`
- `serde_json::Value`
- `sqlx::{types::Json, PgPool, Row}`
- `std::{
    collections::{hash_map::DefaultHasher, BTreeMap, HashMap, HashSet},
    env, fs,
    hash::{Hash, Hasher},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
}`
- `tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream},
    process::Command,
}`
- `tokio_rustls::TlsAcceptor`
- `tracing::{info, warn}`
- `crate::{dkim_signing, integration_shared_secret, observability, storage, transport_policy}`
- `anti_abuse::{evaluate_greylisting, query_dnsbl, DnsblOutcome}`
- `anti_abuse::{dnsbl_query_name, GreylistEntry}`
- `antivirus::{
    classify_inbound_message, evaluate_antivirus_policy, load_antivirus_providers,
    AntivirusProviderConfig, InboundMagikaOutcome,
}`
- `antivirus::{parse_antivirus_output, AntivirusProviderDecision}`
- `audit::{
    append_transport_audit, quarantine_search_text,
}`
- `audit::{postfix_style_mail_log_line, TransportAuditEvent}`
- `auth::{
    apply_authentication_scores, authenticate_message, AuthSummary, AuthenticationAssessment,
    SpfDisposition,
}`
- `auth::{
    dkim_disposition, spf_disposition, summarize_dkim, summarize_dmarc, summarize_spf,
    DkimDisposition,
}`
- `bayes::train_bayespam`
- `pub(crate) use bayes::{score_bayespam, BayesLabel}`
- `pub(crate) use bayes::{load_bayespam_corpus, BAYESPAM_MIN_SCORING_TOKENS}`
- `inbound_policy::{apply_filter_verdict, evaluate_inbound_policy}`
- `inbound_policy::finalize_policy_decision`
- `delivery_bridge::deliver_inbound_message`
- `dns::SystemDnsResolver`
- `dsn::{
    deferred_smtp_reply, direct_mx_failure, is_permanent_direct_mx_error, is_permanent_relay_error,
    parse_enhanced_status, rejected_smtp_reply,
}`
- `policy::{
    accepted_domain_is_verified, domain_part, inbound_domain_policy, matches_any_domain,
    matches_domain, normalized, recipient_domain_accepts_null_reverse_path,
    recipient_domain_is_accepted,
}`
- `pub(crate) use quarantine::{list_quarantine_items, list_quarantine_items_from_spool}`
- `quarantine::{
    persist_quarantine_metadata, persist_quarantine_metadata_or_warn,
    remove_quarantine_metadata_or_warn,
}`
- `queue_store::{
    find_message, inspect_queue, load_message_from_path, move_message, persist_message, spool_path,
}`
- `protocol::{
    expect_smtp, read_smtp_data, read_smtp_reply, smtp_command, smtp_command_reply, write_smtp,
}`
- `pub(crate) use protocol::{
    max_smtp_message_size_bytes, parse_smtp_path, smtp_path_error_reply, SmtpPathKind,
}`
- `pub(crate) use protocol::{ParsedSmtpPath, SmtpPathError}`
- `reputation::{load_reputation_score, update_reputation}`
- `pub(crate) use outbound::compose_rfc822_message`
- `pub(crate) use outbound::encode_quoted_printable`
- `outbound_delivery::{relay_message, sanitize_outbound_ehlo_name}`
- `outbound_policy::{
    default_queue_for_status, evaluate_outbound_throttle, outbound_handoff_response_from_spool,
    resolve_outbound_route,
}`
- `session::handle_smtp_session`
- `session::{
    handle_smtp_command, receive_message, receive_message_with_validator, SmtpCommandOutcome,
    SmtpTransaction,
}`
- `pub(crate) use tls::smtp_starttls_acceptor_for_paths`
- `tls::{smtp_starttls_acceptor_from_store, StartTlsStream}`
- `trace::{
    latest_decision_summary, quarantine_matches, quarantine_summary_from_message,
    trace_details_from_message,
}`
- `pub(crate) use trace_actions::{delete_trace, load_trace_details, release_trace, retry_trace}`
- `super::BASE64`
- `base64::Engine`
- `serde::{Deserialize, Deserializer, Serializer}`

# Member of

- [lpe-ct](../../../packages/LPE-CT.md)