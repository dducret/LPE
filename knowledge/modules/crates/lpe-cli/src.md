---
type: Rust Module
title: src
resource: crates/lpe-cli/src/main.rs#L1-L572
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/lpe-admin-api-bootstrap-admin-bootstrap-admin-request-from-env-bootstrap-admin-request-from-env-or-defaults-ha-allows-active-work-ha-current-role-init-observability-integration-shared-secret-observe-outbound-worker-dispatch-observe-outbound-worker-poll-observe-outbound-worker-poll-failure-router
  - external/lpe-domain-outboundmessagehandoffrequest-outboundmessagehandoffresponse-signedintegrationheaders-integration-key-header-integration-nonce-header-integration-signature-header-integration-timestamp-header
  - external/lpe-imap-imapserver
  - external/lpe-managesieve-managesieveserver
  - external/lpe-storage-storage
  - external/std-env-time-duration
  - external/tokio-net-tcplistener-task-joinset-time-sleep
  - external/tracing-info-warn
  - external/super-run-outbound-worker-send-outbound-handoff
  - external/axum-extract-state-http-headermap-routing-post-json-router
  - external/lpe-domain-outboundmessagehandoffrequest-outboundmessagehandoffresponse-signedintegrationheaders-transportdeliverystatus-transportrecipient-integration-key-header-integration-nonce-header-integration-signature-header-integration-timestamp-header
  - external/sqlx-postgres-pgpooloptions
  - external/std-env-sync-arc-mutex-time-duration
  - external/tokio-net-tcplistener
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-cli
---

# Contains

- [main](../../../functions/crates/lpe-cli/src/main.md)
- [log_startup_fingerprint](../../../functions/crates/lpe-cli/src/log_startup_fingerprint.md)
- [auto_bootstrap_admin_if_missing](../../../functions/crates/lpe-cli/src/auto_bootstrap_admin_if_missing.md)
- [run_bootstrap_admin_command](../../../functions/crates/lpe-cli/src/run_bootstrap_admin_command.md)
- [run_outbound_worker](../../../functions/crates/lpe-cli/src/run_outbound_worker.md)
- [run_jmap_journal_purge_worker](../../../functions/crates/lpe-cli/src/run_jmap_journal_purge_worker.md)
- [dispatch_outbound_message](../../../functions/crates/lpe-cli/src/dispatch_outbound_message.md)
- [send_outbound_handoff](../../../functions/crates/lpe-cli/src/send_outbound_handoff.md)
- [outbound_worker_retries_after_a_closed_pool_failure](../../../functions/crates/lpe-cli/src/outbound_worker_retries_after_a_closed_pool_failure.md)
- [handoff_client_posts_json_and_header](../../../functions/crates/lpe-cli/src/handoff_client_posts_json_and_header.md)
- [Capture](../../../classes/crates/lpe-cli/src/Capture.md)
- [accept](../../../functions/crates/lpe-cli/src/accept.md)

# Imports

- `anyhow::Result`
- `lpe_admin_api::{
    bootstrap_admin, bootstrap_admin_request_from_env,
    bootstrap_admin_request_from_env_or_defaults, ha_allows_active_work, ha_current_role,
    init_observability, integration_shared_secret, observe_outbound_worker_dispatch,
    observe_outbound_worker_poll, observe_outbound_worker_poll_failure, router,
}`
- `lpe_domain::{
    OutboundMessageHandoffRequest, OutboundMessageHandoffResponse, SignedIntegrationHeaders,
    INTEGRATION_KEY_HEADER, INTEGRATION_NONCE_HEADER, INTEGRATION_SIGNATURE_HEADER,
    INTEGRATION_TIMESTAMP_HEADER,
}`
- `lpe_imap::ImapServer`
- `lpe_managesieve::ManageSieveServer`
- `lpe_storage::Storage`
- `std::{env, time::Duration}`
- `tokio::{net::TcpListener, task::JoinSet, time::sleep}`
- `tracing::{info, warn}`
- `super::{run_outbound_worker, send_outbound_handoff}`
- `axum::{extract::State, http::HeaderMap, routing::post, Json, Router}`
- `lpe_domain::{
        OutboundMessageHandoffRequest, OutboundMessageHandoffResponse, SignedIntegrationHeaders,
        TransportDeliveryStatus, TransportRecipient, INTEGRATION_KEY_HEADER,
        INTEGRATION_NONCE_HEADER, INTEGRATION_SIGNATURE_HEADER, INTEGRATION_TIMESTAMP_HEADER,
    }`
- `sqlx::postgres::PgPoolOptions`
- `std::{
        env,
        sync::{Arc, Mutex},
        time::Duration,
    }`
- `tokio::net::TcpListener`
- `uuid::Uuid`

# Member of

- [lpe-cli](../../../packages/crates/lpe-cli.md)