---
type: Rust Module
title: src
resource: LPE-CT/src/main.rs#L1-L1499
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-context-result
  - external/axum-extract-path-as-axumpath-query-state-http-header-content-disposition-content-type-headermap-headervalue-statuscode-middleware-response-intoresponse-response-routing-get-post-put-json-router
  - external/lpe-domain-current-unix-timestamp-bridgeautherror-outboundmessagehandoffrequest-outboundmessagehandoffresponse-recipientverificationrequest-recipientverificationresponse-signedintegrationheaders-default-max-skew-seconds-integration-key-header-integration-nonce-header-integration-signature-header-integration-timestamp-header
  - external/serde-deserialize-serialize
  - external/std-env-fs-path-path-pathbuf-sync-arc-mutex-time-duration-systemtime-unix-epoch
  - external/tokio-net-tcplistener
  - external/tokio-rustls-rustls-pki-types-certificateder-privatekeyder-serverconfig
  - external/tracing-info-warn
  - external/uuid-uuid
  - external/pub-crate-use-dashboard-config-accepted-domain-from-input-apply-env-overrides-default-antivirus-enabled-default-antivirus-fail-closed-default-antivirus-provider-chain-default-bayespam-auto-learn-default-bayespam-enabled-default-bayespam-max-tokens-default-bayespam-min-token-length-default-bayespam-score-weight-default-core-delivery-base-url-default-defer-on-auth-tempfail-default-dkim-headers-default-dkim-settings-default-dnsbl-enabled-default-dnsbl-zones-default-forbidden-canonical-data-default-local-db-network-scope-default-local-db-notes-default-local-db-purposes-default-policy-artifacts-default-recipient-verification-cache-ttl-seconds-default-recipient-verification-settings-default-reputation-enabled-default-reputation-quarantine-threshold-default-reputation-reject-threshold-default-spam-quarantine-threshold-default-spam-reject-threshold-default-spool-queues-default-state-default-true-ensure-management-bootstrap-normalize-accepted-domains-normalize-local-data-stores-normalize-policy-settings-normalize-public-tls-settings-normalize-relay-settings-probe-lpe-core-delivery-probe-lpe-recipient-bridge-submission-listener-is-configured-validate-relay-settings
  - external/pub-crate-use-dashboard-config-lpe-bridge-probe-url-lpe-health-probe-url
  - external/pub-crate-use-http-routes-accepted-domains-connect-lpe-support-create-accepted-domain-dashboard-delete-accepted-domain-delete-host-log-delete-public-tls-profile-delete-trace-digest-report-details-digest-reports-download-host-log-flush-mail-queue-health-health-live-health-ready-host-log-content-host-logs-list-import-accepted-domains-login-logout-mail-history-me-outbound-handoff-policy-status-quarantine-items-release-trace-reporting-snapshot-retry-trace-route-diagnostics-run-apt-update-upgrade-run-digest-reports-run-spam-test-run-system-power-action-run-system-tool-select-public-tls-profile-sync-system-ntp-system-diagnostic-report-system-diagnostic-service-action-system-diagnostic-services-system-health-check-test-accepted-domain-trace-details-trace-history-update-accepted-domain-update-network-update-policies-update-relay-update-reporting-update-site-update-system-ntp-update-updates-upload-public-tls-profile
  - external/pub-crate-use-http-routes-mark-accepted-domain-verified
  - external/management-auth-bearer-token-hash-password-is-known-weak-secret-require-management-admin-verify-password-apierror
  - external/pub-crate-use-management-auth-integration-shared-secret-require-integration-request
  - external/pub-crate-use-readiness-ha-non-active-role-for-traffic
  - external/pub-crate-use-readiness-address-binds-publicly
  - external/readiness-check-dashboard-state-store-check-local-data-store-policy-check-non-empty-value-check-optional-http-dependency-check-optional-tcp-dependency-check-quarantine-backlog-check-spool-layout-check-spool-pressure-dkim-key-status-ha-activation-check-readiness-failed-readiness-ok-readiness-status
  - external/std-os-unix-fs-permissionsext
  - external/super-address-binds-publicly-apply-env-overrides-default-state-env-test-lock-ha-activation-check-ha-non-active-role-for-traffic-integration-shared-secret-lpe-bridge-probe-url-lpe-health-probe-url-mark-accepted-domain-verified-normalize-local-data-stores-persist-state-require-integration-request-submission-listener-is-configured-accepteddomain-dashboardresponse-outbound-handoff-path
  - external/axum-http-headermap
  - external/lpe-domain-current-unix-timestamp-outboundmessagehandoffrequest-signedintegrationheaders-transportrecipient-integration-key-header-integration-nonce-header-integration-signature-header-integration-timestamp-header
  - external/std-fs-path-pathbuf-time-systemtime-unix-epoch
  member_of:
  - packages/LPE-CT
---

# Contains

- [env_test_lock](../../functions/LPE-CT/src/env_test_lock.md)
- [SiteProfile](../../classes/LPE-CT/src/SiteProfile.md)
- [RelaySettings](../../classes/LPE-CT/src/RelaySettings.md)
- [AcceptedDomain](../../classes/LPE-CT/src/AcceptedDomain.md)
- [AcceptedDomainInput](../../classes/LPE-CT/src/AcceptedDomainInput.md)
- [ImportAcceptedDomainsRequest](../../classes/LPE-CT/src/ImportAcceptedDomainsRequest.md)
- [AcceptedDomainTestResponse](../../classes/LPE-CT/src/AcceptedDomainTestResponse.md)
- [LpeHealthProbeResponse](../../classes/LPE-CT/src/LpeHealthProbeResponse.md)
- [RoutingSettings](../../classes/LPE-CT/src/RoutingSettings.md)
- [RoutingRule](../../classes/LPE-CT/src/RoutingRule.md)
- [ThrottlingSettings](../../classes/LPE-CT/src/ThrottlingSettings.md)
- [ThrottleRule](../../classes/LPE-CT/src/ThrottleRule.md)
- [NetworkSettings](../../classes/LPE-CT/src/NetworkSettings.md)
- [PublicTlsSettings](../../classes/LPE-CT/src/PublicTlsSettings.md)
- [PublicTlsProfile](../../classes/LPE-CT/src/PublicTlsProfile.md)
- [PublicTlsUploadRequest](../../classes/LPE-CT/src/PublicTlsUploadRequest.md)
- [PublicTlsSelectionRequest](../../classes/LPE-CT/src/PublicTlsSelectionRequest.md)
- [LocalDataStoresSettings](../../classes/LPE-CT/src/LocalDataStoresSettings.md)
- [LocalPostgresStore](../../classes/LPE-CT/src/LocalPostgresStore.md)
- [AddressPolicySettings](../../classes/LPE-CT/src/AddressPolicySettings.md)
- [RecipientVerificationSettings](../../classes/LPE-CT/src/RecipientVerificationSettings.md)
- [AttachmentPolicySettings](../../classes/LPE-CT/src/AttachmentPolicySettings.md)
- [DkimDomainConfig](../../classes/LPE-CT/src/DkimDomainConfig.md)
- [DkimSettings](../../classes/LPE-CT/src/DkimSettings.md)
- [PolicySettings](../../classes/LPE-CT/src/PolicySettings.md)
- [UpdateSettings](../../classes/LPE-CT/src/UpdateSettings.md)
- [QueueMetrics](../../classes/LPE-CT/src/QueueMetrics.md)
- [AuditEvent](../../classes/LPE-CT/src/AuditEvent.md)
- [ManagementAuthState](../../classes/LPE-CT/src/ManagementAuthState.md)
- [DashboardState](../../classes/LPE-CT/src/DashboardState.md)
- [DashboardResponse](../../classes/LPE-CT/src/DashboardResponse.md)
- [HealthResponse](../../classes/LPE-CT/src/HealthResponse.md)
- [ReadinessCheck](../../classes/LPE-CT/src/ReadinessCheck.md)
- [ReadinessResponse](../../classes/LPE-CT/src/ReadinessResponse.md)
- [AppState](../../classes/LPE-CT/src/AppState.md)
- [LoginRequest](../../classes/LPE-CT/src/LoginRequest.md)
- [ManagementIdentity](../../classes/LPE-CT/src/ManagementIdentity.md)
- [LoginResponse](../../classes/LPE-CT/src/LoginResponse.md)
- [RouteDiagnosticsResponse](../../classes/LPE-CT/src/RouteDiagnosticsResponse.md)
- [PolicyStatusResponse](../../classes/LPE-CT/src/PolicyStatusResponse.md)
- [RecipientVerificationStatusView](../../classes/LPE-CT/src/RecipientVerificationStatusView.md)
- [DkimStatusView](../../classes/LPE-CT/src/DkimStatusView.md)
- [DkimDomainStatusView](../../classes/LPE-CT/src/DkimDomainStatusView.md)
- [ManagementSession](../../classes/LPE-CT/src/ManagementSession.md)
- [main](../../functions/LPE-CT/src/main.md)
- [install_rustls_crypto_provider](../../functions/LPE-CT/src/install_rustls_crypto_provider.md)
- [router](../../functions/LPE-CT/src/router.md)
- [mutate_state](../../functions/LPE-CT/src/mutate_state.md)
- [run_reporting_scheduler](../../functions/LPE-CT/src/run_reporting_scheduler.md)
- [read_state](../../functions/LPE-CT/src/read_state.md)
- [local_db_config_from_dashboard](../../functions/LPE-CT/src/local_db_config_from_dashboard.md)
- [sync_technical_store](../../functions/LPE-CT/src/sync_technical_store.md)
- [sync_dashboard_to_postgres](../../functions/LPE-CT/src/sync_dashboard_to_postgres.md)
- [restore_dashboard_state](../../functions/LPE-CT/src/restore_dashboard_state.md)
- [append_dashboard_audit_event](../../functions/LPE-CT/src/append_dashboard_audit_event.md)
- [append_audit_event_with_actor](../../functions/LPE-CT/src/append_audit_event_with_actor.md)
- [load_or_initialize_state](../../functions/LPE-CT/src/load_or_initialize_state.md)
- [persist_state](../../functions/LPE-CT/src/persist_state.md)
- [public_tls_store_dir](../../functions/LPE-CT/src/public_tls_store_dir.md)
- [store_public_tls_profile](../../functions/LPE-CT/src/store_public_tls_profile.md)
- [normalize_pem_text](../../functions/LPE-CT/src/normalize_pem_text.md)
- [write_private_key_file](../../functions/LPE-CT/src/write_private_key_file.md)
- [validate_tls_pair_from_paths](../../functions/LPE-CT/src/validate_tls_pair_from_paths.md)
- [validate_tls_pair_from_pem](../../functions/LPE-CT/src/validate_tls_pair_from_pem.md)
- [parse_certificates_pem](../../functions/LPE-CT/src/parse_certificates_pem.md)
- [parse_private_key_pem](../../functions/LPE-CT/src/parse_private_key_pem.md)
- [current_timestamp](../../functions/LPE-CT/src/current_timestamp.md)
- [temp_file](../../functions/LPE-CT/src/temp_file.md)
- [temp_dir](../../functions/LPE-CT/src/temp_dir.md)
- [dashboard_response_serializes_runtime_system_without_persisting_it](../../functions/LPE-CT/src/dashboard_response_serializes_runtime_system_without_persisting_it.md)
- [queue_metrics_count_runtime_spool_messages_by_state](../../functions/LPE-CT/src/queue_metrics_count_runtime_spool_messages_by_state.md)
- [integration_secret_must_be_present_and_strong](../../functions/LPE-CT/src/integration_secret_must_be_present_and_strong.md)
- [ha_role_check_accepts_only_active_role](../../functions/LPE-CT/src/ha_role_check_accepts_only_active_role.md)
- [ha_non_active_gate_reports_non_active_roles](../../functions/LPE-CT/src/ha_non_active_gate_reports_non_active_roles.md)
- [local_db_address_must_not_be_public](../../functions/LPE-CT/src/local_db_address_must_not_be_public.md)
- [env_overrides_enable_private_local_db_profile](../../functions/LPE-CT/src/env_overrides_enable_private_local_db_profile.md)
- [submission_listener_requires_bind_and_tls_material](../../functions/LPE-CT/src/submission_listener_requires_bind_and_tls_material.md)
- [lpe_core_probe_urls_use_configured_delivery_base_url](../../functions/LPE-CT/src/lpe_core_probe_urls_use_configured_delivery_base_url.md)
- [accepted_domain_test_marks_domain_verified_once](../../functions/LPE-CT/src/accepted_domain_test_marks_domain_verified_once.md)
- [signed_integration_requests_reject_replay](../../functions/LPE-CT/src/signed_integration_requests_reject_replay.md)

# Imports

- `anyhow::{Context, Result}`
- `axum::{
    extract::{Path as AxumPath, Query, State},
    http::{
        header::{CONTENT_DISPOSITION, CONTENT_TYPE},
        HeaderMap, HeaderValue, StatusCode,
    },
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
}`
- `lpe_domain::{
    current_unix_timestamp, BridgeAuthError, OutboundMessageHandoffRequest,
    OutboundMessageHandoffResponse, RecipientVerificationRequest, RecipientVerificationResponse,
    SignedIntegrationHeaders, DEFAULT_MAX_SKEW_SECONDS, INTEGRATION_KEY_HEADER,
    INTEGRATION_NONCE_HEADER, INTEGRATION_SIGNATURE_HEADER, INTEGRATION_TIMESTAMP_HEADER,
}`
- `serde::{Deserialize, Serialize}`
- `std::{
    env, fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
}`
- `tokio::net::TcpListener`
- `tokio_rustls::rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    ServerConfig,
}`
- `tracing::{info, warn}`
- `uuid::Uuid`
- `pub(crate) use dashboard_config::{
    accepted_domain_from_input, apply_env_overrides, default_antivirus_enabled,
    default_antivirus_fail_closed, default_antivirus_provider_chain, default_bayespam_auto_learn,
    default_bayespam_enabled, default_bayespam_max_tokens, default_bayespam_min_token_length,
    default_bayespam_score_weight, default_core_delivery_base_url, default_defer_on_auth_tempfail,
    default_dkim_headers, default_dkim_settings, default_dnsbl_enabled, default_dnsbl_zones,
    default_forbidden_canonical_data, default_local_db_network_scope, default_local_db_notes,
    default_local_db_purposes, default_policy_artifacts,
    default_recipient_verification_cache_ttl_seconds, default_recipient_verification_settings,
    default_reputation_enabled, default_reputation_quarantine_threshold,
    default_reputation_reject_threshold, default_spam_quarantine_threshold,
    default_spam_reject_threshold, default_spool_queues, default_state, default_true,
    ensure_management_bootstrap, normalize_accepted_domains, normalize_local_data_stores,
    normalize_policy_settings,
    normalize_public_tls_settings, normalize_relay_settings, probe_lpe_core_delivery,
    probe_lpe_recipient_bridge, submission_listener_is_configured, validate_relay_settings,
}`
- `pub(crate) use dashboard_config::{lpe_bridge_probe_url, lpe_health_probe_url}`
- `pub(crate) use http_routes::{
    accepted_domains, connect_lpe_support, create_accepted_domain, dashboard,
    delete_accepted_domain, delete_host_log, delete_public_tls_profile, delete_trace,
    digest_report_details, digest_reports, download_host_log, flush_mail_queue, health,
    health_live, health_ready, host_log_content, host_logs_list, import_accepted_domains, login,
    logout, mail_history, me, outbound_handoff, policy_status, quarantine_items, release_trace,
    reporting_snapshot, retry_trace, route_diagnostics, run_apt_update_upgrade,
    run_digest_reports, run_spam_test, run_system_power_action, run_system_tool,
    select_public_tls_profile, sync_system_ntp, system_diagnostic_report,
    system_diagnostic_service_action, system_diagnostic_services, system_health_check,
    test_accepted_domain, trace_details, trace_history, update_accepted_domain,
    update_network, update_policies, update_relay, update_reporting, update_site,
    update_system_ntp, update_updates, upload_public_tls_profile,
}`
- `pub(crate) use http_routes::mark_accepted_domain_verified`
- `management_auth::{
    bearer_token, hash_password, is_known_weak_secret, require_management_admin, verify_password,
    ApiError,
}`
- `pub(crate) use management_auth::{integration_shared_secret, require_integration_request}`
- `pub(crate) use readiness::ha_non_active_role_for_traffic`
- `pub(crate) use readiness::address_binds_publicly`
- `readiness::{
    check_dashboard_state_store, check_local_data_store_policy, check_non_empty_value,
    check_optional_http_dependency, check_optional_tcp_dependency, check_quarantine_backlog,
    check_spool_layout, check_spool_pressure, dkim_key_status, ha_activation_check,
    readiness_failed, readiness_ok, readiness_status,
}`
- `std::os::unix::fs::PermissionsExt`
- `super::{
        address_binds_publicly, apply_env_overrides, default_state, env_test_lock,
        ha_activation_check, ha_non_active_role_for_traffic, integration_shared_secret,
        lpe_bridge_probe_url, lpe_health_probe_url, mark_accepted_domain_verified,
        normalize_local_data_stores, persist_state, require_integration_request,
        submission_listener_is_configured, AcceptedDomain, DashboardResponse,
        OUTBOUND_HANDOFF_PATH,
    }`
- `axum::http::HeaderMap`
- `lpe_domain::{
        current_unix_timestamp, OutboundMessageHandoffRequest, SignedIntegrationHeaders,
        TransportRecipient, INTEGRATION_KEY_HEADER, INTEGRATION_NONCE_HEADER,
        INTEGRATION_SIGNATURE_HEADER, INTEGRATION_TIMESTAMP_HEADER,
    }`
- `std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    }`

# Member of

- [lpe-ct](../../packages/LPE-CT.md)