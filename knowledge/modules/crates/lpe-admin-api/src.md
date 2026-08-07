---
type: Rust Module
title: src
resource: crates/lpe-admin-api/src/lib.rs#L1-L42
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/pub-use-crate-app-bootstrap-admin-bootstrap-admin-request-from-env-bootstrap-admin-request-from-env-or-defaults-ha-allows-active-work-ha-current-role-init-observability-integration-shared-secret-observe-outbound-worker-dispatch-observe-outbound-worker-poll-observe-outbound-worker-poll-failure-router
  - external/pub-crate-use-crate-access-require-account-require-admin
  - external/pub-crate-use-crate-app-min-admin-password-len-min-integration-secret-len
  - external/pub-crate-use-crate-http-bad-request-error
  - external/pub-crate-use-crate-readiness-build-readiness-response-check-optional-http-dependency-ha-activation-check-lpe-ct-base-url-readiness-failed-readiness-ok-readiness-warn
  - external/pub-crate-use-crate-security-hash-password
  - external/pub-crate-use-crate-util-parse-collaboration-kind-parse-sender-delegation-right
  member_of:
  - packages/crates/lpe-admin-api
---

# Imports

- `pub use crate::app::{
    bootstrap_admin, bootstrap_admin_request_from_env,
    bootstrap_admin_request_from_env_or_defaults, ha_allows_active_work, ha_current_role,
    init_observability, integration_shared_secret, observe_outbound_worker_dispatch,
    observe_outbound_worker_poll, observe_outbound_worker_poll_failure, router,
}`
- `pub(crate) use crate::access::{require_account, require_admin}`
- `pub(crate) use crate::app::{MIN_ADMIN_PASSWORD_LEN, MIN_INTEGRATION_SECRET_LEN}`
- `pub(crate) use crate::http::bad_request_error`
- `pub(crate) use crate::readiness::{
    build_readiness_response, check_optional_http_dependency, ha_activation_check, lpe_ct_base_url,
    readiness_failed, readiness_ok, readiness_warn,
}`
- `pub(crate) use crate::security::hash_password`
- `pub(crate) use crate::util::{parse_collaboration_kind, parse_sender_delegation_right}`

# Member of

- [lpe-admin-api](../../../packages/crates/lpe-admin-api.md)