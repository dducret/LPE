---
type: Rust Function
title: main
resource: LPE-CT/src/main.rs#L583-L704
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/install_rustls_crypto_provider
  - functions/LPE-CT/src/imaps_proxy/imaps_bind_address
  - functions/LPE-CT/src/imaps_proxy/imaps_upstream_address
  - functions/LPE-CT/src/imaps_proxy/imaps_tls_cert_path
  - functions/LPE-CT/src/imaps_proxy/imaps_tls_key_path
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/load_or_initialize_state
  - functions/LPE-CT/src/dashboard_config/apply_env_overrides
  - functions/LPE-CT/src/dashboard_config/normalize_local_data_stores
  - functions/LPE-CT/src/local_db_config_from_dashboard
  - functions/LPE-CT/src/storage/load_dashboard_state
  - functions/LPE-CT/src/dashboard_config/ensure_management_bootstrap
  - functions/LPE-CT/src/dashboard_config/normalize_accepted_domains
  - functions/LPE-CT/src/dashboard_config/normalize_policy_settings
  - functions/LPE-CT/src/dashboard_config/normalize_public_tls_settings
  - functions/LPE-CT/src/reporting/normalize_reporting_settings
  - functions/LPE-CT/src/dashboard_config/normalize_relay_settings
  - functions/LPE-CT/src/smtp/runtime_config_from_dashboard
  - functions/LPE-CT/src/smtp/prepare_local_store
  - functions/LPE-CT/src/reporting/enforce_retention
  - functions/LPE-CT/src/dashboard_config/submission_listener_is_configured
  - functions/LPE-CT/src/persist_state
  - functions/LPE-CT/src/sync_dashboard_to_postgres
  - functions/LPE-CT/src/run_reporting_scheduler
  - functions/LPE-CT/src/smtp/run_smtp_listener
  - functions/LPE-CT/src/submission/run_submission_listener
  - functions/LPE-CT/src/imaps_proxy/run_imaps_proxy
---

# Signature

`async fn main() -> Result<()>`

# Calls

- [install_rustls_crypto_provider](../../../functions/LPE-CT/src/install_rustls_crypto_provider.md)
- [imaps_bind_address](../../../functions/LPE-CT/src/imaps_proxy/imaps_bind_address.md)
- [imaps_upstream_address](../../../functions/LPE-CT/src/imaps_proxy/imaps_upstream_address.md)
- [imaps_tls_cert_path](../../../functions/LPE-CT/src/imaps_proxy/imaps_tls_cert_path.md)
- [imaps_tls_key_path](../../../functions/LPE-CT/src/imaps_proxy/imaps_tls_key_path.md)
- [initialize_spool](../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [load_or_initialize_state](../../../functions/LPE-CT/src/load_or_initialize_state.md)
- [apply_env_overrides](../../../functions/LPE-CT/src/dashboard_config/apply_env_overrides.md)
- [normalize_local_data_stores](../../../functions/LPE-CT/src/dashboard_config/normalize_local_data_stores.md)
- [local_db_config_from_dashboard](../../../functions/LPE-CT/src/local_db_config_from_dashboard.md)
- [load_dashboard_state](../../../functions/LPE-CT/src/storage/load_dashboard_state.md)
- [ensure_management_bootstrap](../../../functions/LPE-CT/src/dashboard_config/ensure_management_bootstrap.md)
- [normalize_accepted_domains](../../../functions/LPE-CT/src/dashboard_config/normalize_accepted_domains.md)
- [normalize_policy_settings](../../../functions/LPE-CT/src/dashboard_config/normalize_policy_settings.md)
- [normalize_public_tls_settings](../../../functions/LPE-CT/src/dashboard_config/normalize_public_tls_settings.md)
- [normalize_reporting_settings](../../../functions/LPE-CT/src/reporting/normalize_reporting_settings.md)
- [normalize_relay_settings](../../../functions/LPE-CT/src/dashboard_config/normalize_relay_settings.md)
- [runtime_config_from_dashboard](../../../functions/LPE-CT/src/smtp/runtime_config_from_dashboard.md)
- [prepare_local_store](../../../functions/LPE-CT/src/smtp/prepare_local_store.md)
- [enforce_retention](../../../functions/LPE-CT/src/reporting/enforce_retention.md)
- [submission_listener_is_configured](../../../functions/LPE-CT/src/dashboard_config/submission_listener_is_configured.md)
- [persist_state](../../../functions/LPE-CT/src/persist_state.md)
- [sync_dashboard_to_postgres](../../../functions/LPE-CT/src/sync_dashboard_to_postgres.md)
- [run_reporting_scheduler](../../../functions/LPE-CT/src/run_reporting_scheduler.md)
- [run_smtp_listener](../../../functions/LPE-CT/src/smtp/run_smtp_listener.md)
- [run_submission_listener](../../../functions/LPE-CT/src/submission/run_submission_listener.md)
- [run_imaps_proxy](../../../functions/LPE-CT/src/imaps_proxy/run_imaps_proxy.md)