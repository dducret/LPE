---
type: Rust Method
title: fetch_admin_dashboard
resource: crates/lpe-storage/src/admin/dashboard.rs#L10-L302
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/admin/Storage/fetch_server_settings
  - functions/crates/lpe-storage/src/admin/Storage/fetch_security_settings
  - functions/crates/lpe-storage/src/admin/Storage/fetch_local_ai_settings
  - functions/crates/lpe-storage/src/admin/Storage/fetch_antispam_settings
  - functions/crates/lpe-storage/src/admin/Storage/fetch_server_administrators
  - functions/crates/lpe-storage/src/admin/Storage/fetch_antispam_rules
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/login
  - functions/crates/lpe-admin-api/src/admin_auth/enroll_totp
  - functions/crates/lpe-admin-api/src/admin_auth/oidc_metadata
  - functions/crates/lpe-admin-api/src/admin_auth/oidc_start
  - functions/crates/lpe-admin-api/src/admin_auth/oidc_callback
  - functions/crates/lpe-admin-api/src/client_auth/client_login
  - functions/crates/lpe-admin-api/src/client_auth/enroll_account_totp
  - functions/crates/lpe-admin-api/src/client_auth/client_oidc_metadata
  - functions/crates/lpe-admin-api/src/client_auth/client_oidc_start
  - functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback
  - functions/crates/lpe-admin-api/src/console/local_ai_health
  - functions/crates/lpe-admin-api/src/console/dashboard
  - functions/crates/lpe-admin-api/src/console/create_account
  - functions/crates/lpe-admin-api/src/console/update_account
  - functions/crates/lpe-admin-api/src/console/create_pst_transfer_job
  - functions/crates/lpe-admin-api/src/console/upload_pst_import
  - functions/crates/lpe-admin-api/src/console/update_server_settings
  - functions/crates/lpe-admin-api/src/console/update_security_settings
  - functions/crates/lpe-admin-api/src/console/update_local_ai_settings
  - functions/crates/lpe-admin-api/src/health/health
  - functions/crates/lpe-admin-api/src/health/health_ready
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_admin_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_admin_dashboard_path
---

# Signature

`pub async fn fetch_admin_dashboard(&self) -> Result<AdminDashboard>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [fetch_server_settings](../../../../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_server_settings.md)
- [fetch_security_settings](../../../../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_security_settings.md)
- [fetch_local_ai_settings](../../../../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_local_ai_settings.md)
- [fetch_antispam_settings](../../../../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_antispam_settings.md)
- [fetch_server_administrators](../../../../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_server_administrators.md)
- [fetch_antispam_rules](../../../../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_antispam_rules.md)

# Called by

- [login](../../../../../../../functions/crates/lpe-admin-api/src/admin_auth/login.md)
- [enroll_totp](../../../../../../../functions/crates/lpe-admin-api/src/admin_auth/enroll_totp.md)
- [oidc_metadata](../../../../../../../functions/crates/lpe-admin-api/src/admin_auth/oidc_metadata.md)
- [oidc_start](../../../../../../../functions/crates/lpe-admin-api/src/admin_auth/oidc_start.md)
- [oidc_callback](../../../../../../../functions/crates/lpe-admin-api/src/admin_auth/oidc_callback.md)
- [client_login](../../../../../../../functions/crates/lpe-admin-api/src/client_auth/client_login.md)
- [enroll_account_totp](../../../../../../../functions/crates/lpe-admin-api/src/client_auth/enroll_account_totp.md)
- [client_oidc_metadata](../../../../../../../functions/crates/lpe-admin-api/src/client_auth/client_oidc_metadata.md)
- [client_oidc_start](../../../../../../../functions/crates/lpe-admin-api/src/client_auth/client_oidc_start.md)
- [client_oidc_callback](../../../../../../../functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback.md)
- [local_ai_health](../../../../../../../functions/crates/lpe-admin-api/src/console/local_ai_health.md)
- [dashboard](../../../../../../../functions/crates/lpe-admin-api/src/console/dashboard.md)
- [create_account](../../../../../../../functions/crates/lpe-admin-api/src/console/create_account.md)
- [update_account](../../../../../../../functions/crates/lpe-admin-api/src/console/update_account.md)
- [create_pst_transfer_job](../../../../../../../functions/crates/lpe-admin-api/src/console/create_pst_transfer_job.md)
- [upload_pst_import](../../../../../../../functions/crates/lpe-admin-api/src/console/upload_pst_import.md)
- [update_server_settings](../../../../../../../functions/crates/lpe-admin-api/src/console/update_server_settings.md)
- [update_security_settings](../../../../../../../functions/crates/lpe-admin-api/src/console/update_security_settings.md)
- [update_local_ai_settings](../../../../../../../functions/crates/lpe-admin-api/src/console/update_local_ai_settings.md)
- [health](../../../../../../../functions/crates/lpe-admin-api/src/health/health.md)
- [health_ready](../../../../../../../functions/crates/lpe-admin-api/src/health/health_ready.md)
- [exercise_admin_path](../../../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_admin_path.md)
- [exercise_admin_dashboard_path](../../../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_admin_dashboard_path.md)