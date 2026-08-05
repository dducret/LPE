---
type: Rust Function
title: require_admin
resource: crates/lpe-admin-api/src/access.rs#L6-L30
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/auth/Storage/fetch_admin_session
  - functions/crates/lpe-admin-api/src/access/admin_has_right
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/me
  - functions/crates/lpe-admin-api/src/admin_auth/admin_auth_factors
  - functions/crates/lpe-admin-api/src/admin_auth/enroll_totp
  - functions/crates/lpe-admin-api/src/admin_auth/verify_totp_factor
  - functions/crates/lpe-admin-api/src/admin_auth/revoke_admin_factor
  - functions/crates/lpe-admin-api/src/console/dashboard
  - functions/crates/lpe-admin-api/src/console/create_account
  - functions/crates/lpe-admin-api/src/console/update_account
  - functions/crates/lpe-admin-api/src/console/create_mailbox
  - functions/crates/lpe-admin-api/src/console/create_pst_transfer_job
  - functions/crates/lpe-admin-api/src/console/upload_pst_import
  - functions/crates/lpe-admin-api/src/console/create_domain
  - functions/crates/lpe-admin-api/src/console/update_domain
  - functions/crates/lpe-admin-api/src/console/create_alias
  - functions/crates/lpe-admin-api/src/console/update_server_settings
  - functions/crates/lpe-admin-api/src/console/update_security_settings
  - functions/crates/lpe-admin-api/src/console/update_local_ai_settings
  - functions/crates/lpe-admin-api/src/console/update_antispam_settings
  - functions/crates/lpe-admin-api/src/console/create_server_administrator
  - functions/crates/lpe-admin-api/src/console/create_filter_rule
  - functions/crates/lpe-admin-api/src/console/search_email_trace
  - functions/crates/lpe-admin-api/src/console/run_pst_jobs
  - functions/crates/lpe-admin-api/src/console/mail_flow
  - functions/crates/lpe-admin-api/src/snapshots/list_snapshots
  - functions/crates/lpe-admin-api/src/snapshots/create_snapshot
  - functions/crates/lpe-admin-api/src/snapshots/delete_snapshot
  - functions/crates/lpe-admin-api/src/snapshots/restore_snapshot
  - functions/crates/lpe-admin-api/src/storage/list_storage_pools
  - functions/crates/lpe-admin-api/src/storage/create_storage_pool
  - functions/crates/lpe-admin-api/src/storage/update_storage_pool
  - functions/crates/lpe-admin-api/src/storage/get_storage_policies
  - functions/crates/lpe-admin-api/src/storage/get_storage_health
  - functions/crates/lpe-admin-api/src/storage/get_storage_migrations
  - functions/crates/lpe-admin-api/src/storage/get_storage_cleanup
  - functions/crates/lpe-admin-api/src/storage/update_platform_storage_policy
  - functions/crates/lpe-admin-api/src/storage/update_tenant_storage_policy
  - functions/crates/lpe-admin-api/src/storage/update_domain_storage_policy
  - functions/crates/lpe-admin-api/src/storage/update_account_storage_policy
---

# Signature

`pub(crate) async fn require_admin( storage: &Storage, headers: &HeaderMap, right: &str, ) -> std::result::Result<AuthenticatedAdmin, (StatusCode, String)>`

# Calls

- [fetch_admin_session](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_session.md)
- [admin_has_right](../../../../../functions/crates/lpe-admin-api/src/access/admin_has_right.md)

# Called by

- [me](../../../../../functions/crates/lpe-admin-api/src/admin_auth/me.md)
- [admin_auth_factors](../../../../../functions/crates/lpe-admin-api/src/admin_auth/admin_auth_factors.md)
- [enroll_totp](../../../../../functions/crates/lpe-admin-api/src/admin_auth/enroll_totp.md)
- [verify_totp_factor](../../../../../functions/crates/lpe-admin-api/src/admin_auth/verify_totp_factor.md)
- [revoke_admin_factor](../../../../../functions/crates/lpe-admin-api/src/admin_auth/revoke_admin_factor.md)
- [dashboard](../../../../../functions/crates/lpe-admin-api/src/console/dashboard.md)
- [create_account](../../../../../functions/crates/lpe-admin-api/src/console/create_account.md)
- [update_account](../../../../../functions/crates/lpe-admin-api/src/console/update_account.md)
- [create_mailbox](../../../../../functions/crates/lpe-admin-api/src/console/create_mailbox.md)
- [create_pst_transfer_job](../../../../../functions/crates/lpe-admin-api/src/console/create_pst_transfer_job.md)
- [upload_pst_import](../../../../../functions/crates/lpe-admin-api/src/console/upload_pst_import.md)
- [create_domain](../../../../../functions/crates/lpe-admin-api/src/console/create_domain.md)
- [update_domain](../../../../../functions/crates/lpe-admin-api/src/console/update_domain.md)
- [create_alias](../../../../../functions/crates/lpe-admin-api/src/console/create_alias.md)
- [update_server_settings](../../../../../functions/crates/lpe-admin-api/src/console/update_server_settings.md)
- [update_security_settings](../../../../../functions/crates/lpe-admin-api/src/console/update_security_settings.md)
- [update_local_ai_settings](../../../../../functions/crates/lpe-admin-api/src/console/update_local_ai_settings.md)
- [update_antispam_settings](../../../../../functions/crates/lpe-admin-api/src/console/update_antispam_settings.md)
- [create_server_administrator](../../../../../functions/crates/lpe-admin-api/src/console/create_server_administrator.md)
- [create_filter_rule](../../../../../functions/crates/lpe-admin-api/src/console/create_filter_rule.md)
- [search_email_trace](../../../../../functions/crates/lpe-admin-api/src/console/search_email_trace.md)
- [run_pst_jobs](../../../../../functions/crates/lpe-admin-api/src/console/run_pst_jobs.md)
- [mail_flow](../../../../../functions/crates/lpe-admin-api/src/console/mail_flow.md)
- [list_snapshots](../../../../../functions/crates/lpe-admin-api/src/snapshots/list_snapshots.md)
- [create_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/create_snapshot.md)
- [delete_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/delete_snapshot.md)
- [restore_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/restore_snapshot.md)
- [list_storage_pools](../../../../../functions/crates/lpe-admin-api/src/storage/list_storage_pools.md)
- [create_storage_pool](../../../../../functions/crates/lpe-admin-api/src/storage/create_storage_pool.md)
- [update_storage_pool](../../../../../functions/crates/lpe-admin-api/src/storage/update_storage_pool.md)
- [get_storage_policies](../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_policies.md)
- [get_storage_health](../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_health.md)
- [get_storage_migrations](../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_migrations.md)
- [get_storage_cleanup](../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_cleanup.md)
- [update_platform_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/update_platform_storage_policy.md)
- [update_tenant_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/update_tenant_storage_policy.md)
- [update_domain_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/update_domain_storage_policy.md)
- [update_account_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/update_account_storage_policy.md)