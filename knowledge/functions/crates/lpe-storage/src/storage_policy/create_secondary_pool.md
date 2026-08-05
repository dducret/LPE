---
type: Rust Function
title: create_secondary_pool
resource: crates/lpe-storage/src/storage_policy.rs#L959-L973
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-storage/src/storage_policy/tenant_domain_and_account_policy_inherit_and_clear
  - functions/crates/lpe-storage/src/storage_policy/policy_rejects_disabled_or_unknown_pool
  - functions/crates/lpe-storage/src/storage_policy/policy_changes_do_not_create_migration_jobs
  - functions/crates/lpe-storage/src/storage_policy/policy_change_records_admin_audit_event
---

# Signature

`async fn create_secondary_pool(storage: &Storage) -> Uuid`

# Calls

- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [tenant_domain_and_account_policy_inherit_and_clear](../../../../../functions/crates/lpe-storage/src/storage_policy/tenant_domain_and_account_policy_inherit_and_clear.md)
- [policy_rejects_disabled_or_unknown_pool](../../../../../functions/crates/lpe-storage/src/storage_policy/policy_rejects_disabled_or_unknown_pool.md)
- [policy_changes_do_not_create_migration_jobs](../../../../../functions/crates/lpe-storage/src/storage_policy/policy_changes_do_not_create_migration_jobs.md)
- [policy_change_records_admin_audit_event](../../../../../functions/crates/lpe-storage/src/storage_policy/policy_change_records_admin_audit_event.md)