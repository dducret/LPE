---
type: Rust Function
title: policy_changes_do_not_create_migration_jobs
resource: crates/lpe-storage/src/storage_policy.rs#L1116-L1138
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_policy/insert_tenant_domain_account
  - functions/crates/lpe-storage/src/storage_policy/create_secondary_pool
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn policy_changes_do_not_create_migration_jobs()`

# Calls

- [insert_tenant_domain_account](../../../../../functions/crates/lpe-storage/src/storage_policy/insert_tenant_domain_account.md)
- [create_secondary_pool](../../../../../functions/crates/lpe-storage/src/storage_policy/create_secondary_pool.md)
- [set_tenant_storage_policy](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)