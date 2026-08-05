---
type: Rust Function
title: policy_rejects_disabled_or_unknown_pool
resource: crates/lpe-storage/src/storage_policy.rs#L1029-L1073
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_policy/insert_tenant_domain_account
  - functions/crates/lpe-storage/src/storage_policy/create_secondary_pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy
---

# Signature

`async fn policy_rejects_disabled_or_unknown_pool()`

# Calls

- [insert_tenant_domain_account](../../../../../functions/crates/lpe-storage/src/storage_policy/insert_tenant_domain_account.md)
- [create_secondary_pool](../../../../../functions/crates/lpe-storage/src/storage_policy/create_secondary_pool.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [set_tenant_storage_policy](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy.md)