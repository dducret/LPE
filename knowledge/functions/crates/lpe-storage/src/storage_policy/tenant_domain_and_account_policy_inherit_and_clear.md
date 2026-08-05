---
type: Rust Function
title: tenant_domain_and_account_policy_inherit_and_clear
resource: crates/lpe-storage/src/storage_policy.rs#L976-L1026
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_policy/insert_tenant_domain_account
  - functions/crates/lpe-storage/src/storage_policy/create_secondary_pool
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/storage_policy/Storage/fetch_tenant_storage_policy_overview
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_domain_storage_policy
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_account_storage_policy
---

# Signature

`async fn tenant_domain_and_account_policy_inherit_and_clear()`

# Calls

- [insert_tenant_domain_account](../../../../../functions/crates/lpe-storage/src/storage_policy/insert_tenant_domain_account.md)
- [create_secondary_pool](../../../../../functions/crates/lpe-storage/src/storage_policy/create_secondary_pool.md)
- [set_tenant_storage_policy](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [fetch_tenant_storage_policy_overview](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_tenant_storage_policy_overview.md)
- [set_domain_storage_policy](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_domain_storage_policy.md)
- [set_account_storage_policy](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_account_storage_policy.md)