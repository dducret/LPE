---
type: Rust Function
title: policy_change_records_admin_audit_event
resource: crates/lpe-storage/src/storage_policy.rs#L1141-L1191
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_policy/insert_tenant_domain_account
  - functions/crates/lpe-storage/src/storage_policy/create_secondary_pool
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn policy_change_records_admin_audit_event()`

# Calls

- [insert_tenant_domain_account](../../../../../functions/crates/lpe-storage/src/storage_policy/insert_tenant_domain_account.md)
- [create_secondary_pool](../../../../../functions/crates/lpe-storage/src/storage_policy/create_secondary_pool.md)
- [set_tenant_storage_policy](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)