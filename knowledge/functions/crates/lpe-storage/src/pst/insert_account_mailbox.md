---
type: Rust Function
title: insert_account_mailbox
resource: crates/lpe-storage/src/pst.rs#L630-L682
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn insert_account_mailbox( storage: &Storage, tenant_id: Uuid, domain_id: Uuid, account_id: Uuid, mailbox_id: Uuid, )`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)