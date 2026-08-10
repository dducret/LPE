---
type: Rust Function
title: test_storage
resource: crates/lpe-storage/src/blob_store/tests.rs#L8-L27
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/core/Storage/connect
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/tools/rca_outlook_connectivity_check/execute
---

# Signature

`async fn test_storage() -> Option<Storage>`

# Calls

- [connect](../../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)