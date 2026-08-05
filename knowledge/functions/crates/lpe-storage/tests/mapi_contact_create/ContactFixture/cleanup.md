---
type: Rust Method
title: cleanup
resource: crates/lpe-storage/tests/mapi_contact_create.rs#L108-L119
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`async fn cleanup(mut self) -> Result<()>`

# Calls

- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)