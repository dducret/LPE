---
type: Rust Method
title: cleanup
resource: crates/lpe-exchange/src/tests/mod.rs#L165-L176
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`async fn cleanup(mut self) -> anyhow::Result<()>`

# Calls

- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)