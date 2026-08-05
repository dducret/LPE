---
type: Rust Function
title: connect_pins_search_path_to_canonical_public_schema
resource: crates/lpe-storage/src/core.rs#L1031-L1081
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-storage/src/core/Storage/connect
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`async fn connect_pins_search_path_to_canonical_public_schema() -> Result<()>`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [connect](../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)