---
type: Rust Function
title: startup_rejects_tagged_schema_without_required_mapi_shape
resource: crates/lpe-storage/src/core.rs#L643-L985
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/assert_required_schema_objects
---

# Signature

`async fn startup_rejects_tagged_schema_without_required_mapi_shape() -> Result<()>`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [assert_required_schema_objects](../../../../../functions/crates/lpe-storage/src/core/Storage/assert_required_schema_objects.md)