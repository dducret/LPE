---
type: Rust Method
title: assert_schema_version
resource: crates/lpe-storage/src/core.rs#L44-L68
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-storage/src/core/Storage/assert_required_schema_objects
---

# Signature

`async fn assert_schema_version(&self) -> Result<()>`

# Calls

- [context](../../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [assert_required_schema_objects](../../../../../../functions/crates/lpe-storage/src/core/Storage/assert_required_schema_objects.md)