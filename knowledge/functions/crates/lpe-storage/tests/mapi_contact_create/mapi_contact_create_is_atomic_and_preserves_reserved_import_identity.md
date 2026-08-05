---
type: Rust Function
title: mapi_contact_create_is_atomic_and_preserves_reserved_import_identity
resource: crates/lpe-storage/tests/mapi_contact_create.rs#L292-L695
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/mapi_contact_create/contact_fixture
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-core/src/sieve/context
---

# Signature

`async fn mapi_contact_create_is_atomic_and_preserves_reserved_import_identity() -> Result<()>`

# Calls

- [contact_fixture](../../../../../functions/crates/lpe-storage/tests/mapi_contact_create/contact_fixture.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)