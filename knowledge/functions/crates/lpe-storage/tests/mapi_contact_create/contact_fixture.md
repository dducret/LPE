---
type: Rust Function
title: contact_fixture
resource: crates/lpe-storage/tests/mapi_contact_create.rs#L122-L205
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/tests/mapi_contact_create/mapi_contact_create_is_atomic_and_preserves_reserved_import_identity
---

# Signature

`async fn contact_fixture() -> Result<Option<ContactFixture>>`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [mapi_contact_create_is_atomic_and_preserves_reserved_import_identity](../../../../../functions/crates/lpe-storage/tests/mapi_contact_create/mapi_contact_create_is_atomic_and_preserves_reserved_import_identity.md)