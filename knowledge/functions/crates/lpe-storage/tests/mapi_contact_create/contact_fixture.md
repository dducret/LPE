---
type: Rust Function
title: contact_fixture
resource: crates/lpe-storage/tests/mapi_contact_create.rs#L123-L206
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-storage/tests/mapi_contact_create/mapi_contact_create_is_atomic_and_preserves_reserved_import_identity
  - functions/crates/lpe-storage/tests/mapi_contact_create/mapi_contact_commit_is_atomic_and_rejects_a_stale_modseq
---

# Signature

`async fn contact_fixture() -> Result<Option<ContactFixture>>`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [mapi_contact_create_is_atomic_and_preserves_reserved_import_identity](../../../../../functions/crates/lpe-storage/tests/mapi_contact_create/mapi_contact_create_is_atomic_and_preserves_reserved_import_identity.md)
- [mapi_contact_commit_is_atomic_and_rejects_a_stale_modseq](../../../../../functions/crates/lpe-storage/tests/mapi_contact_create/mapi_contact_commit_is_atomic_and_rejects_a_stale_modseq.md)