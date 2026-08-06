---
type: Rust Function
title: mapi_contact_commit_is_atomic_and_rejects_a_stale_modseq
resource: crates/lpe-storage/tests/mapi_contact_create.rs#L699-L794
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/mapi_contact_create/contact_fixture
  - functions/crates/lpe-storage/tests/mapi_contact_create/contact_input
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn mapi_contact_commit_is_atomic_and_rejects_a_stale_modseq() -> Result<()>`

# Calls

- [contact_fixture](../../../../../functions/crates/lpe-storage/tests/mapi_contact_create/contact_fixture.md)
- [contact_input](../../../../../functions/crates/lpe-storage/tests/mapi_contact_create/contact_input.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)