---
type: Rust Function
title: contact_input
resource: crates/lpe-storage/tests/mapi_contact_create.rs#L223-L270
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/tests/mapi_contact_create/create_input
  - functions/crates/lpe-storage/tests/mapi_contact_create/mapi_contact_commit_is_atomic_and_rejects_a_stale_modseq
---

# Signature

`fn contact_input(account_id: Uuid, contact_id: Uuid, name: &str) -> UpsertClientContactInput`

# Called by

- [create_input](../../../../../functions/crates/lpe-storage/tests/mapi_contact_create/create_input.md)
- [mapi_contact_commit_is_atomic_and_rejects_a_stale_modseq](../../../../../functions/crates/lpe-storage/tests/mapi_contact_create/mapi_contact_commit_is_atomic_and_rejects_a_stale_modseq.md)