---
type: Rust Function
title: contact_input
resource: crates/lpe-storage/tests/mapi_contact_create.rs#L222-L269
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/tests/mapi_contact_create/create_input
---

# Signature

`fn contact_input(account_id: Uuid, contact_id: Uuid, name: &str) -> UpsertClientContactInput`

# Called by

- [create_input](../../../../../functions/crates/lpe-storage/tests/mapi_contact_create/create_input.md)