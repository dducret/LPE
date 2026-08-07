---
type: Rust Function
title: client_address_book_id_for_role
resource: crates/lpe-storage/src/workspace.rs#L1245-L1252
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role
  - functions/crates/lpe-storage/src/workspace/contact_update_is_unchanged
---

# Signature

`fn client_address_book_id_for_role(role: &str) -> &'static str`

# Called by

- [upsert_client_contact_in_book_role](../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role.md)
- [contact_update_is_unchanged](../../../../../functions/crates/lpe-storage/src/workspace/contact_update_is_unchanged.md)