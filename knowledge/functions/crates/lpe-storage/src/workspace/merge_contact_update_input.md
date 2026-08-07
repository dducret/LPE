---
type: Rust Function
title: merge_contact_update_input
resource: crates/lpe-storage/src/workspace.rs#L1108-L1139
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/workspace/contact_json_with_primary_value
  called_by:
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role
  - functions/crates/lpe-storage/src/workspace/contact_update_merges_missing_rich_fields
  - functions/crates/lpe-storage/src/workspace/contact_update_can_clear_explicit_rich_fields
---

# Signature

`fn merge_contact_update_input( existing: &ClientContact, mut input: UpsertClientContactInput, ) -> UpsertClientContactInput`

# Calls

- [contact_json_with_primary_value](../../../../../functions/crates/lpe-storage/src/workspace/contact_json_with_primary_value.md)

# Called by

- [upsert_client_contact_in_book_role](../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role.md)
- [contact_update_merges_missing_rich_fields](../../../../../functions/crates/lpe-storage/src/workspace/contact_update_merges_missing_rich_fields.md)
- [contact_update_can_clear_explicit_rich_fields](../../../../../functions/crates/lpe-storage/src/workspace/contact_update_can_clear_explicit_rich_fields.md)