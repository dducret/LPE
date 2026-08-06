---
type: Rust Function
title: contact_update_is_unchanged
resource: crates/lpe-storage/src/workspace.rs#L1172-L1216
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/workspace/client_address_book_id_for_role
  called_by:
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role
---

# Signature

`fn contact_update_is_unchanged( existing: &ClientContact, input: &UpsertClientContactInput, contact_book_role: &str, emails_json: &Value, phones_json: &Value, addresses_json: &Value, urls_json: &Value, source_payload_json: &Value, ) -> bool`

# Calls

- [client_address_book_id_for_role](../../../../../functions/crates/lpe-storage/src/workspace/client_address_book_id_for_role.md)

# Called by

- [upsert_client_contact_in_book_role](../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role.md)