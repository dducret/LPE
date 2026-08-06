---
type: Rust Function
title: saved_message_id_from_response
resource: crates/lpe-exchange/src/tests/mod.rs#L15139-L15147
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_outlook_contact_create_resolves_named_email_addresses
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_crud_uses_canonical_contacts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_custom_named_property_set_before_save_persists_on_created_item
---

# Signature

`fn saved_message_id_from_response(response_rops: &[u8], response_handle_index: u8) -> Option<u64>`

# Calls

- [position](../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [mapi_over_http_outlook_contact_create_resolves_named_email_addresses](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_outlook_contact_create_resolves_named_email_addresses.md)
- [mapi_over_http_contact_crud_uses_canonical_contacts](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_crud_uses_canonical_contacts.md)
- [mapi_over_http_custom_named_property_set_before_save_persists_on_created_item](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_custom_named_property_set_before_save_persists_on_created_item.md)