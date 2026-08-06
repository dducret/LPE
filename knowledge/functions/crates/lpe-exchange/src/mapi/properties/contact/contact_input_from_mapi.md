---
type: Rust Function
title: contact_input_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L376-L504
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_display_name_from_structured
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_emails_json_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_phones_json_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_urls_json_from_mapi
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_pending_contact
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi_with_deletions
  - functions/crates/lpe-exchange/src/mapi/properties/contact/apply_canonical_contact_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_contact_narrow_update_omits_unowned_rich_fields
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_contact_row
---

# Signature

`pub(in crate::mapi) fn contact_input_from_mapi( account_id: Uuid, id: Option<Uuid>, existing: &AccessibleContact, properties: &HashMap<u32, MapiValue>, ) -> UpsertClientContactInput`

# Calls

- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)
- [contact_display_name_from_structured](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_display_name_from_structured.md)
- [contact_emails_json_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_emails_json_from_mapi.md)
- [contact_phones_json_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_phones_json_from_mapi.md)
- [contact_urls_json_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_urls_json_from_mapi.md)

# Called by

- [save_pending_contact](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_pending_contact.md)
- [contact_input_from_mapi_with_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi_with_deletions.md)
- [apply_canonical_contact_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/apply_canonical_contact_property_values.md)
- [mapi_contact_narrow_update_omits_unowned_rich_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_contact_narrow_update_omits_unowned_rich_fields.md)
- [serialize_pending_contact_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_contact_row.md)