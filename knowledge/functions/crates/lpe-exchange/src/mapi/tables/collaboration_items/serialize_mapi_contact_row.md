---
type: Rust Function
title: serialize_mapi_contact_row
resource: crates/lpe-exchange/src/mapi/tables/collaboration_items.rs#L54-L78
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/outlook_contact_empty_email_table_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_contact_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(in crate::mapi) fn serialize_mapi_contact_row( contact: &MapiContact, folder_id: u64, mailbox_guid: Uuid, columns: &[u32], ) -> Vec<u8>`

# Calls

- [contact_property_value_with_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity.md)
- [outlook_contact_empty_email_table_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/outlook_contact_empty_email_table_value.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [format_contact_query_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_contact_query_row_summary.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)