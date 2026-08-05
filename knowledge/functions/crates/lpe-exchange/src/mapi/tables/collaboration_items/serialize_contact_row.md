---
type: Rust Function
title: serialize_contact_row
resource: crates/lpe-exchange/src/mapi/tables/collaboration_items.rs#L38-L52
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/contact_table_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_contact_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contact_table_projects_missing_secondary_email_slots_as_empty_strings
---

# Signature

`pub(in crate::mapi) fn serialize_contact_row( contact: &AccessibleContact, item_id: u64, folder_id: u64, columns: &[u32], ) -> Vec<u8>`

# Calls

- [contact_table_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/contact_table_property_value.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [serialize_pending_contact_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_contact_row.md)
- [contact_table_projects_missing_secondary_email_slots_as_empty_strings](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contact_table_projects_missing_secondary_email_slots_as_empty_strings.md)