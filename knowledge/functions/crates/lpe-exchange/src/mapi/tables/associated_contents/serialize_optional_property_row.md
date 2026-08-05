---
type: Rust Function
title: serialize_optional_property_row
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L326-L343
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_views_property_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_views_property_row_for_principal
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_associated_table_property_row
---

# Signature

`fn serialize_optional_property_row(columns: &[u32], values: Vec<Option<MapiValue>>) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)

# Called by

- [serialize_common_views_property_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_views_property_row_with_mailbox_guid.md)
- [serialize_common_views_property_row_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_views_property_row_for_principal.md)
- [serialize_associated_table_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_associated_table_property_row.md)