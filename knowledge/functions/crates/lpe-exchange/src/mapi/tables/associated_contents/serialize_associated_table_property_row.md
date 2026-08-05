---
type: Rust Function
title: serialize_associated_table_property_row
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L272-L284
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_optional_property_row
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_row_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(super) fn serialize_associated_table_property_row( message: &AssociatedTableRow, mailbox_guid: Uuid, columns: &[u32], ) -> Vec<u8>`

# Calls

- [serialize_optional_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_optional_property_row.md)
- [associated_table_row_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_row_property_value.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)