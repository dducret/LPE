---
type: Rust Function
title: serialize_common_views_property_row_with_mailbox_guid
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L32-L44
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_optional_property_row
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(super) fn serialize_common_views_property_row_with_mailbox_guid( message: &MapiCommonViewsMessage, mailbox_guid: Uuid, columns: &[u32], ) -> Vec<u8>`

# Calls

- [serialize_optional_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_optional_property_row.md)
- [common_views_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)