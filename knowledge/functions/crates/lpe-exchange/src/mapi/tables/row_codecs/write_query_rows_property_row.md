---
type: Rust Function
title: write_query_rows_property_row
resource: crates/lpe-exchange/src/mapi/tables/row_codecs.rs#L10-L27
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_value
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_with_durable_identity_and_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_deleted_items_content_property_row
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_truncates_variable_property_values_to_microsoft_limit
---

# Signature

`pub(super) fn write_query_rows_property_row( response: &mut Vec<u8>, columns: &[u32], values: &[u8], )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_query_rows_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_value.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [serialize_message_property_row_with_durable_identity_and_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_with_durable_identity_and_mailbox_guid.md)
- [serialize_deleted_items_content_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_deleted_items_content_property_row.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [query_rows_truncates_variable_property_values_to_microsoft_limit](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_truncates_variable_property_values_to_microsoft_limit.md)