---
type: Rust Function
title: mapi_content_table_order_by
resource: crates/lpe-exchange/src/store/storage_impl/mapi_helpers.rs#L812-L837
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_content_table_order_by_uses_projected_columns
---

# Signature

`fn mapi_content_table_order_by(sort_orders: &[MapiContentTableSort]) -> String`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_content_table_order_by_uses_projected_columns](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_content_table_order_by_uses_projected_columns.md)