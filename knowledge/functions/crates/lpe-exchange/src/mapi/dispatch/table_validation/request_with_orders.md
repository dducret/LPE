---
type: Rust Function
title: request_with_orders
resource: crates/lpe-exchange/src/mapi/dispatch/table_validation.rs#L347-L368
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/request
---

# Signature

`fn request_with_orders( flags: u8, category_count: u16, expanded_count: u16, orders: &[(u32, u8)], ) -> RopRequest`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/request.md)