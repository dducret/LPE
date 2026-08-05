---
type: Rust Function
title: rop_get_status_response
resource: crates/lpe-exchange/src/mapi/tables/controls.rs#L67-L79
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_status_response
---

# Signature

`pub(in crate::mapi) fn rop_get_status_response( request: &RopRequest, object: Option<&MapiObject>, ) -> Vec<u8>`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [get_status_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_status_response.md)