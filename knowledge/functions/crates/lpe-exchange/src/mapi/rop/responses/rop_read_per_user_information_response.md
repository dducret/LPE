---
type: Rust Function
title: rop_read_per_user_information_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L527-L549
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_data_offset
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_max_data_size
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_read_per_user_information_response
---

# Signature

`pub(in crate::mapi) fn rop_read_per_user_information_response( request: &RopRequest, stream: &[u8], ) -> Vec<u8>`

# Calls

- [per_user_data_offset](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_data_offset.md)
- [per_user_max_data_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_max_data_size.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_read_per_user_information_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_read_per_user_information_response.md)