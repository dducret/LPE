---
type: Rust Function
title: rop_get_search_criteria_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L434-L460
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/get_search_criteria_include_restriction
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/get_search_criteria_include_folders
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_search_criteria_response
---

# Signature

`pub(in crate::mapi) fn rop_get_search_criteria_response( request: &RopRequest, restriction: &[u8], folder_ids: &[u64], search_flags: u32, ) -> Vec<u8>`

# Calls

- [get_search_criteria_include_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/get_search_criteria_include_restriction.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [get_search_criteria_include_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/get_search_criteria_include_folders.md)

# Called by

- [get_search_criteria_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_search_criteria_response.md)