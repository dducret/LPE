---
type: Rust Function
title: get_search_criteria_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1392-L1399
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_search_criteria_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_get_search_criteria_response
---

# Signature

`pub(super) fn get_search_criteria_response( request: &RopRequest, restriction: &[u8], folder_ids: &[u64], search_flags: u32, ) -> Vec<u8>`

# Calls

- [rop_get_search_criteria_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_search_criteria_response.md)

# Called by

- [append_get_search_criteria_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_get_search_criteria_response.md)