---
type: Rust Function
title: query_columns_all_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1374-L1380
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_columns_all_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
---

# Signature

`pub(super) fn query_columns_all_response( request: &RopRequest, object: Option<&MapiObject>, snapshot: &MapiMailStoreSnapshot, ) -> Vec<u8>`

# Calls

- [rop_query_columns_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_columns_all_response.md)

# Called by

- [append_table_control_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)