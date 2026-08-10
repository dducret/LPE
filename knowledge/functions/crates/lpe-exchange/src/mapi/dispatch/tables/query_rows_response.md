---
type: Rust Function
title: query_rows_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1979-L1988
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_for_principal
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
---

# Signature

`pub(super) fn query_rows_response( request: &RopRequest, object: Option<&mut MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, principal: &AccountPrincipal, ) -> Vec<u8>`

# Calls

- [rop_query_rows_response_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_for_principal.md)

# Called by

- [append_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)