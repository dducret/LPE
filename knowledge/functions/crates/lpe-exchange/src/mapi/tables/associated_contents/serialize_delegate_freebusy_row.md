---
type: Rust Function
title: serialize_delegate_freebusy_row
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L105-L111
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_freebusy_row_staged
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(in crate::mapi) fn serialize_delegate_freebusy_row( message: &MapiDelegateFreeBusyMessage, mailbox_guid: Uuid, columns: &[u32], ) -> Vec<u8>`

# Calls

- [serialize_freebusy_row_staged](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_freebusy_row_staged.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)