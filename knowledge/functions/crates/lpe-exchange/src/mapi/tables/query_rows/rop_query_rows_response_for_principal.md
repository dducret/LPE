---
type: Rust Function
title: rop_query_rows_response_for_principal
resource: crates/lpe-exchange/src/mapi/tables/query_rows.rs#L23-L40
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/query_rows_response
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_query_rows_projects_mailbox_store_object_entry_id
---

# Signature

`pub(in crate::mapi) fn rop_query_rows_response_for_principal( request: &RopRequest, object: Option<&mut MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, principal: &AccountPrincipal, ) -> Vec<u8>`

# Calls

- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)

# Called by

- [query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/query_rows_response.md)
- [common_views_query_rows_projects_mailbox_store_object_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_query_rows_projects_mailbox_store_object_entry_id.md)