---
type: Rust Function
title: create_bookmark_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1344-L1353
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_create_bookmark_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
---

# Signature

`pub(super) fn create_bookmark_response( request: &RopRequest, object: Option<&mut MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, ) -> Vec<u8>`

# Calls

- [rop_create_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_create_bookmark_response.md)

# Called by

- [append_table_control_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)