---
type: Rust Function
title: next_pending_attachment_num
resource: crates/lpe-exchange/src/mapi/sync.rs#L1399-L1438
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response
---

# Signature

`pub(in crate::mapi) fn next_pending_attachment_num( session: &MapiSession, folder_id: u64, message_id: u64, snapshot: &MapiMailStoreSnapshot, ) -> u32`

# Called by

- [append_create_attachment_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response.md)