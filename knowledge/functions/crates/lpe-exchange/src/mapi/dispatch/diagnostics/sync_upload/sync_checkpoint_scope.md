---
type: Rust Function
title: sync_checkpoint_scope
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/sync_upload.rs#L3-L20
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
---

# Signature

`pub(in crate::mapi::dispatch) fn sync_checkpoint_scope( folder_id: u64, checkpoint_mailbox_id: Option<Uuid>, special_objects: &[mapi_mailstore::SpecialMessageSyncFact], ) -> &'static str`

# Calls

- [virtual_special_mailbox](../../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)