---
type: Rust Function
title: changed_sync_mailboxes
resource: crates/lpe-exchange/src/mapi/sync.rs#L143-L154
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(in crate::mapi) fn changed_sync_mailboxes( mailboxes: Vec<JmapMailbox>, changed_ids: &[Uuid], ) -> Vec<JmapMailbox>`

# Called by

- [append_synchronization_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)