---
type: Rust Function
title: changed_sync_emails
resource: crates/lpe-exchange/src/mapi/sync.rs#L156-L167
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(in crate::mapi) fn changed_sync_emails( emails: Vec<JmapEmail>, changed_ids: &[Uuid], ) -> Vec<JmapEmail>`

# Called by

- [append_synchronization_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)