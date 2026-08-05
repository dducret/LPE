---
type: Rust Function
title: abandon_event_attachment_transaction
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L1161-L1173
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/clear_event_attachment_transaction
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
---

# Signature

`pub(super) fn abandon_event_attachment_transaction(session: &mut MapiSession, parent_handle: u32)`

# Calls

- [clear_event_attachment_transaction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/clear_event_attachment_transaction.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [append_release_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)