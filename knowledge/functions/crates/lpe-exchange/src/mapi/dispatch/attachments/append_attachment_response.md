---
type: Rust Function
title: append_attachment_response
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L16-L115
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_valid_attachments_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_attachment_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_embedded_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) async fn append_attachment_response<S, V>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, validator: &Validator<V>, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, ) where S: ExchangeStore, V: Detector,`

# Calls

- [append_get_valid_attachments_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_valid_attachments_response.md)
- [append_get_attachment_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_attachment_table_response.md)
- [append_open_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_attachment_response.md)
- [append_create_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response.md)
- [append_delete_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response.md)
- [append_open_embedded_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_embedded_message_response.md)
- [append_save_changes_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)