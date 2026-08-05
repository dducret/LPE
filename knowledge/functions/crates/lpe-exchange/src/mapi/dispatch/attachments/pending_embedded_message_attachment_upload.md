---
type: Rust Function
title: pending_embedded_message_attachment_upload
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L1403-L1443
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response
---

# Signature

`pub(super) fn pending_embedded_message_attachment_upload( attach_num: u32, attachment_properties: &HashMap<u32, MapiValue>, embedded_properties: &HashMap<u32, MapiValue>, ) -> AttachmentUploadInput`

# Calls

- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)

# Called by

- [append_save_changes_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response.md)