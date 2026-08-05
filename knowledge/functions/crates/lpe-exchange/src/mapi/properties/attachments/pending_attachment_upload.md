---
type: Rust Function
title: pending_attachment_upload
resource: crates/lpe-exchange/src/mapi/properties/attachments.rs#L91-L117
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_file_name
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_media_type
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response
---

# Signature

`pub(in crate::mapi) fn pending_attachment_upload( attach_num: u32, properties: &HashMap<u32, MapiValue>, data: Vec<u8>, ) -> AttachmentUploadInput`

# Calls

- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [as_bool](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool.md)
- [pending_attachment_file_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_file_name.md)
- [pending_attachment_media_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_media_type.md)

# Called by

- [append_save_changes_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response.md)