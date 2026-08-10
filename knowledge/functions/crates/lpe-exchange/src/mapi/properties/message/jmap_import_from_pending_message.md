---
type: Rust Function
title: jmap_import_from_pending_message
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L465-L517
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_body_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/message/conversation_id_from_index
  - functions/crates/lpe-exchange/src/mapi/properties/message/pending_recipients_for_import
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/properties/tests/pending_html_only_message_derives_plain_body_for_save_and_submit
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_inline_image_html_body_preserves_cid_for_save_and_submit
---

# Signature

`pub(in crate::mapi) fn jmap_import_from_pending_message( principal: &AccountPrincipal, mailbox: &JmapMailbox, properties: &HashMap<u32, MapiValue>, recipients: &[PendingRecipient], attachments: Vec<AttachmentUploadInput>, ) -> JmapImportedEmailInput`

# Calls

- [pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property.md)
- [pending_body_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_body_text_property.md)
- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [conversation_id_from_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/conversation_id_from_index.md)
- [pending_recipients_for_import](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/pending_recipients_for_import.md)
- [pending_html_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_property.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [pending_html_only_message_derives_plain_body_for_save_and_submit](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/pending_html_only_message_derives_plain_body_for_save_and_submit.md)
- [microsoft_inline_image_html_body_preserves_cid_for_save_and_submit](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_inline_image_html_body_preserves_cid_for_save_and_submit.md)