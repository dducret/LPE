---
type: Rust Function
title: mapi_submit_from_pending_message
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L533-L597
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_body_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/message/optional_pending_submit_address
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/message/pending_recipients_for_import
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_message_size
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response
  - functions/crates/lpe-exchange/src/mapi/properties/tests/pending_html_only_message_derives_plain_body_for_save_and_submit
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_inline_image_html_body_preserves_cid_for_save_and_submit
---

# Signature

`pub(in crate::mapi) fn mapi_submit_from_pending_message( principal: &AccountPrincipal, properties: &HashMap<u32, MapiValue>, recipients: &[PendingRecipient], ) -> SubmitMessageInput`

# Calls

- [pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property.md)
- [pending_body_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_body_text_property.md)
- [optional_pending_submit_address](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/optional_pending_submit_address.md)
- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)
- [pending_recipients_for_import](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/pending_recipients_for_import.md)
- [pending_html_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_property.md)
- [pending_message_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_message_size.md)

# Called by

- [append_submit_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response.md)
- [pending_html_only_message_derives_plain_body_for_save_and_submit](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/pending_html_only_message_derives_plain_body_for_save_and_submit.md)
- [microsoft_inline_image_html_body_preserves_cid_for_save_and_submit](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_inline_image_html_body_preserves_cid_for_save_and_submit.md)