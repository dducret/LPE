---
type: Rust Function
title: rop_open_message_response_with_named_properties
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L41-L62
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string_reduced_unicode_when_lossless
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response
  - functions/crates/lpe-exchange/src/mapi/rop/tests/open_message_response_does_not_advertise_missing_recipient_rows
  - functions/crates/lpe-exchange/src/mapi/rop/tests/open_message_response_uses_reduced_unicode_for_exchange_configuration_subjects
  - functions/crates/lpe-exchange/src/mapi/rop/tests/open_message_response_keeps_full_unicode_when_reduced_unicode_would_lose_data
---

# Signature

`pub(in crate::mapi) fn rop_open_message_response_with_named_properties( request: &RopRequest, subject: &str, recipient_count: usize, has_named_properties: bool, use_reduced_unicode: bool, ) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_typed_string_reduced_unicode_when_lossless](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string_reduced_unicode_when_lossless.md)
- [write_typed_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string.md)

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [rop_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response.md)
- [open_message_response_does_not_advertise_missing_recipient_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/open_message_response_does_not_advertise_missing_recipient_rows.md)
- [open_message_response_uses_reduced_unicode_for_exchange_configuration_subjects](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/open_message_response_uses_reduced_unicode_for_exchange_configuration_subjects.md)
- [open_message_response_keeps_full_unicode_when_reduced_unicode_would_lose_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/open_message_response_keeps_full_unicode_when_reduced_unicode_would_lose_data.md)