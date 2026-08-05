---
type: Rust Function
title: attachment_is_embedded_message
resource: crates/lpe-exchange/src/mapi/properties/attachments.rs#L49-L51
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_metadata_is_embedded_message
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/open_embedded_message_source
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_method_value
---

# Signature

`pub(in crate::mapi) fn attachment_is_embedded_message(attachment: &MapiAttachment) -> bool`

# Calls

- [attachment_metadata_is_embedded_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_metadata_is_embedded_message.md)

# Called by

- [open_embedded_message_source](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/open_embedded_message_source.md)
- [attachment_method_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_method_value.md)