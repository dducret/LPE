---
type: Rust Function
title: attachment_metadata_is_embedded_message
resource: crates/lpe-exchange/src/mapi/properties/attachments.rs#L53-L61
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/open_embedded_message_source
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_is_embedded_message
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_method_value_from_metadata
---

# Signature

`pub(in crate::mapi) fn attachment_metadata_is_embedded_message( media_type: &str, file_name: &str, ) -> bool`

# Called by

- [open_embedded_message_source](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/open_embedded_message_source.md)
- [attachment_is_embedded_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_is_embedded_message.md)
- [attachment_method_value_from_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_method_value_from_metadata.md)