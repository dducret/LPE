---
type: Rust Function
title: attachment_property_value
resource: crates/lpe-exchange/src/mapi/properties/attachments.rs#L3-L39
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_file_extension
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_method_value
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_is_inline
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_attachment
---

# Signature

`pub(in crate::mapi) fn attachment_property_value( attachment: &MapiAttachment, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [attachment_file_extension](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_file_extension.md)
- [attachment_method_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_method_value.md)
- [attachment_is_inline](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_is_inline.md)

# Called by

- [restriction_matches_attachment](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_attachment.md)