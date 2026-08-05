---
type: Rust Function
title: recipient_property_value
resource: crates/lpe-exchange/src/mapi/properties.rs#L301-L325
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments
---

# Signature

`fn recipient_property_value(recipient: &MapiRecipient<'_>, property_tag: u32) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)

# Called by

- [restriction_matches_email_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments.md)