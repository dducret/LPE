---
type: Rust Function
title: restriction_matches_email_with_attachments
resource: crates/lpe-exchange/src/mapi/properties.rs#L221-L299
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/message_recipients
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/recipient_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_attachment
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text
  - functions/crates/lpe-exchange/src/mapi/properties/content_restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_u32
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/size
  - functions/crates/lpe-exchange/src/mapi/properties/compare_i64
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email
  - functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_email_in_snapshot
---

# Signature

`pub(in crate::mapi) fn restriction_matches_email_with_attachments( restriction: Option<&MapiRestriction>, email: &JmapEmail, attachments: &[MapiAttachment], ) -> bool`

# Calls

- [canonical_property_storage_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [message_recipients](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/message_recipients.md)
- [restriction_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [recipient_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recipient_property_value.md)
- [restriction_matches_attachment](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_attachment.md)
- [email_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)
- [into_text](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text.md)
- [content_restriction_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/content_restriction_matches.md)
- [compare_mapi_values](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values.md)
- [into_u32](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_u32.md)
- [size](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/size.md)
- [compare_i64](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_i64.md)

# Called by

- [restriction_matches_email](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email.md)
- [restriction_matches_email_in_snapshot](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_email_in_snapshot.md)