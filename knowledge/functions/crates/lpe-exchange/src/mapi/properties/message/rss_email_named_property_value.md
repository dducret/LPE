---
type: Rust Function
title: rss_email_named_property_value
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L426-L447
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
---

# Signature

`fn rss_email_named_property_value(email: &JmapEmail, property_tag: u32) -> Option<MapiValue>`

# Calls

- [canonical_message_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number.md)

# Called by

- [email_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)