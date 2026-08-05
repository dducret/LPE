---
type: Rust Function
title: transport_headers
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L357-L376
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/display_to
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/display_cc
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
---

# Signature

`fn transport_headers(email: &JmapEmail) -> String`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [display_to](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/display_to.md)
- [display_cc](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/display_cc.md)

# Called by

- [email_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)