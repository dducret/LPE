---
type: Rust Function
title: email_sent_representing_name
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L256-L258
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/folder/sent_representing_entry_id
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
---

# Signature

`pub(in crate::mapi) fn email_sent_representing_name(email: &JmapEmail) -> &str`

# Called by

- [normal_message_debug_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value.md)
- [sent_representing_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/sent_representing_entry_id.md)
- [email_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)
- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)