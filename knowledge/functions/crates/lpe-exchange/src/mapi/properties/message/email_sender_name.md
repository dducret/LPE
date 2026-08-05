---
type: Rust Function
title: email_sender_name
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L234-L241
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_mapi_messages
---

# Signature

`pub(in crate::mapi) fn email_sender_name(email: &JmapEmail) -> &str`

# Called by

- [normal_message_debug_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value.md)
- [email_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)
- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)
- [sort_emails](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails.md)
- [sort_mapi_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_mapi_messages.md)