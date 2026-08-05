---
type: Rust Function
title: sent_representing_entry_id
resource: crates/lpe-exchange/src/mapi/properties/folder.rs#L264-L275
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sent_representing_name
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sent_representing_address
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_permanent_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
---

# Signature

`pub(in crate::mapi) fn sent_representing_entry_id(email: &JmapEmail) -> Vec<u8>`

# Calls

- [email_sent_representing_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sent_representing_name.md)
- [email_sent_representing_address](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sent_representing_address.md)
- [nspi_entry_permanent_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_permanent_entry_id.md)

# Called by

- [normal_message_debug_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value.md)
- [email_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)
- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)