---
type: Rust Function
title: message_flags
resource: crates/lpe-exchange/src/mapi/tables/flags.rs#L3-L5
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/canonical_message_flags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_mapi_messages
---

# Signature

`pub(in crate::mapi) fn message_flags(email: &JmapEmail) -> u32`

# Calls

- [canonical_message_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_message_flags.md)

# Called by

- [normal_message_debug_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value.md)
- [email_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)
- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)
- [sort_emails](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails.md)
- [sort_mapi_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_mapi_messages.md)