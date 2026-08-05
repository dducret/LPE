---
type: Rust Function
title: pending_message_size
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L867-L877
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_body_text_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message
  - functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_property_value
---

# Signature

`pub(in crate::mapi) fn pending_message_size(properties: &HashMap<u32, MapiValue>) -> i64`

# Calls

- [pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property.md)
- [pending_body_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_body_text_property.md)

# Called by

- [mapi_submit_from_pending_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message.md)
- [pending_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_property_value.md)