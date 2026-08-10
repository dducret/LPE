---
type: Rust Function
title: optional_pending_submit_address
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L747-L752
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message
---

# Signature

`fn optional_pending_submit_address( properties: &HashMap<u32, MapiValue>, tags: &[u32], ) -> Option<String>`

# Calls

- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)

# Called by

- [mapi_submit_from_pending_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message.md)