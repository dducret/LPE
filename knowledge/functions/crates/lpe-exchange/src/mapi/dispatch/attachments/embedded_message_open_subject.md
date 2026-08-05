---
type: Rust Function
title: embedded_message_open_subject
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L1254-L1260
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_embedded_message_response
---

# Signature

`pub(super) fn embedded_message_open_subject(properties: &HashMap<u32, MapiValue>) -> String`

# Calls

- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)

# Called by

- [append_open_embedded_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_embedded_message_response.md)