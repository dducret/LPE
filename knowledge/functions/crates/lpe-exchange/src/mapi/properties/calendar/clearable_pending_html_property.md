---
type: Rust Function
title: clearable_pending_html_property
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L843-L851
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_binary_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi
---

# Signature

`fn clearable_pending_html_property(properties: &HashMap<u32, MapiValue>, existing: &str) -> String`

# Calls

- [pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property.md)
- [pending_html_binary_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_binary_property.md)

# Called by

- [event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi.md)