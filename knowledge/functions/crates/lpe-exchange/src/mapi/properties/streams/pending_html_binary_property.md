---
type: Rust Function
title: pending_html_binary_property
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L539-L549
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/clearable_pending_html_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_property
---

# Signature

`pub(in crate::mapi) fn pending_html_binary_property( properties: &HashMap<u32, MapiValue>, ) -> Option<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [clearable_pending_html_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/clearable_pending_html_property.md)
- [message_body_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data.md)
- [pending_html_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_property.md)