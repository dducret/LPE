---
type: Rust Function
title: categories_from_mapi_value
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L876-L889
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_from_mapi_values
---

# Signature

`pub(in crate::mapi) fn categories_from_mapi_value(value: MapiValue) -> Result<Vec<String>>`

# Called by

- [message_followup_update_from_mapi_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_from_mapi_values.md)