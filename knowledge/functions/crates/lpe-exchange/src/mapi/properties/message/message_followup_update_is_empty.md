---
type: Rust Function
title: message_followup_update_is_empty
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L879-L897
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/apply_canonical_message_property_values
---

# Signature

`pub(in crate::mapi) fn message_followup_update_is_empty( update: &lpe_storage::JmapEmailFollowupUpdate, ) -> bool`

# Called by

- [apply_canonical_message_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/apply_canonical_message_property_values.md)