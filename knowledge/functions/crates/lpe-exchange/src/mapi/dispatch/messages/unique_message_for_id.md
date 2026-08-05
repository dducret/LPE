---
type: Rust Function
title: unique_message_for_id
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L50-L56
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/mapi_item_id_matches
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_object_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/apply_staged_message_property_values
---

# Signature

`pub(super) fn unique_message_for_id(message_id: u64, emails: &[JmapEmail]) -> Option<&JmapEmail>`

# Calls

- [mapi_item_id_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/mapi_item_id_matches.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [custom_property_object_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_object_identity.md)
- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [apply_staged_message_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/apply_staged_message_property_values.md)