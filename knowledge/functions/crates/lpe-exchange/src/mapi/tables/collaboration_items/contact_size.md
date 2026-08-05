---
type: Rust Function
title: contact_size
resource: crates/lpe-exchange/src/mapi/tables/collaboration_items.rs#L3-L12
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity
  - functions/crates/lpe-exchange/src/mapi/sync/contact_sync_object
---

# Signature

`pub(in crate::mapi) fn contact_size(contact: &AccessibleContact) -> i64`

# Called by

- [contact_property_value_with_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity.md)
- [contact_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/contact_sync_object.md)