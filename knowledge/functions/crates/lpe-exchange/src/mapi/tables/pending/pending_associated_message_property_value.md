---
type: Rust Function
title: pending_associated_message_property_value
resource: crates/lpe-exchange/src/mapi/tables/pending.rs#L426-L444
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_associated_message_row
---

# Signature

`pub(in crate::mapi) fn pending_associated_message_property_value( principal: &AccountPrincipal, properties: &HashMap<u32, MapiValue>, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [pending_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_property_value.md)

# Called by

- [serialize_pending_associated_message_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_associated_message_row.md)