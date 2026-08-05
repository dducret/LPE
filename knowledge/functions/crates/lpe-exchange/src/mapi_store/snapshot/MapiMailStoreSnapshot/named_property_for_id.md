---
type: Rust Method
title: named_property_for_id
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L434-L439
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/populate_special_message_named_property_definitions
---

# Signature

`pub(crate) fn named_property_for_id( &self, property_id: u16, ) -> Option<&crate::mapi::properties::MapiNamedProperty>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [populate_special_message_named_property_definitions](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/populate_special_message_named_property_definitions.md)