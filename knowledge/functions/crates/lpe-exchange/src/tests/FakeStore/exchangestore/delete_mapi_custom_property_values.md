---
type: Rust Method
title: delete_mapi_custom_property_values
resource: crates/lpe-exchange/src/tests/mod.rs#L7070-L7095
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/delete_custom_property_values
  - functions/crates/lpe-exchange/src/tests/fake_store_custom_property_values_survive_restart_style_clone
---

# Signature

`fn delete_mapi_custom_property_values<'a>( &'a self, account_id: Uuid, object_kind: MapiCustomPropertyObjectKind, canonical_id: Uuid, property_tags: &'a [u32], ) -> StoreFuture<'a, ()>`

# Called by

- [delete_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/delete_custom_property_values.md)
- [fake_store_custom_property_values_survive_restart_style_clone](../../../../../../../functions/crates/lpe-exchange/src/tests/fake_store_custom_property_values_survive_restart_style_clone.md)