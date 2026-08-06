---
type: Rust Function
title: modeled_zero_or_default_property
resource: crates/lpe-exchange/src/mapi/rop.rs#L942-L1022
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_modeled_empty_property
  - functions/crates/lpe-exchange/src/mapi/rop/is_modeled_empty_special_folder_class_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_property_value_shapes_for_debug
---

# Signature

`fn modeled_zero_or_default_property(object: Option<&MapiObject>, tag: u32) -> bool`

# Calls

- [canonical_property_storage_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [associated_config_modeled_empty_property](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_modeled_empty_property.md)
- [is_modeled_empty_special_folder_class_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/is_modeled_empty_special_folder_class_property.md)

# Called by

- [fallback_default_specific_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property.md)
- [log_get_properties_specific_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)
- [format_property_value_shapes_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_property_value_shapes_for_debug.md)