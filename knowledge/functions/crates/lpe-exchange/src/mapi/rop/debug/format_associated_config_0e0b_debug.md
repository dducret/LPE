---
type: Rust Function
title: format_associated_config_0e0b_debug
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L851-L898
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_u32
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_ipm_configuration_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/rop/tests/associated_config_0e0b_debug_reports_stored_value_and_fallback
---

# Signature

`pub(in crate::mapi) fn format_associated_config_0e0b_debug( columns: &[u32], message: &crate::mapi_store::MapiAssociatedConfigMessage, fallback_tags: &[u32], ) -> String`

# Calls

- [mapi_properties_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [associated_config_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value.md)
- [into_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_u32.md)

# Called by

- [format_ipm_configuration_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_ipm_configuration_getprops_contract.md)
- [associated_config_0e0b_debug_reports_stored_value_and_fallback](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/associated_config_0e0b_debug_reports_stored_value_and_fallback.md)