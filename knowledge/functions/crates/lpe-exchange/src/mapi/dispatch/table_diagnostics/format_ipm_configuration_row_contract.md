---
type: Rust Function
title: format_ipm_configuration_row_contract
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L154-L192
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_u32
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_mapi_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/ipm_configuration_row_issues
---

# Signature

`fn format_ipm_configuration_row_contract( message: &crate::mapi_store::MapiAssociatedConfigMessage, ) -> String`

# Calls

- [associated_config_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value.md)
- [into_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_u32.md)
- [format_debug_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_mapi_value.md)
- [ipm_configuration_row_issues](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/ipm_configuration_row_issues.md)