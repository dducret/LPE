---
type: Rust Function
title: ipm_configuration_row_issues
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L198-L229
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_u32
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_ipm_configuration_contract_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_ipm_configuration_row_contract
---

# Signature

`fn ipm_configuration_row_issues( message: &crate::mapi_store::MapiAssociatedConfigMessage, ) -> String`

# Calls

- [associated_config_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value.md)
- [into_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_u32.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [format_ipm_configuration_contract_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_ipm_configuration_contract_summary.md)
- [format_ipm_configuration_row_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_ipm_configuration_row_contract.md)