---
type: Rust Function
title: outlook_logon_bootstrap_row_shape
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L535-L557
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/folder/logon_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_logon_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
  - functions/crates/lpe-exchange/src/mapi/rop/tests/outlook_logon_bootstrap_details_match_exchange_absent_store_properties
---

# Signature

`pub(in crate::mapi) fn outlook_logon_bootstrap_row_shape( principal: &AccountPrincipal, columns: &[u32], ) -> OutlookLogonBootstrapRowShape`

# Calls

- [logon_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/logon_property_value.md)
- [serialize_logon_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_logon_row.md)

# Called by

- [log_get_properties_specific_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)
- [outlook_logon_bootstrap_details_match_exchange_absent_store_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/outlook_logon_bootstrap_details_match_exchange_absent_store_properties.md)