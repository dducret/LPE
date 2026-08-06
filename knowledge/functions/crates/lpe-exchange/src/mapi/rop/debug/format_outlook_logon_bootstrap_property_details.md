---
type: Rust Function
title: format_outlook_logon_bootstrap_property_details
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L610-L668
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/folder/logon_property_value
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_mailbox_owner_entry_id_details
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_ico_header_details
  - functions/crates/lpe-exchange/src/mapi/rop/debug/shapes/mapi_value_shape_for_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
  - functions/crates/lpe-exchange/src/mapi/rop/tests/outlook_logon_bootstrap_details_match_exchange_absent_store_properties
---

# Signature

`pub(in crate::mapi) fn format_outlook_logon_bootstrap_property_details( principal: &AccountPrincipal, columns: &[u32], ) -> String`

# Calls

- [logon_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/logon_property_value.md)
- [format_mailbox_owner_entry_id_details](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_mailbox_owner_entry_id_details.md)
- [format_ico_header_details](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_ico_header_details.md)
- [mapi_value_shape_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/shapes/mapi_value_shape_for_debug.md)

# Called by

- [log_get_properties_specific_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)
- [outlook_logon_bootstrap_details_match_exchange_absent_store_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/outlook_logon_bootstrap_details_match_exchange_absent_store_properties.md)