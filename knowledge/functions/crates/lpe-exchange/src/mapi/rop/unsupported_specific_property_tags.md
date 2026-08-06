---
type: Rust Function
title: unsupported_specific_property_tags
resource: crates/lpe-exchange/src/mapi/rop.rs#L611-L629
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/property_is_unsupported_for_object
  - functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/debug/property_row_kind_for_debug
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
  - functions/crates/lpe-exchange/src/mapi/rop/tests/fallback_property_errors_for_debug_match_wire_error_codes
---

# Signature

`fn unsupported_specific_property_tags( object: Option<&MapiObject>, principal: &AccountPrincipal, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, columns: &[u32], ) -> Vec<u32>`

# Calls

- [property_is_unsupported_for_object](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_is_unsupported_for_object.md)
- [fallback_default_specific_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [property_row_kind_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/property_row_kind_for_debug.md)
- [log_get_properties_specific_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)
- [fallback_property_errors_for_debug_match_wire_error_codes](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/fallback_property_errors_for_debug_match_wire_error_codes.md)