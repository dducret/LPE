---
type: Rust Function
title: flagged_property_error_code
resource: crates/lpe-exchange/src/mapi/rop.rs#L756-L775
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/property_is_unsupported_for_object
  - functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/write_flagged_property_row
---

# Signature

`fn flagged_property_error_code( object: Option<&MapiObject>, principal: &AccountPrincipal, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, tag: u32, ) -> u32`

# Calls

- [property_is_unsupported_for_object](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_is_unsupported_for_object.md)
- [fallback_default_specific_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property.md)

# Called by

- [write_flagged_property_row](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/write_flagged_property_row.md)