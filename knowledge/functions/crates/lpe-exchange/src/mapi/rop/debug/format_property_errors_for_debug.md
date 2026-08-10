---
type: Rust Function
title: format_property_errors_for_debug
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L377-L395
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/tests/fallback_property_errors_for_debug_match_wire_error_codes
---

# Signature

`pub(in crate::mapi) fn format_property_errors_for_debug( object: Option<&MapiObject>, principal: &AccountPrincipal, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, tags: &[u32], ) -> String`

# Called by

- [fallback_property_errors_for_debug_match_wire_error_codes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/fallback_property_errors_for_debug_match_wire_error_codes.md)