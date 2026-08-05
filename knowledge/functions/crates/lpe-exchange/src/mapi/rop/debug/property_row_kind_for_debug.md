---
type: Rust Function
title: property_row_kind_for_debug
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L45-L60
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/unsupported_specific_property_tags
---

# Signature

`pub(in crate::mapi) fn property_row_kind_for_debug( object: Option<&MapiObject>, principal: &AccountPrincipal, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, columns: &[u32], ) -> &'static str`

# Calls

- [unsupported_specific_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/unsupported_specific_property_tags.md)