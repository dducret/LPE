---
type: Rust Function
title: navigation_shortcut_object_property_is_deleted
resource: crates/lpe-exchange/src/mapi/properties/navigation_shortcut.rs#L111-L122
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property
---

# Signature

`pub(in crate::mapi) fn navigation_shortcut_object_property_is_deleted( object: Option<&MapiObject>, property_tag: u32, ) -> bool`

# Called by

- [fallback_default_specific_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property.md)