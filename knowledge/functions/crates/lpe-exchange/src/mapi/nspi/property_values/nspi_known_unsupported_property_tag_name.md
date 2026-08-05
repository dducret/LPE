---
type: Rust Function
title: nspi_known_unsupported_property_tag_name
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L145-L149
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/format_nspi_known_unsupported_property_tags_for_debug
---

# Signature

`pub(in crate::mapi) fn nspi_known_unsupported_property_tag_name(tag: u32) -> Option<&'static str>`

# Called by

- [format_nspi_known_unsupported_property_tags_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/format_nspi_known_unsupported_property_tags_for_debug.md)