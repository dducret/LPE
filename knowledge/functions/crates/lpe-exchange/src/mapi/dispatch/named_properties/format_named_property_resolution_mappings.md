---
type: Rust Function
title: format_named_property_resolution_mappings
resource: crates/lpe-exchange/src/mapi/dispatch/named_properties.rs#L628-L646
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
---

# Signature

`fn format_named_property_resolution_mappings( properties: &[MapiNamedProperty], property_ids: &[u16], sources: &[&str], ) -> String`

# Called by

- [append_get_property_ids_from_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)