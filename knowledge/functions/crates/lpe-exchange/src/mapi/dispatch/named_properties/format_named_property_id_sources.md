---
type: Rust Function
title: format_named_property_id_sources
resource: crates/lpe-exchange/src/mapi/dispatch/named_properties.rs#L581-L591
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
---

# Signature

`fn format_named_property_id_sources(sources: &[&str]) -> String`

# Calls

- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [append_get_property_ids_from_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)