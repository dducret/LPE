---
type: Rust Function
title: format_named_property_family_summary
resource: crates/lpe-exchange/src/mapi/dispatch/named_properties.rs#L593-L608
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/named_property_family_summary_groups_guid_and_kind
---

# Signature

`pub(super) fn format_named_property_family_summary(properties: &[MapiNamedProperty]) -> String`

# Calls

- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [append_get_property_ids_from_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)
- [named_property_family_summary_groups_guid_and_kind](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/named_property_family_summary_groups_guid_and_kind.md)