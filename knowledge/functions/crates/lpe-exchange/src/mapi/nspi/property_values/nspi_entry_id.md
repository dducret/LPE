---
type: Rust Function
title: nspi_entry_id
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L656-L660
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/mapped_nspi_object_id
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_minimal_id_from_object_id
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/legacy_nspi_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_match
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_instance_key
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_filter_explicit_table_entries
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_entry
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_ranked_matching_entries
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_get_props_debug
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/principal_minimal_entry_id
---

# Signature

`pub(in crate::mapi) fn nspi_entry_id(account_id: Uuid, entry: &ExchangeAddressBookEntry) -> u32`

# Calls

- [mapped_nspi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/mapped_nspi_object_id.md)
- [nspi_minimal_id_from_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_minimal_id_from_object_id.md)
- [legacy_nspi_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/legacy_nspi_entry_id.md)

# Called by

- [nspi_get_prop_list_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response.md)
- [nspi_dn_to_mid_match](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_match.md)
- [nspi_props_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)
- [nspi_matches_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response.md)
- [nspi_entry_instance_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_instance_key.md)
- [nspi_requested_entry](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry.md)
- [nspi_filter_explicit_table_entries](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_filter_explicit_table_entries.md)
- [nspi_match_entry](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_entry.md)
- [nspi_ranked_matching_entries](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_ranked_matching_entries.md)
- [log_nspi_get_props_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_get_props_debug.md)
- [nspi_entry_value_with_directory](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory.md)
- [principal_minimal_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/principal_minimal_entry_id.md)