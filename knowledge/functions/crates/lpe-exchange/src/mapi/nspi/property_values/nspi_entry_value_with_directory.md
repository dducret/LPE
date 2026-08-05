---
type: Rust Function
title: nspi_entry_value_with_directory
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L447-L524
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_permanent_entry_id
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_alias
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_display_type
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_display_type_ex
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_instance_key
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_record_key
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_search_key
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_legacy_dn
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_distribution_list_members
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_resolved_entry_row
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_property_value_list
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_get_props_property_value_list
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value
---

# Signature

`pub(in crate::mapi) fn nspi_entry_value_with_directory<'a>( account_id: Uuid, entry: &'a ExchangeAddressBookEntry, property_tag: u32, directory_entries: &'a [ExchangeAddressBookEntry], ) -> NspiValue<'a>`

# Calls

- [nspi_entry_permanent_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_permanent_entry_id.md)
- [nspi_entry_unprefixed_legacy_dn](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn.md)
- [nspi_entry_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_alias.md)
- [nspi_entry_display_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_display_type.md)
- [nspi_entry_display_type_ex](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_display_type_ex.md)
- [nspi_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id.md)
- [nspi_entry_instance_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_instance_key.md)
- [nspi_entry_record_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_record_key.md)
- [nspi_entry_search_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_search_key.md)
- [nspi_entry_legacy_dn](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_legacy_dn.md)
- [nspi_distribution_list_members](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_distribution_list_members.md)

# Called by

- [nspi_resolved_entry_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_resolved_entry_row.md)
- [nspi_entry_property_value_list](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_property_value_list.md)
- [nspi_get_props_property_value_list](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_get_props_property_value_list.md)
- [nspi_entry_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value.md)