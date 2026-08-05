---
type: Rust Function
title: nspi_special_table_row
resource: crates/lpe-exchange/src/mapi/nspi/special_tables.rs#L158-L194
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_tagged_property_value
  - functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_container_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/special_tables/hierarchy_rows_begin_with_address_book_property_value_count
---

# Signature

`fn nspi_special_table_row(container: &NspiSpecialTableContainer) -> Vec<u8>`

# Calls

- [write_address_book_tagged_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_tagged_property_value.md)
- [nspi_container_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_container_entry_id.md)

# Called by

- [hierarchy_rows_begin_with_address_book_property_value_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/special_tables/hierarchy_rows_begin_with_address_book_property_value_count.md)