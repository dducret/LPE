---
type: Rust Function
title: nspi_container_entry_id
resource: crates/lpe-exchange/src/mapi/nspi/special_tables.rs#L196-L205
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_special_table_row
  - functions/crates/lpe-exchange/src/mapi/nspi/special_tables/hierarchy_permanent_entry_ids_use_address_list_dn_forms
---

# Signature

`fn nspi_container_entry_id(dn: &str) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [nspi_special_table_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_special_table_row.md)
- [hierarchy_permanent_entry_ids_use_address_list_dn_forms](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/special_tables/hierarchy_permanent_entry_ids_use_address_list_dn_forms.md)