---
type: Rust Function
title: normal_message_table_column_is_backed
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1131-L1255
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/property_ids_match
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/normal_message_defaulted_column_detail
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/associated_contents_table_column_is_backed
---

# Signature

`fn normal_message_table_column_is_backed(storage_tag: u32) -> bool`

# Calls

- [property_ids_match](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/property_ids_match.md)

# Called by

- [normal_message_defaulted_column_detail](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/normal_message_defaulted_column_detail.md)
- [associated_contents_table_column_is_backed](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/associated_contents_table_column_is_backed.md)