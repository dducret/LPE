---
type: Rust Function
title: restriction_matches_contact_in_folder
resource: crates/lpe-exchange/src/mapi/properties.rs#L327-L335
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_contact_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
---

# Signature

`pub(in crate::mapi) fn restriction_matches_contact_in_folder( restriction: Option<&MapiRestriction>, contact: &AccessibleContact, folder_id: u64, ) -> bool`

# Calls

- [restriction_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [contact_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value.md)

# Called by

- [format_contact_query_row_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_contact_query_row_summary.md)
- [rop_find_row_response](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [table_position_and_count](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [rop_query_rows_response_inner](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)