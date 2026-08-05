---
type: Rust Function
title: sort_mapi_messages
resource: crates/lpe-exchange/src/mapi/tables/sorting.rs#L82-L131
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_name
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_address
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/display_to
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/client_submit_sort_key
  - functions/crates/lpe-exchange/src/mapi/tables/flags/message_flags
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
---

# Signature

`pub(in crate::mapi) fn sort_mapi_messages( rows: &mut [&crate::mapi_store::MapiMessage], sort_orders: &[MapiSortOrder], )`

# Calls

- [compare_case_insensitive](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive.md)
- [email_sender_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_name.md)
- [email_sender_address](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_address.md)
- [display_to](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/display_to.md)
- [client_submit_sort_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/client_submit_sort_key.md)
- [message_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/flags/message_flags.md)
- [apply_sort_direction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)