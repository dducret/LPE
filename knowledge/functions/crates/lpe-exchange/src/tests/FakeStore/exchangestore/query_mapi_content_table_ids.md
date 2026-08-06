---
type: Rust Method
title: query_mapi_content_table_ids
resource: crates/lpe-exchange/src/tests/mod.rs#L11063-L11139
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/test_client_submit_time
  - functions/crates/lpe-exchange/src/tests/display_to_for_test
  - functions/crates/lpe-exchange/src/tests/test_message_flags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
---

# Signature

`fn query_mapi_content_table_ids<'a>( &'a self, _account_id: Uuid, query: MapiContentTableQuery, ) -> StoreFuture<'a, MapiContentTableQueryResult>`

# Calls

- [test_client_submit_time](../../../../../../../functions/crates/lpe-exchange/src/tests/test_client_submit_time.md)
- [display_to_for_test](../../../../../../../functions/crates/lpe-exchange/src/tests/display_to_for_test.md)
- [test_message_flags](../../../../../../../functions/crates/lpe-exchange/src/tests/test_message_flags.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)