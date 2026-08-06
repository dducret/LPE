---
type: Rust Function
title: inbox_associated_extended_rule_snapshot
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L8717-L8739
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_can_return_a_persisted_extended_rule_message
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_includes_persisted_extended_rule_message
---

# Signature

`fn inbox_associated_extended_rule_snapshot() -> MapiMailStoreSnapshot`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [with_associated_configs](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs.md)

# Called by

- [inbox_associated_find_row_can_return_a_persisted_extended_rule_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_can_return_a_persisted_extended_rule_message.md)
- [inbox_associated_query_rows_includes_persisted_extended_rule_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_includes_persisted_extended_rule_message.md)