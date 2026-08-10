---
type: Rust Function
title: common_views_sort_snapshot
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L8776-L8822
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi/properties/default_wlink_group_uuid
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_navigation_shortcuts
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_query_rows_uses_account_bound_wlink_entry_ids
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_wlink_query_rows_do_not_add_named_views_without_restriction
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_restricted_named_view_query_rows_are_empty_without_persisted_fai
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_query_rows_uses_wlink_sort_order
---

# Signature

`fn common_views_sort_snapshot(account_id: Uuid) -> MapiMailStoreSnapshot`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [default_wlink_group_uuid](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_wlink_group_uuid.md)
- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [with_navigation_shortcuts](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_navigation_shortcuts.md)

# Called by

- [common_views_query_rows_uses_account_bound_wlink_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_query_rows_uses_account_bound_wlink_entry_ids.md)
- [common_views_wlink_query_rows_do_not_add_named_views_without_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_wlink_query_rows_do_not_add_named_views_without_restriction.md)
- [common_views_restricted_named_view_query_rows_are_empty_without_persisted_fai](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_restricted_named_view_query_rows_are_empty_without_persisted_fai.md)
- [common_views_query_rows_uses_wlink_sort_order](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_query_rows_uses_wlink_sort_order.md)