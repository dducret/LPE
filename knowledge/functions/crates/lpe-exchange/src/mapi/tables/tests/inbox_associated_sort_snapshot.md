---
type: Rust Function
title: inbox_associated_sort_snapshot
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L8587-L8608
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_invariant_uses_mailbox_guid_entry_id
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_exact_configuration_find_row_uses_sort_order
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_broad_configuration_find_row_projects_single_followup_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_restriction_does_not_add_a_modeled_startup_class
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_uses_sort_order
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_open_count_includes_unrestricted_persisted_configuration_rows
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_uses_standard_property_rows_for_complete_rows
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_default_columns_cover_required_configuration_contract
---

# Signature

`fn inbox_associated_sort_snapshot() -> MapiMailStoreSnapshot`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [with_associated_configs](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs.md)

# Called by

- [inbox_associated_invariant_uses_mailbox_guid_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_invariant_uses_mailbox_guid_entry_id.md)
- [inbox_associated_exact_configuration_find_row_uses_sort_order](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_exact_configuration_find_row_uses_sort_order.md)
- [inbox_associated_broad_configuration_find_row_projects_single_followup_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_broad_configuration_find_row_projects_single_followup_row.md)
- [inbox_associated_restriction_does_not_add_a_modeled_startup_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_restriction_does_not_add_a_modeled_startup_class.md)
- [inbox_associated_query_rows_uses_sort_order](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_uses_sort_order.md)
- [inbox_associated_open_count_includes_unrestricted_persisted_configuration_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_open_count_includes_unrestricted_persisted_configuration_rows.md)
- [inbox_associated_query_rows_uses_standard_property_rows_for_complete_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_uses_standard_property_rows_for_complete_rows.md)
- [inbox_associated_query_rows_default_columns_cover_required_configuration_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_default_columns_cover_required_configuration_contract.md)