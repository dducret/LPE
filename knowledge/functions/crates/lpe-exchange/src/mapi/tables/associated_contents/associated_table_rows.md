---
type: Rust Function
title: associated_table_rows
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L173-L186
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows_with_lookup_restriction
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/suggested_contacts_associated_table_does_not_expose_folder_default_named_view
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_table_exposes_folder_local_default_named_view_for_exact_lookup
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_table_exposes_folder_local_default_named_view_without_restriction
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_exact_named_view_find_row_respects_existing_table_restriction
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_greater_than_restriction_uses_normal_property_semantics
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_content_restriction_projects_persisted_configs
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_restriction_does_not_add_a_modeled_startup_class
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_open_count_includes_unrestricted_persisted_configuration_rows
  - functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_associated_query_rows_do_not_inject_synthetic_default_named_view
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_default_columns_cover_required_configuration_contract
---

# Signature

`pub(super) fn associated_table_rows( folder_id: u64, snapshot: &MapiMailStoreSnapshot, restriction: Option<&MapiRestriction>, mailbox_guid: Uuid, ) -> Vec<AssociatedTableRow>`

# Calls

- [associated_table_rows_with_lookup_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows_with_lookup_restriction.md)

# Called by

- [associated_folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count.md)
- [restricted_associated_folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count.md)
- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [suggested_contacts_associated_table_does_not_expose_folder_default_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/suggested_contacts_associated_table_does_not_expose_folder_default_named_view.md)
- [inbox_associated_table_exposes_folder_local_default_named_view_for_exact_lookup](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_table_exposes_folder_local_default_named_view_for_exact_lookup.md)
- [inbox_associated_table_exposes_folder_local_default_named_view_without_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_table_exposes_folder_local_default_named_view_without_restriction.md)
- [inbox_associated_exact_named_view_find_row_respects_existing_table_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_exact_named_view_find_row_respects_existing_table_restriction.md)
- [inbox_associated_greater_than_restriction_uses_normal_property_semantics](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_greater_than_restriction_uses_normal_property_semantics.md)
- [inbox_associated_content_restriction_projects_persisted_configs](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_content_restriction_projects_persisted_configs.md)
- [inbox_associated_restriction_does_not_add_a_modeled_startup_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_restriction_does_not_add_a_modeled_startup_class.md)
- [inbox_associated_open_count_includes_unrestricted_persisted_configuration_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_open_count_includes_unrestricted_persisted_configuration_rows.md)
- [calendar_associated_query_rows_do_not_inject_synthetic_default_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_associated_query_rows_do_not_inject_synthetic_default_named_view.md)
- [inbox_associated_query_rows_default_columns_cover_required_configuration_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_default_columns_cover_required_configuration_contract.md)