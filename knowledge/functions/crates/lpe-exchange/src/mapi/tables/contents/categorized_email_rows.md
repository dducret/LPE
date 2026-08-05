---
type: Rust Function
title: categorized_email_rows
resource: crates/lpe-exchange/src/mapi/tables/contents.rs#L380-L478
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/message_for_canonical_id
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_mapi_message_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
  - functions/crates/lpe-exchange/src/mapi/tables/contents/category_values_for_email
  - functions/crates/lpe-exchange/src/mapi/tables/contents/category_id_for_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_category_header_row
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_categorized_message_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/expanded_categorized_rows
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_keywords_project_multivalue_instances_and_table_row_metadata
  - functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_and_deleted_message_rows_keep_long_term_entry_ids
---

# Signature

`pub(super) fn categorized_email_rows( snapshot: Option<&MapiMailStoreSnapshot>, mailbox_guid: Uuid, folder_id: u64, emails: Vec<&JmapEmail>, columns: &[u32], sort_orders: &[MapiSortOrder], expanded_count: u16, collapsed_categories: &HashSet<u64>, ) -> Vec<CategorizedTableRow>`

# Calls

- [message_for_canonical_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/message_for_canonical_id.md)
- [serialize_mapi_message_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_mapi_message_row_with_mailbox_guid.md)
- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)
- [category_values_for_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/category_values_for_email.md)
- [category_id_for_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/category_id_for_value.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [apply_sort_direction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction.md)
- [compare_case_insensitive](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive.md)
- [serialize_category_header_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_category_header_row.md)
- [serialize_categorized_message_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_categorized_message_row.md)

# Called by

- [expanded_categorized_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/expanded_categorized_rows.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [categorized_keywords_project_multivalue_instances_and_table_row_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_keywords_project_multivalue_instances_and_table_row_metadata.md)
- [categorized_and_deleted_message_rows_keep_long_term_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_and_deleted_message_rows_keep_long_term_entry_ids.md)