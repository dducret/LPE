---
type: Rust Function
title: mapi_content_restriction
resource: crates/lpe-exchange/src/tests/mod.rs#L15719-L15727
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_hierarchy_find_row_default_calendar_entry_id_uses_account_guid
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_root_hierarchy_findrow_finds_ipm_subtree_by_display_name
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_contents_table_findrow_finds_restricted_item
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_findrow_rejects_invalid_microsoft_find_row_flags
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_restrict_filters_contents_table_rows
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_find_row_returns_matching_contents_row
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_microsoft_comment_restriction_wraps_find_row_predicate
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_microsoft_count_restriction_wraps_find_row_predicate
---

# Signature

`fn mapi_content_restriction(property_tag: u32, value: &str) -> Vec<u8>`

# Called by

- [mapi_over_http_hierarchy_find_row_default_calendar_entry_id_uses_account_guid](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_hierarchy_find_row_default_calendar_entry_id_uses_account_guid.md)
- [mapi_over_http_root_hierarchy_findrow_finds_ipm_subtree_by_display_name](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_root_hierarchy_findrow_finds_ipm_subtree_by_display_name.md)
- [mapi_over_http_public_folder_contents_table_findrow_finds_restricted_item](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_contents_table_findrow_finds_restricted_item.md)
- [mapi_over_http_findrow_rejects_invalid_microsoft_find_row_flags](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_findrow_rejects_invalid_microsoft_find_row_flags.md)
- [mapi_over_http_restrict_filters_contents_table_rows](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_restrict_filters_contents_table_rows.md)
- [mapi_over_http_find_row_returns_matching_contents_row](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_find_row_returns_matching_contents_row.md)
- [mapi_over_http_microsoft_comment_restriction_wraps_find_row_predicate](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_microsoft_comment_restriction_wraps_find_row_predicate.md)
- [mapi_over_http_microsoft_count_restriction_wraps_find_row_predicate](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_microsoft_count_restriction_wraps_find_row_predicate.md)