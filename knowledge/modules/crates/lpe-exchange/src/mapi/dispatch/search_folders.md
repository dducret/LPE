---
type: Rust Module
title: search_folders
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L1-L1013
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/lpe-storage-upsertsearchfolderinput
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [BoundedSearchCriteria](../../../../../../classes/crates/lpe-exchange/src/mapi/dispatch/search_folders/BoundedSearchCriteria.md)
- [is_search_criteria_rop](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/is_search_criteria_rop.md)
- [append_search_criteria_dispatch_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_search_criteria_dispatch_response.md)
- [search_folder_handle_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/search_folder_handle_properties.md)
- [append_set_search_criteria_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_set_search_criteria_response.md)
- [append_get_search_criteria_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_get_search_criteria_response.md)
- [bounded_search_criteria_from_rop](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop.md)
- [previous_mapi_bounded_restriction_json](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/previous_mapi_bounded_restriction_json.md)
- [previous_mapi_bounded_scope_json](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/previous_mapi_bounded_scope_json.md)
- [set_search_criteria_flags_are_valid](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/set_search_criteria_flags_are_valid.md)
- [bounded_search_restriction_clauses](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_restriction_clauses.md)
- [microsoft_oxcdata_reminders_restriction](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/microsoft_oxcdata_reminders_restriction.md)
- [microsoft_oxcdata_excluded_parent_folders_restriction](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/microsoft_oxcdata_excluded_parent_folders_restriction.md)
- [microsoft_oxcdata_reminder_core_restriction](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/microsoft_oxcdata_reminder_core_restriction.md)
- [microsoft_oxcdata_not_schedule_message_class](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/microsoft_oxcdata_not_schedule_message_class.md)
- [microsoft_oxcdata_reminder_or_recurring](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/microsoft_oxcdata_reminder_or_recurring.md)
- [microsoft_oxcdata_reminder_set_true_property](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/microsoft_oxcdata_reminder_set_true_property.md)
- [microsoft_oxcdata_recurring_exists_and_true](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/microsoft_oxcdata_recurring_exists_and_true.md)
- [bounded_search_content_clause](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_content_clause.md)
- [bounded_search_not_clause](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_not_clause.md)
- [bounded_search_property_clause](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_property_clause.md)
- [bounded_search_criteria_to_rop](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_to_rop.md)
- [is_message_class_exclusion_clause](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/is_message_class_exclusion_clause.md)
- [and_restriction](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/and_restriction.md)
- [builtin_search_criteria_to_rop](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_criteria_to_rop.md)
- [builtin_search_scope_folder_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_scope_folder_ids.md)
- [builtin_search_role_for_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_role_for_folder_id.md)
- [builtin_search_criteria_to_rop_for_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_criteria_to_rop_for_folder_id.md)
- [rop_restriction_from_json_clause](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_restriction_from_json_clause.md)
- [rop_content_restriction](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_content_restriction.md)
- [rop_not_content_restriction](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_not_content_restriction.md)
- [rop_property_restriction](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_property_restriction.md)
- [property_tag_for_search_field](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/property_tag_for_search_field.md)
- [string_search_property_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/string_search_property_tag.md)
- [multiple_string_search_property_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/multiple_string_search_property_tag.md)
- [folder_id_for_role](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/folder_id_for_role.md)

# Imports

- `super::*`
- `lpe_storage::UpsertSearchFolderInput`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)