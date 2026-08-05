---
type: Rust Function
title: bounded_search_criteria_from_rop
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L295-L384
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/search_criteria_restriction_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/search_criteria_folder_ids
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/search_criteria_flags
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/set_search_criteria_flags_are_valid
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/microsoft_oxcdata_reminders_restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_restriction_clauses
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/previous_mapi_bounded_restriction_json
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/previous_mapi_bounded_scope_json
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_set_search_criteria_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/blank_search_criteria_is_invalid
---

# Signature

`pub(super) fn bounded_search_criteria_from_rop( request: &RopRequest, search_folder_id: u64, previous_definition: Option<&SearchFolderDefinition>, mailboxes: &[JmapMailbox], ) -> Result<BoundedSearchCriteria, u32>`

# Calls

- [search_criteria_restriction_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/search_criteria_restriction_bytes.md)
- [parse_mapi_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction.md)
- [search_criteria_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/search_criteria_folder_ids.md)
- [search_criteria_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/search_criteria_flags.md)
- [set_search_criteria_flags_are_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/set_search_criteria_flags_are_valid.md)
- [microsoft_oxcdata_reminders_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/microsoft_oxcdata_reminders_restriction.md)
- [bounded_search_restriction_clauses](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_restriction_clauses.md)
- [previous_mapi_bounded_restriction_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/previous_mapi_bounded_restriction_json.md)
- [previous_mapi_bounded_scope_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/previous_mapi_bounded_scope_json.md)
- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id.md)

# Called by

- [append_set_search_criteria_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_set_search_criteria_response.md)
- [blank_search_criteria_is_invalid](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/blank_search_criteria_is_invalid.md)