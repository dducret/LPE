---
type: Rust Function
title: builtin_search_criteria_to_rop_for_folder_id
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L848-L860
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_scope_folder_ids
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_role_for_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_get_search_criteria_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/builtin_search_criteria_fallback_covers_advertised_reminders_folder
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/builtin_search_criteria_fallback_covers_tracked_mail_processing_folder
---

# Signature

`pub(super) fn builtin_search_criteria_to_rop_for_folder_id( folder_id: u64, ) -> Option<(Vec<u8>, Vec<u64>, u32)>`

# Calls

- [builtin_search_scope_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_scope_folder_ids.md)
- [builtin_search_role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_role_for_folder_id.md)

# Called by

- [append_get_search_criteria_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_get_search_criteria_response.md)
- [builtin_search_criteria_fallback_covers_advertised_reminders_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/builtin_search_criteria_fallback_covers_advertised_reminders_folder.md)
- [builtin_search_criteria_fallback_covers_tracked_mail_processing_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/builtin_search_criteria_fallback_covers_tracked_mail_processing_folder.md)