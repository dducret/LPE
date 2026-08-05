---
type: Rust Function
title: log_mapi_store_load_summary
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L1298-L1354
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
---

# Signature

`fn log_mapi_store_load_summary( account_id: Uuid, plan: &MapiAccessPlan, snapshot_backed_contents: bool, mailbox_count: usize, email_count: usize, attachment_set_count: usize, contact_collection_count: usize, calendar_collection_count: usize, task_collection_count: usize, contact_count: usize, calendar_event_count: usize, task_count: usize, note_count: usize, journal_entry_count: usize, search_folder_count: usize, search_folder_definitions: &[lpe_storage::SearchFolderDefinition], conversation_action_count: usize, reminder_count: usize, folder_permission_count: usize, content_window_count: usize, requested_calendar_event_identity_count: usize, default_calendar_collection_loaded: bool, loaded_default_calendar_event_count: usize, )`

# Called by

- [load_mapi_store_for_access_plan](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)