---
type: Rust Function
title: log_mapi_requested_collaboration_resolution
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L1048-L1111
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
---

# Signature

`fn log_mapi_requested_collaboration_resolution( account_id: Uuid, plan: &MapiAccessPlan, snapshot_backed_contents: bool, identities: &[MapiIdentityLookupRecord], loaded_contact_ids: &[Uuid], loaded_event_ids: &[Uuid], loaded_task_ids: &[Uuid], loaded_note_ids: &[Uuid], loaded_journal_entry_ids: &[Uuid], requested_contact_identity_count: usize, loaded_contact_count: usize, requested_calendar_event_identity_count: usize, loaded_calendar_event_count: usize, requested_task_identity_count: usize, loaded_task_count: usize, requested_note_identity_count: usize, loaded_note_count: usize, requested_journal_entry_identity_count: usize, loaded_journal_entry_count: usize, )`

# Called by

- [load_mapi_store_for_access_plan](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)