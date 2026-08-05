---
type: Rust Module
title: store_adapter
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L1-L1377
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-properties
  - external/super-rop
  - external/super-session
  - external/super-sync-calendar-folder-id-common-views-folder-id-contacts-folder-id-contacts-search-folder-id-im-contact-list-folder-id-inbox-folder-id-journal-folder-id-notes-folder-id-quick-contacts-folder-id-reminders-folder-id-root-folder-id-sent-folder-id-suggested-contacts-folder-id-tasks-folder-id-todo-search-folder-id-tracked-mail-processing-folder-id-trash-folder-id
  - external/super-tables
  - external/super
  - external/crate-mapi-store
  - external/crate-store-mapicontenttablequery-mapicontenttablesort-mapicontenttablesortfield-mapiidentitylookuprecord-mapiidentityrecord-mapiidentityrequest
  - external/anyhow-context
  - external/lpe-storage-reminderquery
  - external/pub-in-crate-mapi-use-access-plan
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [MapiIdentityScope](../../../../../classes/crates/lpe-exchange/src/mapi/store_adapter/MapiIdentityScope.md)
- [request_identity_scope](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/MapiIdentityScope/request_identity_scope.md)
- [load_mapi_identity_scope](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope.md)
- [load_mapi_identity_codec_for_test](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_codec_for_test.md)
- [load_mapi_store_for_access_plan](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [finalize_mapi_store_snapshot](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/finalize_mapi_store_snapshot.md)
- [calendar_event_versions_required](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/calendar_event_versions_required.md)
- [requested_identity_has_backing_row](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/requested_identity_has_backing_row.md)
- [log_mapi_store_load_step](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_store_load_step.md)
- [log_mapi_store_full_snapshot](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_store_full_snapshot.md)
- [log_mapi_requested_identity_resolution](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_requested_identity_resolution.md)
- [log_mapi_requested_collaboration_resolution](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_requested_collaboration_resolution.md)
- [format_missing_mapi_identities](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/format_missing_mapi_identities.md)
- [deduplicate_mapi_identity_requests](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/deduplicate_mapi_identity_requests.md)
- [log_mapi_identity_request_summary](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_identity_request_summary.md)
- [format_mapi_object_ids](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/format_mapi_object_ids.md)
- [format_unresolved_mapi_object_scopes](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/format_unresolved_mapi_object_scopes.md)
- [unresolved_mapi_object_scope](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/unresolved_mapi_object_scope.md)
- [is_expected_unbacked_mapi_object](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/is_expected_unbacked_mapi_object.md)
- [format_mapi_identity_kinds](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/format_mapi_identity_kinds.md)
- [mapi_identity_kind_name](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/mapi_identity_kind_name.md)
- [log_mapi_store_load_summary](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_store_load_summary.md)
- [format_search_folder_roles](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/format_search_folder_roles.md)

# Imports

- `super::properties::*`
- `super::rop::*`
- `super::session::*`
- `super::sync::{
    CALENDAR_FOLDER_ID, COMMON_VIEWS_FOLDER_ID, CONTACTS_FOLDER_ID, CONTACTS_SEARCH_FOLDER_ID,
    IM_CONTACT_LIST_FOLDER_ID, INBOX_FOLDER_ID, JOURNAL_FOLDER_ID, NOTES_FOLDER_ID,
    QUICK_CONTACTS_FOLDER_ID, REMINDERS_FOLDER_ID, ROOT_FOLDER_ID, SENT_FOLDER_ID,
    SUGGESTED_CONTACTS_FOLDER_ID, TASKS_FOLDER_ID, TODO_SEARCH_FOLDER_ID,
    TRACKED_MAIL_PROCESSING_FOLDER_ID, TRASH_FOLDER_ID,
}`
- `super::tables::*`
- `super::*`
- `crate::mapi_store`
- `crate::store::{
    MapiContentTableQuery, MapiContentTableSort, MapiContentTableSortField,
    MapiIdentityLookupRecord, MapiIdentityRecord, MapiIdentityRequest,
}`
- `anyhow::Context`
- `lpe_storage::ReminderQuery`
- `pub(in crate::mapi) use access_plan::*`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)