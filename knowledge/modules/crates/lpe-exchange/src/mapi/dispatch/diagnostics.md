---
type: Rust Module
title: diagnostics
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L1-L1518
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/axum-http-headermap
  - external/pub-in-crate-mapi-use-lpe-domain-crypto-hex-lower-as-bytes-to-hex
  - external/lpe-mail-auth-accountprincipal
  - external/std-collections-hashmap
  - external/uuid-uuid
  - external/crate-mapi-identity-archive-folder-id-calendar-folder-id-common-views-folder-id-conflicts-folder-id-contacts-folder-id-contacts-search-folder-id-conversation-action-settings-folder-id-conversation-history-folder-id-deferred-action-folder-id-document-libraries-folder-id-drafts-folder-id-freebusy-data-folder-id-im-contact-list-folder-id-inbox-folder-id-ipm-subtree-folder-id-journal-folder-id-junk-folder-id-local-failures-folder-id-notes-folder-id-outbox-folder-id-quick-contacts-folder-id-quick-step-settings-folder-id-reminders-folder-id-root-folder-id-rss-feeds-folder-id-schedule-folder-id-search-folder-id-sent-folder-id-server-failures-folder-id-shortcuts-folder-id-spooler-queue-folder-id-suggested-contacts-folder-id-sync-issues-folder-id-tasks-folder-id-todo-search-folder-id-tracked-mail-processing-folder-id-trash-folder-id-views-folder-id-object-id-from-folder-identifier-bytes-object-id-from-source-key-object-id-from-wire-id
  - external/crate-mapi-nspi-normalize-nspi-lookup-value-principal-legacy-dn-aliases
  - external/crate-mapi-properties-mapisortorder-mapivalue
  - external/crate-mapi-rop-cursor-roplogonrequest-is-rpc-header-ext-rop-buffer-private-logon-response-logon-flags-public-folder-logon-response-logon-flags-read-rop-request-rpc-header-ext-payload-split-rop-buffer
  - external/crate-mapi-session-read-handle-table
  - external/crate-mapi-session-mapiobject-mapisession
  - external/crate-mapi-store-adapter-mapiaccessplan
  - external/crate-mapi-sync-private-logon-special-folder-ids
  - external/crate-mapi-tables-role-for-folder-id
  - external/crate-mapi-transport-mapiendpoint-debug-payload-preview-hex-hex-preview-safe-header
  - external/crate-mapi-wire-ropid
  - external/super-max-rop-debug-entries
  - external/pub-super-use-associated-config
  - external/pub-super-use-calendar
  - external/pub-super-use-calendar-contract
  - external/pub-super-use-common-views
  - external/pub-super-use-default-folders
  - external/pub-super-use-execute
  - external/pub-super-use-fast-transfer
  - external/pub-super-use-message
  - external/pub-super-use-named-properties
  - external/pub-super-use-open-folder
  - external/pub-in-crate-mapi-use-post-hierarchy
  - external/pub-super-use-probes
  - external/pub-super-use-property-names
  - external/pub-super-use-property-responses
  - external/pub-super-use-recipients
  - external/pub-super-use-special-folders
  - external/pub-super-use-sync-upload
  - external/pub-super-use-table-queries
  - external/pub-super-use-values
  - external/super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [RopRequestDebugSummary](../../../../../../classes/crates/lpe-exchange/src/mapi/dispatch/diagnostics/RopRequestDebugSummary.md)
- [FirstPostHierarchyProbeDebugSummary](../../../../../../classes/crates/lpe-exchange/src/mapi/dispatch/diagnostics/FirstPostHierarchyProbeDebugSummary.md)
- [OpenFolderProbeRequest](../../../../../../classes/crates/lpe-exchange/src/mapi/dispatch/diagnostics/OpenFolderProbeRequest.md)
- [GetPropertiesSpecificProbeRequest](../../../../../../classes/crates/lpe-exchange/src/mapi/dispatch/diagnostics/GetPropertiesSpecificProbeRequest.md)
- [SetPropertiesProbeRequest](../../../../../../classes/crates/lpe-exchange/src/mapi/dispatch/diagnostics/SetPropertiesProbeRequest.md)
- [RopResponseDebugSummary](../../../../../../classes/crates/lpe-exchange/src/mapi/dispatch/diagnostics/RopResponseDebugSummary.md)
- [LogonResponseDebugSummary](../../../../../../classes/crates/lpe-exchange/src/mapi/dispatch/diagnostics/LogonResponseDebugSummary.md)
- [debug_role_for_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_role_for_folder_id.md)
- [debug_container_class_for_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_container_class_for_folder_id.md)
- [post_hierarchy_probe_folder_name](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy_probe_folder_name.md)
- [expected_special_folder_container_class](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/expected_special_folder_container_class.md)
- [rop_ids_csv](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_ids_csv.md)
- [rop_id_hex](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_id_hex.md)
- [rop_names_csv](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_names_csv.md)
- [rop_name](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_name.md)
- [rop_has_no_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_has_no_response.md)
- [summarize_non_release_request_rops](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_non_release_request_rops.md)
- [summarize_request_rop_raw_frames](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_raw_frames.md)
- [summarize_handle_table](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_handle_table.md)
- [summarize_request_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer.md)
- [summarize_response_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer.md)
- [summarize_response_rop_buffer_with_expected_handles](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_expected_handles.md)
- [summarize_response_rop_buffer_with_optional_expected_handles](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles.md)
- [response_rop_frame_end](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_rop_frame_end.md)
- [response_rop_fixed_frame_end](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_rop_fixed_frame_end.md)
- [next_response_rop_start_validated](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_validated.md)
- [next_response_rop_start_from](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_from.md)
- [response_handle_index_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_handle_index_matches.md)
- [next_response_rop_start](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start.md)
- [is_plausible_response_return_value](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/is_plausible_response_return_value.md)
- [rop_buffer_size_word](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_buffer_size_word.md)
- [rop_buffer_layout_name](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_buffer_layout_name.md)
- [summarize_logon_response_rop](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_logon_response_rop.md)
- [read_u64](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_u64.md)
- [read_guid_le](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_guid_le.md)
- [format_logon_special_folder_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_logon_special_folder_contract.md)
- [logon_special_folder_contract_issues](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/logon_special_folder_contract_issues.md)
- [logon_special_folder_contract_entries](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/logon_special_folder_contract_entries.md)
- [read_response_error_code](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code.md)
- [execute_response_framing_context](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute_response_framing_context.md)
- [summarize_response_rop_frame](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_frame.md)
- [execute_batch_has_same_save_getprops_not_found](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute_batch_has_same_save_getprops_not_found.md)
- [format_debug_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_property_tags.md)
- [format_debug_sort_orders](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_sort_orders.md)
- [format_expected_folder_id_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_expected_folder_id_for_debug.md)
- [log_rop_logon_request_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_rop_logon_request_identity.md)
- [logon_open_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/logon_open_flags.md)
- [logon_store_state](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/logon_store_state.md)
- [projected_logon_response_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/projected_logon_response_flags.md)
- [format_logon_request_shape](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_logon_request_shape.md)
- [formats_observed_outlook_logon_flags_0x09_path](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/formats_observed_outlook_logon_flags_0x09_path.md)
- [formats_initial_private_logon_open_flags_without_dropped_bits](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/formats_initial_private_logon_open_flags_without_dropped_bits.md)
- [decode_logon_identity_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/decode_logon_identity_bytes.md)
- [log_outlook_bootstrap_phase](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_outlook_bootstrap_phase.md)
- [log_outlook_bootstrap_row_invariant](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_outlook_bootstrap_row_invariant.md)
- [log_execute_request_start_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_execute_request_start_debug.md)
- [log_execute_store_access_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_execute_store_access_debug.md)
- [format_debug_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_object_ids.md)
- [format_optional_debug_handle](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_optional_debug_handle.md)
- [format_handle_lineage_context](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_handle_lineage_context.md)
- [mapi_object_debug_kind](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/mapi_object_debug_kind.md)
- [mapi_object_debug_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/mapi_object_debug_folder_id.md)
- [format_live_handle_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_live_handle_debug_summary.md)

# Imports

- `anyhow::Result`
- `axum::http::HeaderMap`
- `pub(in crate::mapi) use lpe_domain::crypto::hex_lower as bytes_to_hex`
- `lpe_mail_auth::AccountPrincipal`
- `std::collections::HashMap`
- `uuid::Uuid`
- `crate::mapi::identity::{
    ARCHIVE_FOLDER_ID, CALENDAR_FOLDER_ID, COMMON_VIEWS_FOLDER_ID, CONFLICTS_FOLDER_ID,
    CONTACTS_FOLDER_ID, CONTACTS_SEARCH_FOLDER_ID, CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    CONVERSATION_HISTORY_FOLDER_ID, DEFERRED_ACTION_FOLDER_ID, DOCUMENT_LIBRARIES_FOLDER_ID,
    DRAFTS_FOLDER_ID, FREEBUSY_DATA_FOLDER_ID, IM_CONTACT_LIST_FOLDER_ID, INBOX_FOLDER_ID,
    IPM_SUBTREE_FOLDER_ID, JOURNAL_FOLDER_ID, JUNK_FOLDER_ID, LOCAL_FAILURES_FOLDER_ID,
    NOTES_FOLDER_ID, OUTBOX_FOLDER_ID, QUICK_CONTACTS_FOLDER_ID, QUICK_STEP_SETTINGS_FOLDER_ID,
    REMINDERS_FOLDER_ID, ROOT_FOLDER_ID, RSS_FEEDS_FOLDER_ID, SCHEDULE_FOLDER_ID, SEARCH_FOLDER_ID,
    SENT_FOLDER_ID, SERVER_FAILURES_FOLDER_ID, SHORTCUTS_FOLDER_ID, SPOOLER_QUEUE_FOLDER_ID,
    SUGGESTED_CONTACTS_FOLDER_ID, SYNC_ISSUES_FOLDER_ID, TASKS_FOLDER_ID, TODO_SEARCH_FOLDER_ID,
    TRACKED_MAIL_PROCESSING_FOLDER_ID, TRASH_FOLDER_ID, VIEWS_FOLDER_ID,
    object_id_from_folder_identifier_bytes, object_id_from_source_key, object_id_from_wire_id,
}`
- `crate::mapi::nspi::{normalize_nspi_lookup_value, principal_legacy_dn_aliases}`
- `crate::mapi::properties::{MapiSortOrder, MapiValue}`
- `crate::mapi::rop::{
    Cursor, RopLogonRequest, is_rpc_header_ext_rop_buffer, private_logon_response_logon_flags,
    public_folder_logon_response_logon_flags, read_rop_request, rpc_header_ext_payload,
    split_rop_buffer,
}`
- `crate::mapi::session::read_handle_table`
- `crate::mapi::session::{MapiObject, MapiSession}`
- `crate::mapi::store_adapter::MapiAccessPlan`
- `crate::mapi::sync::PRIVATE_LOGON_SPECIAL_FOLDER_IDS`
- `crate::mapi::tables::role_for_folder_id`
- `crate::mapi::transport::{MapiEndpoint, debug_payload_preview_hex, hex_preview, safe_header}`
- `crate::mapi::wire::RopId`
- `super::MAX_ROP_DEBUG_ENTRIES`
- `pub(super) use associated_config::*`
- `pub(super) use calendar::*`
- `pub(super) use calendar_contract::*`
- `pub(super) use common_views::*`
- `pub(super) use default_folders::*`
- `pub(super) use execute::*`
- `pub(super) use fast_transfer::*`
- `pub(super) use message::*`
- `pub(super) use named_properties::*`
- `pub(super) use open_folder::*`
- `pub(in crate::mapi) use post_hierarchy::*`
- `pub(super) use probes::*`
- `pub(super) use property_names::*`
- `pub(super) use property_responses::*`
- `pub(super) use recipients::*`
- `pub(super) use special_folders::*`
- `pub(super) use sync_upload::*`
- `pub(super) use table_queries::*`
- `pub(super) use values::*`
- `super::*`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)