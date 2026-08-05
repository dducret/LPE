---
type: Rust Module
title: service
resource: crates/lpe-activesync/src/service.rs#L1-L1471
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/axum-http-headermap-response-response
  - external/lpe-mail-auth-authenticate-account-accountprincipal
  - external/lpe-storage-activesyncitemstate-auditentryinput
  - external/serde-json-value
  - external/std-collections-hashmap
  - external/uuid-uuid
  - external/crate-constants-calendar-class-contacts-class-folder-sync-collection-id-mail-class-message-draft-input-from-application-data-merged-draft-input-protocol-activesynccommand-activesyncfoldertype-activesyncstatus-bodypreferencetype-response-sync-status-node-wbxml-response-snapshot-calendar-application-data-collection-window-size-contact-application-data-diff-collection-states-drafts-collection-email-application-data-mail-collection-parse-collection-mailbox-id-require-collection-id-require-sync-collections-bodypreference-store-activesyncstore-types-authenticatedprincipal-collectiondefinition-collectionstateentry-parsedactivesyncquery-snapshotchange-storedsyncstate-wbxml-decode-wbxml-encode-wbxml-wbxmlnode
  - external/crate-types-activesyncquery
  - external/application-data-mail-flag-update-parse-contact-input-parse-event-input
  - external/body-preferences-collection-body-preference-collection-deletes-as-moves-fetch-body-preference
  - external/mime-validation-validate-mime-attachments
  - external/provisioning-header-policy-key-policy-required-response
  - external/sync-helpers-completed-sync-state-decode-sync-state-has-client-commands-hierarchy-generation-hierarchy-generation-from-snapshot-pending-page-sync-collection-has-unsupported-command-sync-collection-status-node-value-to-wbxml
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [Pipe](../../../../interfaces/crates/lpe-activesync/src/service/Pipe.md)
- [pipe](../../../../functions/crates/lpe-activesync/src/service/Pipe/pipe.md)
- [ActiveSyncService](../../../../classes/crates/lpe-activesync/src/service/ActiveSyncService.md)
- [PolicyMode](../../../../classes/crates/lpe-activesync/src/service/PolicyMode.md)
- [new](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/new.md)
- [with_policy_enforcement](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/with_policy_enforcement.md)
- [from_env](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/from_env.md)
- [mailbox_accesses](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_accesses.md)
- [mailbox_access_for_account](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_access_for_account.md)
- [mailbox_access_for_from_address](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_access_for_from_address.md)
- [handle_request](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_request.md)
- [handle_parsed_request](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)
- [authenticate](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/authenticate.md)
- [handle_sync](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_sync.md)
- [sync_collection](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)
- [store_sync_state](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/store_sync_state.md)
- [load_requested_sync_state](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/load_requested_sync_state.md)
- [current_hierarchy_generation](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/current_hierarchy_generation.md)
- [device_hierarchy_is_current](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/device_hierarchy_is_current.md)
- [collection_state](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/collection_state.md)
- [fetch_all_mail_states](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_all_mail_states.md)
- [build_commands](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/build_commands.md)
- [fetch_collection_nodes](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_nodes.md)
- [pending_page_is_stable](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/pending_page_is_stable.md)
- [fetch_collection_states_by_ids](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_states_by_ids.md)
- [apply_mail_sync_commands](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_mail_sync_commands.md)
- [hard_delete_mail_command](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/hard_delete_mail_command.md)
- [trash_collection](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/trash_collection.md)
- [apply_draft_sync_commands](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_draft_sync_commands.md)
- [apply_contact_sync_commands](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_contact_sync_commands.md)
- [apply_calendar_sync_commands](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_calendar_sync_commands.md)
- [folder_collections](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/folder_collections.md)
- [resolve_collection](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/resolve_collection.md)
- [owned_mail_folder](../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/owned_mail_folder.md)
- [command_status_response](../../../../functions/crates/lpe-activesync/src/service/command_status_response.md)
- [search_status_response](../../../../functions/crates/lpe-activesync/src/service/search_status_response.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `axum::{http::HeaderMap, response::Response}`
- `lpe_mail_auth::{authenticate_account, AccountPrincipal}`
- `lpe_storage::{ActiveSyncItemState, AuditEntryInput}`
- `serde_json::Value`
- `std::collections::HashMap`
- `uuid::Uuid`
- `crate::{
    constants::{CALENDAR_CLASS, CONTACTS_CLASS, FOLDER_SYNC_COLLECTION_ID, MAIL_CLASS},
    message::{draft_input_from_application_data, merged_draft_input},
    protocol::{ActiveSyncCommand, ActiveSyncFolderType, ActiveSyncStatus, BodyPreferenceType},
    response::{sync_status_node, wbxml_response},
    snapshot::{
        calendar_application_data, collection_window_size, contact_application_data,
        diff_collection_states, drafts_collection, email_application_data, mail_collection,
        parse_collection_mailbox_id, require_collection_id, require_sync_collections,
        BodyPreference,
    },
    store::ActiveSyncStore,
    types::{
        AuthenticatedPrincipal, CollectionDefinition, CollectionStateEntry, ParsedActiveSyncQuery,
        SnapshotChange, StoredSyncState,
    },
    wbxml::{decode_wbxml, encode_wbxml, WbxmlNode},
}`
- `crate::types::ActiveSyncQuery`
- `application_data::{mail_flag_update, parse_contact_input, parse_event_input}`
- `body_preferences::{
    collection_body_preference, collection_deletes_as_moves, fetch_body_preference,
}`
- `mime_validation::validate_mime_attachments`
- `provisioning::{header_policy_key, policy_required_response}`
- `sync_helpers::{
    completed_sync_state, decode_sync_state, has_client_commands, hierarchy_generation,
    hierarchy_generation_from_snapshot, pending_page, sync_collection_has_unsupported_command,
    sync_collection_status_node, value_to_wbxml,
}`

# Member of

- [lpe-activesync](../../../../packages/crates/lpe-activesync.md)