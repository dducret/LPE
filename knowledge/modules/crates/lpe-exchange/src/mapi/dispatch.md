---
type: Rust Module
title: dispatch
resource: crates/lpe-exchange/src/mapi/dispatch.rs#L1-L1618
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-notifications
  - external/super-permissions
  - external/super-properties
  - external/super-rop
  - external/super-session
  - external/super-store-adapter
  - external/super-sync
  - external/super-tables
  - external/super-transport
  - external/super-wire-mapipropertytype-mapisynctype-ropid
  - external/super
  - external/crate-mapi-identity-conversation-members-contents-table-id-quick-step-settings-folder-id
  - external/crate-mapi-outlook-startup-normal-inbox-visible-row-missing-reason-normal-inbox-visible-row-release-request-shape-outlook-startup-gate-summary
  - external/crate-store-commitmapiassociatedconfigimportinput-commitmapinavigationshortcutcreateinput-commitmapinavigationshortcutimportinput-mapicustompropertyobjectkind-mapicustompropertyvalue-mapieventcreateoutcome-mapifaiimportdisposition-mapifaiimportedidentity-mapifolderhierarchycommitoutcome-mapiidentityobjectkind-mapispecialfolderalias-mapisyncchangeset-mapisynccheckpoint-upsertmapiassociatedconfiginput-upsertmapinavigationshortcutinput
  - external/lpe-core-outlook-trace-write-outlook-trace-outlooktracedirection-outlooktraceevent
  - external/lpe-domain-current-windows-filetime
  - external/lpe-storage-auditentryinput-createpublicfolderinput-jmapemail-jmapmailbox-jmapmailboxcreateinput-jmapmailboxupdateinput-mapicontactcreateinput-mapicontactcustompropertyvalue-mapieventattachmentchanges-mapieventattachmentupsert-mapieventcommitinput-mapieventcommitoutcome-mapieventcreateinput-mapieventcustompropertyvalue-mapieventimportedidentity-mapieventimportedmoveidentity-mapieventreminderpatch-mapimessageimportedmoveidentity-publicfolderpermissioninput-searchfolderdefinition-submittedrecipientinput-updatepublicfolderinput-upsertpublicfolderiteminput
  - external/serde-json-json-value
  - external/sha2-digest-sha256
  - external/std-cmp-ordering
  - external/associated-config
  - external/attachments
  - external/calendar-move-copy
  - external/contact-save
  - external/contacts
  - external/conversation-actions
  - external/custom-properties
  - external/default-folders
  - external/pub-in-crate-mapi-use-diagnostics
  - external/event-save
  - external/event-transactions
  - external/pub-in-crate-mapi-use-execute
  - external/folder-create
  - external/folder-dispatch
  - external/folder-open
  - external/folders
  - external/local-replica-sync
  - external/logon
  - external/message-dispatch
  - external/message-move-copy
  - external/message-open
  - external/message-save
  - external/message-state
  - external/messages
  - external/named-properties
  - external/navigation-shortcut-save
  - external/notification-subscriptions
  - external/object-ids
  - external/permissions
  - external/properties
  - external/property-dispatch
  - external/property-mutations
  - external/property-tags
  - external/public-folders
  - external/recipients
  - external/recoverable-items
  - external/release
  - external/rules
  - external/search-folders
  - external/stream-dispatch
  - external/submission
  - external/sync-configure
  - external/sync-conflicts
  - external/sync-get-buffer
  - external/sync-import
  - external/sync-import-deletes
  - external/sync-import-hierarchy
  - external/sync-import-message
  - external/sync-import-message-move
  - external/sync-import-read-state
  - external/sync-transfer
  - external/sync-upload-state
  - external/table-controls
  - external/table-diagnostics
  - external/table-lifecycle
  - external/table-open
  - external/table-validation
  - external/tables
  - external/unsupported
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [execute_response](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [log_post_common_views_handoff_execute_response](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response.md)
- [execute_rops](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)

# Imports

- `super::notifications::*`
- `super::permissions::*`
- `super::properties::*`
- `super::rop::*`
- `super::session::*`
- `super::store_adapter::*`
- `super::sync::*`
- `super::tables::*`
- `super::transport::*`
- `super::wire::{MapiPropertyType, MapiSyncType, RopId}`
- `super::*`
- `crate::mapi::identity::{
    CONVERSATION_MEMBERS_CONTENTS_TABLE_ID, QUICK_STEP_SETTINGS_FOLDER_ID,
}`
- `crate::mapi::outlook_startup::{
    normal_inbox_visible_row_missing_reason, normal_inbox_visible_row_release_request_shape,
    outlook_startup_gate_summary,
}`
- `crate::store::{
    CommitMapiAssociatedConfigImportInput, CommitMapiNavigationShortcutCreateInput,
    CommitMapiNavigationShortcutImportInput, MapiCustomPropertyObjectKind, MapiCustomPropertyValue,
    MapiEventCreateOutcome, MapiFaiImportDisposition, MapiFaiImportedIdentity,
    MapiFolderHierarchyCommitOutcome, MapiIdentityObjectKind, MapiSpecialFolderAlias,
    MapiSyncChangeSet, MapiSyncCheckpoint, UpsertMapiAssociatedConfigInput,
    UpsertMapiNavigationShortcutInput,
}`
- `lpe_core::outlook_trace::{write_outlook_trace, OutlookTraceDirection, OutlookTraceEvent}`
- `lpe_domain::current_windows_filetime`
- `lpe_storage::{
    AuditEntryInput, CreatePublicFolderInput, JmapEmail, JmapMailbox, JmapMailboxCreateInput,
    JmapMailboxUpdateInput, MapiContactCreateInput, MapiContactCustomPropertyValue,
    MapiEventAttachmentChanges, MapiEventAttachmentUpsert, MapiEventCommitInput,
    MapiEventCommitOutcome, MapiEventCreateInput, MapiEventCustomPropertyValue,
    MapiEventImportedIdentity, MapiEventImportedMoveIdentity, MapiEventReminderPatch,
    MapiMessageImportedMoveIdentity, PublicFolderPermissionInput, SearchFolderDefinition,
    SubmittedRecipientInput, UpdatePublicFolderInput, UpsertPublicFolderItemInput,
}`
- `serde_json::{json, Value}`
- `sha2::{Digest, Sha256}`
- `std::cmp::Ordering`
- `associated_config::*`
- `attachments::*`
- `calendar_move_copy::*`
- `contact_save::*`
- `contacts::*`
- `conversation_actions::*`
- `custom_properties::*`
- `default_folders::*`
- `pub(in crate::mapi) use diagnostics::*`
- `event_save::*`
- `event_transactions::*`
- `pub(in crate::mapi) use execute::*`
- `folder_create::*`
- `folder_dispatch::*`
- `folder_open::*`
- `folders::*`
- `local_replica_sync::*`
- `logon::*`
- `message_dispatch::*`
- `message_move_copy::*`
- `message_open::*`
- `message_save::*`
- `message_state::*`
- `messages::*`
- `named_properties::*`
- `navigation_shortcut_save::*`
- `notification_subscriptions::*`
- `object_ids::*`
- `permissions::*`
- `properties::*`
- `property_dispatch::*`
- `property_mutations::*`
- `property_tags::*`
- `public_folders::*`
- `recipients::*`
- `recoverable_items::*`
- `release::*`
- `rules::*`
- `search_folders::*`
- `stream_dispatch::*`
- `submission::*`
- `sync_configure::*`
- `sync_conflicts::*`
- `sync_get_buffer::*`
- `sync_import::*`
- `sync_import_deletes::*`
- `sync_import_hierarchy::*`
- `sync_import_message::*`
- `sync_import_message_move::*`
- `sync_import_read_state::*`
- `sync_transfer::*`
- `sync_upload_state::*`
- `table_controls::*`
- `table_diagnostics::*`
- `table_lifecycle::*`
- `table_open::*`
- `table_validation::*`
- `tables::*`
- `unsupported::*`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)