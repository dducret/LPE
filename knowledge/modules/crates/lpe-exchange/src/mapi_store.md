---
type: Rust Module
title: mapi_store
resource: crates/lpe-exchange/src/mapi_store.rs#L1-L1248
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/base64-engine-general-purpose-standard-as-base64-standard-engine-as
  - external/lpe-mail-auth-storefuture
  - external/lpe-storage-accessiblecontact-accessibleevent-activesyncattachment-calendareventattachment-clientnote-clientreminder-clienttask-collaborationcollection-conversationaction-delegatefreebusymessageobject-jmapemail-jmapmailbox-journalentry-mailboxrule-mapieventreminderstate-mapieventversion-publicfolder-publicfolderitem-publicfolderpermission-publicfolderreplica-recoverableitem-reminderquery-searchfolderdefinition
  - external/sha2-digest-sha256
  - external/std-collections-hashmap-hashset
  - external/uuid-uuid
  - external/crate-mapi-permissions-access-from-rights-reserved-permission-rows-rights-from-grant-mapifolderaccess-mapifolderpermission
  - external/crate-store-exchangestore
  - external/crate-store-mapiassociatedconfigrecord-mapinamedpropertymapping
  - external/crate-store-mapiidentityobjectkind-mapiidentityrecord-mapiidentityrequest-mapinavigationshortcutclientproperties-mapinavigationshortcutrecord
  - external/pub-crate-use-crate-store-mapifolderversion
  - external/pub-crate-use-folder-versions-mapi-folder-identity-requests
  - external/folder-versions-mapifolderversions
  - external/associated-config
  - external/pub-crate-use-associated-config-copy-associated-config-server-metadata-is-associated-config-read-only-property-tag-is-associated-config-server-owned-property-tag-is-outlook-common-views-default-navigation-shortcut-id-is-outlook-configuration-message-class-is-outlook-configuration-message-class-name-is-outlook-default-conversation-action-id-is-outlook-inbox-default-associated-config-id-is-outlook-inbox-virtual-only-associated-config-id-is-outlook-local-freebusy-message-id-is-outlook-umolk-user-options-message-class-modeled-virtual-associated-config-message-for-canonical-id-outlook-default-folder-named-view-id-outlook-default-folder-named-view-name-outlook-inbox-exact-virtual-associated-config-for-message-class-outlook-common-views-compact-named-view-id-outlook-inbox-rule-organizer-config-class-outlook-local-freebusy-message-id-outlook-quick-step-custom-action-class
  - external/pub-crate-use-associated-config-outlook-common-views-sent-to-named-view-id-outlook-default-folder-named-view-id
  - external/pub-crate-use-associated-config-outlook-inbox-compact-view-config-class-outlook-inbox-compact-view-config-id
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [MapiMailStoreSnapshot](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiMailStoreSnapshot.md)
- [MapiFolder](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiFolder.md)
- [MapiPublicFolder](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiPublicFolder.md)
- [MapiPublicFolderItem](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiPublicFolderItem.md)
- [MapiPublicFolderReplica](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiPublicFolderReplica.md)
- [MapiCollaborationFolder](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiCollaborationFolder.md)
- [MapiCollaborationFolderKind](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiCollaborationFolderKind.md)
- [MapiMessage](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiMessage.md)
- [MapiContact](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiContact.md)
- [MapiEvent](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiEvent.md)
- [MapiTask](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiTask.md)
- [MapiNote](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiNote.md)
- [MapiJournalEntry](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiJournalEntry.md)
- [MapiRule](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiRule.md)
- [MapiNavigationShortcutMessage](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiNavigationShortcutMessage.md)
- [MapiCommonViewNamedViewMessage](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiCommonViewNamedViewMessage.md)
- [MapiAssociatedConfigMessage](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiAssociatedConfigMessage.md)
- [MapiAssociatedConfigIdentity](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiAssociatedConfigIdentity.md)
- [MapiCommonViewsMessage](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiCommonViewsMessage.md)
- [MapiConversationActionMessage](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiConversationActionMessage.md)
- [MapiDelegateFreeBusyMessage](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiDelegateFreeBusyMessage.md)
- [MapiRecoverableItemMessage](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiRecoverableItemMessage.md)
- [MapiAttachment](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiAttachment.md)
- [MapiContentTableWindow](../../../../classes/crates/lpe-exchange/src/mapi_store/MapiContentTableWindow.md)
- [fixed_search_folder_role](../../../../functions/crates/lpe-exchange/src/mapi_store/fixed_search_folder_role.md)
- [associated_config_source_key](../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config_source_key.md)
- [hex_to_bytes](../../../../functions/crates/lpe-exchange/src/mapi_store/hex_to_bytes.md)
- [hex_digit](../../../../functions/crates/lpe-exchange/src/mapi_store/hex_digit.md)
- [mapi_search_folder_definition_to_folder](../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_search_folder_definition_to_folder.md)
- [user_saved_search_folder_projection_key](../../../../functions/crates/lpe-exchange/src/mapi_store/user_saved_search_folder_projection_key.md)
- [user_saved_search_folder_is_projectable](../../../../functions/crates/lpe-exchange/src/mapi_store/user_saved_search_folder_is_projectable.md)
- [mapi_search_folder_role](../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_search_folder_role.md)
- [MapiStore](../../../../interfaces/crates/lpe-exchange/src/mapi_store/MapiStore.md)
- [load_mapi_mail_store](../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [mapi_public_folder_permission](../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_public_folder_permission.md)
- [mapi_identity_requests](../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_identity_requests.md)
- [collaboration_folder_identity_requests](../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_requests.md)
- [is_virtual_special_mailbox](../../../../functions/crates/lpe-exchange/src/mapi_store/is_virtual_special_mailbox.md)
- [mapi_message_folder_id](../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_message_folder_id.md)
- [mapi_folder_id](../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_folder_id.md)
- [mapi_folder_id_for_role](../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_folder_id_for_role.md)
- [mapi_message_id](../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_message_id.md)
- [mapi_item_id](../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_item_id.md)
- [mapi_event_id_matches](../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_event_id_matches.md)
- [mapi_public_folder_id](../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_public_folder_id.md)
- [mapi_recoverable_item_id](../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_recoverable_item_id.md)
- [recoverable_mapi_folder_id](../../../../functions/crates/lpe-exchange/src/mapi_store/recoverable_mapi_folder_id.md)
- [recoverable_storage_folder](../../../../functions/crates/lpe-exchange/src/mapi_store/recoverable_storage_folder.md)
- [mapi_collaboration_folder_id](../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_collaboration_folder_id.md)
- [mapi_collaboration_folder_id_for_collection](../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_collaboration_folder_id_for_collection.md)
- [collaboration_folder_identity_canonical_id](../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id.md)
- [collaboration_folder_identity_canonical_id_for_collection](../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id_for_collection.md)
- [deterministic_collaboration_folder_uuid](../../../../functions/crates/lpe-exchange/src/mapi_store/deterministic_collaboration_folder_uuid.md)
- [task_collection_matches](../../../../functions/crates/lpe-exchange/src/mapi_store/task_collection_matches.md)
- [reserved_folder_counter_for_role](../../../../functions/crates/lpe-exchange/src/mapi_store/reserved_folder_counter_for_role.md)
- [reserved_folder_id_for_role](../../../../functions/crates/lpe-exchange/src/mapi_store/reserved_folder_id_for_role.md)
- [normalize_navigation_shortcut_group_name](../../../../functions/crates/lpe-exchange/src/mapi_store/normalize_navigation_shortcut_group_name.md)
- [format_navigation_shortcut_debug_summary](../../../../functions/crates/lpe-exchange/src/mapi_store/format_navigation_shortcut_debug_summary.md)
- [format_common_views_table_shortcut_debug_summary](../../../../functions/crates/lpe-exchange/src/mapi_store/format_common_views_table_shortcut_debug_summary.md)
- [common_views_search_folder_definition_is_projectable](../../../../functions/crates/lpe-exchange/src/mapi_store/common_views_search_folder_definition_is_projectable.md)
- [search_folder_definition_blob_has_required_blocks](../../../../functions/crates/lpe-exchange/src/mapi_store/search_folder_definition_blob_has_required_blocks.md)
- [format_navigation_shortcut_debug_entry](../../../../functions/crates/lpe-exchange/src/mapi_store/format_navigation_shortcut_debug_entry.md)

# Imports

- `base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _}`
- `lpe_mail_auth::StoreFuture`
- `lpe_storage::{
    AccessibleContact, AccessibleEvent, ActiveSyncAttachment, CalendarEventAttachment, ClientNote,
    ClientReminder, ClientTask, CollaborationCollection, ConversationAction,
    DelegateFreeBusyMessageObject, JmapEmail, JmapMailbox, JournalEntry, MailboxRule,
    MapiEventReminderState, MapiEventVersion, PublicFolder, PublicFolderItem,
    PublicFolderPermission, PublicFolderReplica, RecoverableItem, ReminderQuery,
    SearchFolderDefinition,
}`
- `sha2::{Digest, Sha256}`
- `std::collections::{HashMap, HashSet}`
- `uuid::Uuid`
- `crate::mapi::permissions::{
    access_from_rights, reserved_permission_rows, rights_from_grant, MapiFolderAccess,
    MapiFolderPermission,
}`
- `crate::store::ExchangeStore`
- `crate::store::{MapiAssociatedConfigRecord, MapiNamedPropertyMapping}`
- `crate::store::{
    MapiIdentityObjectKind, MapiIdentityRecord, MapiIdentityRequest,
    MapiNavigationShortcutClientProperties, MapiNavigationShortcutRecord,
}`
- `pub(crate) use crate::store::MapiFolderVersion`
- `pub(crate) use folder_versions::mapi_folder_identity_requests`
- `folder_versions::MapiFolderVersions`
- `associated_config::*`
- `pub(crate) use associated_config::{
    copy_associated_config_server_metadata, is_associated_config_read_only_property_tag,
    is_associated_config_server_owned_property_tag,
    is_outlook_common_views_default_navigation_shortcut_id, is_outlook_configuration_message_class,
    is_outlook_configuration_message_class_name, is_outlook_default_conversation_action_id,
    is_outlook_inbox_default_associated_config_id,
    is_outlook_inbox_virtual_only_associated_config_id, is_outlook_local_freebusy_message_id,
    is_outlook_umolk_user_options_message_class,
    modeled_virtual_associated_config_message_for_canonical_id,
    outlook_default_folder_named_view_id, outlook_default_folder_named_view_name,
    outlook_inbox_exact_virtual_associated_config_for_message_class,
    OUTLOOK_COMMON_VIEWS_COMPACT_NAMED_VIEW_ID, OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS,
    OUTLOOK_LOCAL_FREEBUSY_MESSAGE_ID, OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS,
}`
- `pub(crate) use associated_config::{
    OUTLOOK_COMMON_VIEWS_SENT_TO_NAMED_VIEW_ID, OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
}`
- `pub(crate) use associated_config::{
    OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS, OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_ID,
}`

# Member of

- [lpe-exchange](../../../../packages/crates/lpe-exchange.md)