use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use lpe_mail_auth::StoreFuture;
use lpe_storage::{
    AccessibleContact, AccessibleEvent, ActiveSyncAttachment, CalendarEventAttachment, ClientNote,
    ClientReminder, ClientTask, CollaborationCollection, ConversationAction,
    DelegateFreeBusyMessageObject, JmapEmail, JmapMailbox, JournalEntry, MailboxRule, PublicFolder,
    PublicFolderItem, PublicFolderPermission, PublicFolderReplica, RecoverableItem, ReminderQuery,
    SearchFolderDefinition,
};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use uuid::Uuid;

use crate::mapi::permissions::{
    access_from_rights, reserved_permission_rows, rights_from_grant, MapiFolderAccess,
    MapiFolderPermission,
};
use crate::store::ExchangeStore;
use crate::store::MapiAssociatedConfigRecord;
use crate::store::{
    MapiIdentityObjectKind, MapiIdentityRequest, MapiNavigationShortcutRecord,
    UpsertMapiAssociatedConfigInput,
};

#[derive(Debug, Clone)]
pub(crate) struct MapiMailStoreSnapshot {
    folders: Vec<MapiFolder>,
    public_folders: Vec<MapiPublicFolder>,
    public_folder_items: Vec<MapiPublicFolderItem>,
    public_folder_replicas: Vec<MapiPublicFolderReplica>,
    collaboration_folders: Vec<MapiCollaborationFolder>,
    messages: Vec<MapiMessage>,
    contacts: Vec<MapiContact>,
    events: Vec<MapiEvent>,
    tasks: Vec<MapiTask>,
    notes: Vec<MapiNote>,
    journal_entries: Vec<MapiJournalEntry>,
    search_folder_definitions: Vec<SearchFolderDefinition>,
    rules: Vec<MapiRule>,
    navigation_shortcuts: Vec<MapiNavigationShortcutMessage>,
    associated_configs: Vec<MapiAssociatedConfigMessage>,
    associated_config_identity_ids: Vec<MapiAssociatedConfigIdentity>,
    conversation_actions: Vec<MapiConversationActionMessage>,
    delegate_freebusy_messages: Vec<MapiDelegateFreeBusyMessage>,
    recoverable_items: Vec<MapiRecoverableItemMessage>,
    reminders: Vec<ClientReminder>,
    folder_permissions: Vec<MapiFolderPermission>,
    public_folder_permissions: Vec<MapiFolderPermission>,
    content_windows: Vec<MapiContentTableWindow>,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiFolder {
    pub(crate) id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) mailbox: JmapMailbox,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiPublicFolder {
    pub(crate) id: u64,
    pub(crate) folder: PublicFolder,
    pub(crate) item_count: u32,
    pub(crate) child_count: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiPublicFolderItem {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) item: PublicFolderItem,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiPublicFolderReplica {
    pub(crate) folder_id: u64,
    pub(crate) replica: PublicFolderReplica,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiCollaborationFolder {
    pub(crate) id: u64,
    pub(crate) kind: MapiCollaborationFolderKind,
    pub(crate) collection: CollaborationCollection,
    pub(crate) item_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MapiCollaborationFolderKind {
    Contacts,
    Calendar,
    Task,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiMessage {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) email: JmapEmail,
    pub(crate) attachments: Vec<MapiAttachment>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiContact {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) contact: AccessibleContact,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiEvent {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) event: AccessibleEvent,
    pub(crate) attachments: Vec<MapiAttachment>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiTask {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) task: ClientTask,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiNote {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) note: ClientNote,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiJournalEntry {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) entry: JournalEntry,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiRule {
    pub(crate) id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) name: String,
    pub(crate) is_active: bool,
    pub(crate) condition_summary: String,
    pub(crate) action_summary: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiNavigationShortcutMessage {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) subject: String,
    pub(crate) target_folder_id: Option<u64>,
    pub(crate) shortcut_type: u32,
    pub(crate) flags: u32,
    pub(crate) save_stamp: u32,
    pub(crate) section: u32,
    pub(crate) ordinal: u32,
    pub(crate) group_header_id: Option<Uuid>,
    pub(crate) group_name: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiCommonViewNamedViewMessage {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) name: String,
    pub(crate) view_flags: u32,
    pub(crate) view_type: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct MapiAssociatedConfigMessage {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) message_class: String,
    pub(crate) subject: String,
    pub(crate) properties_json: serde_json::Value,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiAssociatedConfigIdentity {
    pub(crate) canonical_id: Uuid,
    pub(crate) object_id: u64,
}

fn deduplicate_associated_config_messages(
    messages: Vec<MapiAssociatedConfigMessage>,
) -> Vec<MapiAssociatedConfigMessage> {
    let mut seen = HashSet::new();
    messages
        .into_iter()
        .filter(|message| seen.insert((message.folder_id, message.message_class.clone())))
        .collect()
}

const OUTLOOK_INBOX_ACCOUNT_PREFS_CONFIG_CLASS: &str = "IPM.Configuration.AccountPrefs";
const OUTLOOK_INBOX_ACCOUNT_PREFS_CONFIG_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFFB);
const OUTLOOK_INBOX_EAS_CONFIG_CLASS: &str = "IPM.Configuration.EAS";
const OUTLOOK_INBOX_EAS_CONFIG_ID: u64 = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFFD);
const OUTLOOK_INBOX_ELC_CONFIG_CLASS: &str = "IPM.Configuration.ELC";
const OUTLOOK_INBOX_ELC_CONFIG_ID: u64 = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFFC);
const OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_CLASS: &str =
    "IPM.Configuration.MessageListSettings";
const OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF8);
const OUTLOOK_INBOX_MRM_CONFIG_CLASS: &str = "IPM.Configuration.MRM";
const OUTLOOK_INBOX_MRM_CONFIG_ID: u64 = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE1);
const OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS: &str = "IPM.Configuration.UMOLK.UserOptions";
const OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFFA);
pub(crate) const OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS: &str = "IPM.RuleOrganizer";
const OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFED);
const OUTLOOK_RULE_ORGANIZER_BINARY_6802_JSON_KEY: &str = "0x68020102";
pub(crate) const OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS: &str =
    "IPM.Microsoft.FolderDesign.NamedView";
pub(crate) const OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF6);
pub(crate) const OUTLOOK_COMMON_VIEWS_COMPACT_NAMED_VIEW_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF7);
pub(crate) const OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE9);
const OUTLOOK_COMMON_VIEWS_SENT_TO_NAMED_VIEW_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE8);
const OUTLOOK_COMMON_VIEWS_DEFAULT_MAIL_GROUP_HEADER_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE7);
const OUTLOOK_COMMON_VIEWS_DEFAULT_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF9);
const OUTLOOK_COMMON_VIEWS_DEFAULT_SENT_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE6);
const OUTLOOK_COMMON_VIEWS_DEFAULT_TRASH_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE5);
const OUTLOOK_MAIL_FAVORITES_GROUP_NAME: &str = "Favorites";
const OUTLOOK_INBOX_SHARING_CONFIGURATION_CLASS: &str = "IPM.Sharing.Configuration";
const OUTLOOK_INBOX_SHARING_CONFIGURATION_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF5);
const OUTLOOK_INBOX_SHARING_INDEX_CLASS: &str = "IPM.Sharing.Index";
const OUTLOOK_INBOX_SHARING_INDEX_ID: u64 = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF3);
const OUTLOOK_INBOX_AGGREGATION_CLASS: &str = "IPM.Aggregation";
const OUTLOOK_INBOX_AGGREGATION_ID: u64 = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE3);
pub(crate) const OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS: &str = "IPM.Microsoft.CustomAction";
const OUTLOOK_QUICK_STEP_CUSTOM_ACTION_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF4);
const OUTLOOK_CONTACT_SYNC_CONFIG_CLASS: &str = "IPM.Microsoft.OSC.ContactSync";
const OUTLOOK_CONTACTS_OSC_CONTACT_SYNC_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF1);
const OUTLOOK_SUGGESTED_CONTACTS_OSC_CONTACT_SYNC_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF0);
const OUTLOOK_QUICK_CONTACTS_OSC_CONTACT_SYNC_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFEF);
const OUTLOOK_IM_CONTACT_LIST_OSC_CONTACT_SYNC_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFEE);
const OUTLOOK_DYNAMIC_CONTACT_SYNC_CONFIG_COUNTER_BASE: u64 = 0x7FFF_FF00_0000;
const OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS: &str = "IPM.Microsoft.ContactLink.TimeStamp";
const OUTLOOK_CONTACTS_CONTACT_LINK_TIMESTAMP_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFEC);
const OUTLOOK_SUGGESTED_CONTACTS_CONTACT_LINK_TIMESTAMP_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFEB);
const OUTLOOK_QUICK_CONTACTS_CONTACT_LINK_TIMESTAMP_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFEA);
const OUTLOOK_IM_CONTACT_LIST_CONTACT_LINK_TIMESTAMP_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE2);
const OUTLOOK_DYNAMIC_CONTACT_LINK_TIMESTAMP_COUNTER_BASE: u64 = 0x7FFF_FE00_0000;
const OUTLOOK_DEFAULT_CONVERSATION_ACTION_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2);
const OUTLOOK_LOCAL_FREEBUSY_MESSAGE_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE4);

pub(crate) fn is_outlook_inbox_default_associated_config_id(item_id: u64) -> bool {
    matches!(
        item_id,
        OUTLOOK_INBOX_ACCOUNT_PREFS_CONFIG_ID
            | OUTLOOK_INBOX_EAS_CONFIG_ID
            | OUTLOOK_INBOX_ELC_CONFIG_ID
            | OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_ID
            | OUTLOOK_INBOX_MRM_CONFIG_ID
            | OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_ID
            | OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_ID
            | OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_ID
            | OUTLOOK_INBOX_SHARING_CONFIGURATION_ID
            | OUTLOOK_INBOX_SHARING_INDEX_ID
            | OUTLOOK_INBOX_AGGREGATION_ID
    )
}

pub(crate) fn is_outlook_inbox_virtual_only_associated_config_id(item_id: u64) -> bool {
    matches!(
        item_id,
        OUTLOOK_INBOX_EAS_CONFIG_ID
            | OUTLOOK_INBOX_ELC_CONFIG_ID
            | OUTLOOK_INBOX_MRM_CONFIG_ID
            | OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_ID
            | OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_ID
            | OUTLOOK_INBOX_SHARING_CONFIGURATION_ID
            | OUTLOOK_INBOX_SHARING_INDEX_ID
            | OUTLOOK_INBOX_AGGREGATION_ID
    )
}

pub(crate) fn is_outlook_quick_step_default_associated_config_id(item_id: u64) -> bool {
    item_id == OUTLOOK_QUICK_STEP_CUSTOM_ACTION_ID
}

pub(crate) fn is_outlook_contact_default_associated_config_id(item_id: u64) -> bool {
    if matches!(
        item_id,
        OUTLOOK_CONTACTS_OSC_CONTACT_SYNC_ID
            | OUTLOOK_SUGGESTED_CONTACTS_OSC_CONTACT_SYNC_ID
            | OUTLOOK_QUICK_CONTACTS_OSC_CONTACT_SYNC_ID
            | OUTLOOK_IM_CONTACT_LIST_OSC_CONTACT_SYNC_ID
            | OUTLOOK_CONTACTS_CONTACT_LINK_TIMESTAMP_ID
            | OUTLOOK_SUGGESTED_CONTACTS_CONTACT_LINK_TIMESTAMP_ID
            | OUTLOOK_QUICK_CONTACTS_CONTACT_LINK_TIMESTAMP_ID
            | OUTLOOK_IM_CONTACT_LIST_CONTACT_LINK_TIMESTAMP_ID
    ) {
        return true;
    }
    crate::mapi::identity::global_counter_from_store_id(item_id).is_some_and(|counter| {
        let folder_counter = counter & 0x00FF_FFFF;
        matches!(
            counter & !0x00FF_FFFF,
            OUTLOOK_DYNAMIC_CONTACT_SYNC_CONFIG_COUNTER_BASE
                | OUTLOOK_DYNAMIC_CONTACT_LINK_TIMESTAMP_COUNTER_BASE
        ) && folder_counter != 0
            && folder_counter < 0x00FF_FF00
    })
}

pub(crate) fn is_outlook_common_views_default_named_view_id(item_id: u64) -> bool {
    matches!(
        item_id,
        OUTLOOK_COMMON_VIEWS_COMPACT_NAMED_VIEW_ID | OUTLOOK_COMMON_VIEWS_SENT_TO_NAMED_VIEW_ID
    )
}

pub(crate) fn is_outlook_default_folder_named_view_id(item_id: u64) -> bool {
    item_id == OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID
}

pub(crate) fn outlook_default_folder_named_view_name(folder_id: u64) -> &'static str {
    match folder_id {
        crate::mapi::identity::CALENDAR_FOLDER_ID => "Calendar",
        crate::mapi::identity::CONTACTS_FOLDER_ID
        | crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID => "Contacts",
        crate::mapi::identity::TASKS_FOLDER_ID | crate::mapi::identity::TODO_SEARCH_FOLDER_ID => {
            "Tasks"
        }
        crate::mapi::identity::NOTES_FOLDER_ID => "Notes",
        crate::mapi::identity::JOURNAL_FOLDER_ID => "Journal",
        _ => "Compact",
    }
}

pub(crate) fn is_outlook_common_views_default_navigation_shortcut_id(item_id: u64) -> bool {
    matches!(
        item_id,
        OUTLOOK_COMMON_VIEWS_DEFAULT_MAIL_GROUP_HEADER_ID
            | OUTLOOK_COMMON_VIEWS_DEFAULT_NAVIGATION_SHORTCUT_ID
            | OUTLOOK_COMMON_VIEWS_DEFAULT_SENT_NAVIGATION_SHORTCUT_ID
            | OUTLOOK_COMMON_VIEWS_DEFAULT_TRASH_NAVIGATION_SHORTCUT_ID
    )
}

pub(crate) fn is_outlook_default_conversation_action_id(item_id: u64) -> bool {
    item_id == OUTLOOK_DEFAULT_CONVERSATION_ACTION_ID
}

pub(crate) fn is_outlook_local_freebusy_message_id(item_id: u64) -> bool {
    item_id == OUTLOOK_LOCAL_FREEBUSY_MESSAGE_ID
}

fn outlook_inbox_associated_config_defaults(folder_id: u64) -> Vec<MapiAssociatedConfigMessage> {
    vec![
        MapiAssociatedConfigMessage {
            id: OUTLOOK_INBOX_ACCOUNT_PREFS_CONFIG_ID,
            folder_id,
            canonical_id: Uuid::from_u128(0x6d617069_6163_6350_8000_000000000001),
            message_class: OUTLOOK_INBOX_ACCOUNT_PREFS_CONFIG_CLASS.to_string(),
            subject: OUTLOOK_INBOX_ACCOUNT_PREFS_CONFIG_CLASS.to_string(),
            properties_json: serde_json::json!({}),
        },
        MapiAssociatedConfigMessage {
            id: OUTLOOK_INBOX_EAS_CONFIG_ID,
            folder_id,
            canonical_id: Uuid::from_u128(0x6d617069_6561_7343_8000_000000000001),
            message_class: OUTLOOK_INBOX_EAS_CONFIG_CLASS.to_string(),
            subject: OUTLOOK_INBOX_EAS_CONFIG_CLASS.to_string(),
            properties_json: serde_json::json!({}),
        },
        MapiAssociatedConfigMessage {
            id: OUTLOOK_INBOX_ELC_CONFIG_ID,
            folder_id,
            canonical_id: Uuid::from_u128(0x6d617069_656c_6343_8000_000000000001),
            message_class: OUTLOOK_INBOX_ELC_CONFIG_CLASS.to_string(),
            subject: OUTLOOK_INBOX_ELC_CONFIG_CLASS.to_string(),
            properties_json: serde_json::json!({}),
        },
        MapiAssociatedConfigMessage {
            id: OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_ID,
            folder_id,
            canonical_id: Uuid::from_u128(0x6d617069_6d6c_7343_8000_000000000001),
            message_class: OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_CLASS.to_string(),
            subject: OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_CLASS.to_string(),
            properties_json: serde_json::json!({}),
        },
        MapiAssociatedConfigMessage {
            id: OUTLOOK_INBOX_MRM_CONFIG_ID,
            folder_id,
            canonical_id: Uuid::from_u128(0x6d617069_6d72_6d43_8000_000000000001),
            message_class: OUTLOOK_INBOX_MRM_CONFIG_CLASS.to_string(),
            subject: OUTLOOK_INBOX_MRM_CONFIG_CLASS.to_string(),
            properties_json: serde_json::json!({}),
        },
        MapiAssociatedConfigMessage {
            id: OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_ID,
            folder_id,
            canonical_id: Uuid::from_u128(0x6d617069_756d_6f6c_8000_000000000001),
            message_class: OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS.to_string(),
            subject: OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS.to_string(),
            properties_json: serde_json::json!({}),
        },
        MapiAssociatedConfigMessage {
            id: OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_ID,
            folder_id,
            canonical_id: Uuid::from_u128(0x6d617069_7275_6c65_8000_000000000001),
            message_class: OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS.to_string(),
            subject: OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS.to_string(),
            properties_json: serde_json::json!({}),
        },
        MapiAssociatedConfigMessage {
            id: OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_ID,
            folder_id,
            canonical_id: Uuid::from_u128(0x6d617069_696e_4e76_8000_000000000001),
            message_class: OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS.to_string(),
            subject: "Compact".to_string(),
            properties_json: serde_json::json!({}),
        },
        MapiAssociatedConfigMessage {
            id: OUTLOOK_INBOX_SHARING_CONFIGURATION_ID,
            folder_id,
            canonical_id: Uuid::from_u128(0x6d617069_7368_4366_8000_000000000001),
            message_class: OUTLOOK_INBOX_SHARING_CONFIGURATION_CLASS.to_string(),
            subject: OUTLOOK_INBOX_SHARING_CONFIGURATION_CLASS.to_string(),
            properties_json: serde_json::json!({}),
        },
        MapiAssociatedConfigMessage {
            id: OUTLOOK_INBOX_SHARING_INDEX_ID,
            folder_id,
            canonical_id: Uuid::from_u128(0x6d617069_7368_496e_8000_000000000001),
            message_class: OUTLOOK_INBOX_SHARING_INDEX_CLASS.to_string(),
            subject: OUTLOOK_INBOX_SHARING_INDEX_CLASS.to_string(),
            properties_json: serde_json::json!({}),
        },
        MapiAssociatedConfigMessage {
            id: OUTLOOK_INBOX_AGGREGATION_ID,
            folder_id,
            canonical_id: Uuid::from_u128(0x6d617069_6167_6772_8000_000000000001),
            message_class: OUTLOOK_INBOX_AGGREGATION_CLASS.to_string(),
            subject: OUTLOOK_INBOX_AGGREGATION_CLASS.to_string(),
            properties_json: serde_json::json!({}),
        },
    ]
}

fn outlook_inbox_associated_config_sync_defaults(
    folder_id: u64,
) -> Vec<MapiAssociatedConfigMessage> {
    let _ = folder_id;
    Vec::new()
}

pub(crate) fn outlook_inbox_exact_virtual_associated_config_for_message_class(
    message_class: &str,
) -> Option<MapiAssociatedConfigMessage> {
    if !matches!(
        message_class,
        "IPM.Configuration.ELC"
            | OUTLOOK_INBOX_MRM_CONFIG_CLASS
            | OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS
            | OUTLOOK_INBOX_SHARING_CONFIGURATION_CLASS
            | OUTLOOK_INBOX_SHARING_INDEX_CLASS
            | OUTLOOK_INBOX_AGGREGATION_CLASS
    ) {
        return None;
    }
    outlook_inbox_associated_config_defaults(crate::mapi::identity::INBOX_FOLDER_ID)
        .into_iter()
        .find(|message| message.message_class.eq_ignore_ascii_case(message_class))
}

fn outlook_inbox_exact_virtual_associated_config_for_id(
    item_id: u64,
) -> Option<MapiAssociatedConfigMessage> {
    outlook_inbox_associated_config_defaults(crate::mapi::identity::INBOX_FOLDER_ID)
        .into_iter()
        .find(|message| {
            message.id == item_id
                && outlook_inbox_exact_virtual_associated_config_for_message_class(
                    &message.message_class,
                )
                .is_some()
        })
}

pub(crate) fn outlook_inbox_message_list_settings_default() -> MapiAssociatedConfigMessage {
    outlook_inbox_associated_config_defaults(crate::mapi::identity::INBOX_FOLDER_ID)
        .into_iter()
        .find(|message| message.message_class == OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_CLASS)
        .expect("Inbox MessageListSettings default")
}

pub(crate) fn modeled_virtual_associated_config_message_for_canonical_id(
    canonical_id: Uuid,
) -> Option<MapiAssociatedConfigMessage> {
    outlook_inbox_associated_config_sync_defaults(crate::mapi::identity::INBOX_FOLDER_ID)
        .into_iter()
        .chain(outlook_quick_step_associated_config_defaults(
            crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
        ))
        .chain(
            [
                crate::mapi::identity::CONTACTS_FOLDER_ID,
                crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
                crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID,
                crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID,
            ]
            .into_iter()
            .flat_map(outlook_contact_associated_config_defaults),
        )
        .find(|message| message.canonical_id == canonical_id)
}

fn format_associated_config_classes(configs: &[MapiAssociatedConfigRecord]) -> String {
    let mut classes = configs
        .iter()
        .map(|config| config.message_class.clone())
        .collect::<Vec<_>>();
    classes.sort();
    classes.dedup();
    classes.join(",")
}

fn format_associated_config_inputs(inputs: &[UpsertMapiAssociatedConfigInput]) -> String {
    let mut classes = inputs
        .iter()
        .map(|input| input.message_class.clone())
        .collect::<Vec<_>>();
    classes.sort();
    classes.dedup();
    classes.join(",")
}

fn log_outlook_inbox_associated_config_bootstrap(
    account_id: Uuid,
    persisted: &[MapiAssociatedConfigRecord],
    inserted: &[MapiAssociatedConfigRecord],
    required_defaults: &[UpsertMapiAssociatedConfigInput],
) {
    if inserted.is_empty() {
        return;
    }
    // Keep EAS, ELC, Sharing, and RuleOrganizer rows virtual or client-created
    // until Outlook traces require durable modeled state with non-empty payloads.
    let virtual_only_defaults = [
        OUTLOOK_INBOX_EAS_CONFIG_CLASS,
        OUTLOOK_INBOX_ELC_CONFIG_CLASS,
        OUTLOOK_INBOX_MRM_CONFIG_CLASS,
        OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS,
        OUTLOOK_INBOX_SHARING_CONFIGURATION_CLASS,
        OUTLOOK_INBOX_SHARING_INDEX_CLASS,
        OUTLOOK_INBOX_AGGREGATION_CLASS,
    ];
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        account_id = %account_id,
        folder_id = crate::mapi::identity::INBOX_FOLDER_ID,
        required_persisted_defaults = %format_associated_config_inputs(required_defaults),
        inserted_persisted_defaults = %format_associated_config_classes(inserted),
        persisted_associated_config_classes = %format_associated_config_classes(persisted),
        virtual_only_defaults = %virtual_only_defaults.join(","),
        "rca debug mapi inbox associated config bootstrap"
    );
}

fn is_empty_synthetic_inbox_associated_config(config: &MapiAssociatedConfigRecord) -> bool {
    if config.folder_id != crate::mapi::identity::INBOX_FOLDER_ID {
        return false;
    }
    if !config
        .properties_json
        .as_object()
        .is_some_and(|object| object.is_empty())
    {
        return false;
    }
    outlook_inbox_associated_config_defaults(crate::mapi::identity::INBOX_FOLDER_ID)
        .into_iter()
        .any(|default| {
            config.id == default.canonical_id
                && config.message_class == default.message_class
                && config.subject == default.subject
        })
}

fn is_empty_outlook_inbox_named_view_placeholder(config: &MapiAssociatedConfigRecord) -> bool {
    config.folder_id == crate::mapi::identity::INBOX_FOLDER_ID
        && config.message_class == OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS
        && config.subject == "Compact"
        && config
            .properties_json
            .as_object()
            .is_some_and(|object| object.is_empty())
}

fn is_empty_outlook_rule_organizer_placeholder(config: &MapiAssociatedConfigRecord) -> bool {
    config.folder_id == crate::mapi::identity::INBOX_FOLDER_ID
        && config.message_class == OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS
        && !config
            .properties_json
            .get(OUTLOOK_RULE_ORGANIZER_BINARY_6802_JSON_KEY)
            .and_then(|value| value.get("value"))
            .and_then(|value| value.as_str())
            .is_some_and(|value| !value.is_empty())
}

fn outlook_quick_step_associated_config_defaults(
    folder_id: u64,
) -> Vec<MapiAssociatedConfigMessage> {
    vec![MapiAssociatedConfigMessage {
        id: OUTLOOK_QUICK_STEP_CUSTOM_ACTION_ID,
        folder_id,
        canonical_id: Uuid::from_u128(0x6d617069_7173_4361_8000_000000000001),
        message_class: OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS.to_string(),
        subject: OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS.to_string(),
        properties_json: serde_json::json!({}),
    }]
}

fn outlook_common_views_default_named_views() -> Vec<MapiCommonViewNamedViewMessage> {
    vec![
        MapiCommonViewNamedViewMessage {
            id: OUTLOOK_COMMON_VIEWS_COMPACT_NAMED_VIEW_ID,
            folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x6d617069_6376_4e76_8000_000000000001),
            name: "Compact".to_string(),
            view_flags: 14_745_605,
            view_type: 8,
        },
        MapiCommonViewNamedViewMessage {
            id: OUTLOOK_COMMON_VIEWS_SENT_TO_NAMED_VIEW_ID,
            folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x6d617069_6376_4e76_8000_000000000002),
            name: "Sent To".to_string(),
            view_flags: 15_269_893,
            view_type: 8,
        },
    ]
}

fn outlook_common_views_default_navigation_shortcuts() -> Vec<MapiNavigationShortcutMessage> {
    vec![
        MapiNavigationShortcutMessage {
            id: OUTLOOK_COMMON_VIEWS_DEFAULT_MAIL_GROUP_HEADER_ID,
            folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
            canonical_id: crate::mapi::properties::default_wlink_group_uuid(),
            subject: OUTLOOK_MAIL_FAVORITES_GROUP_NAME.to_string(),
            target_folder_id: None,
            shortcut_type: 4,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 0,
            group_header_id: Some(crate::mapi::properties::default_wlink_group_uuid()),
            group_name: OUTLOOK_MAIL_FAVORITES_GROUP_NAME.to_string(),
        },
        MapiNavigationShortcutMessage {
            id: OUTLOOK_COMMON_VIEWS_DEFAULT_NAVIGATION_SHORTCUT_ID,
            folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x6d617069_776c_496e_8000_000000000001),
            subject: "Inbox".to_string(),
            target_folder_id: Some(crate::mapi::identity::INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 127,
            group_header_id: Some(crate::mapi::properties::default_wlink_group_uuid()),
            group_name: OUTLOOK_MAIL_FAVORITES_GROUP_NAME.to_string(),
        },
        MapiNavigationShortcutMessage {
            id: OUTLOOK_COMMON_VIEWS_DEFAULT_SENT_NAVIGATION_SHORTCUT_ID,
            folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x6d617069_776c_536e_8000_000000000001),
            subject: "Sent".to_string(),
            target_folder_id: Some(crate::mapi::identity::SENT_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 128,
            group_header_id: Some(crate::mapi::properties::default_wlink_group_uuid()),
            group_name: OUTLOOK_MAIL_FAVORITES_GROUP_NAME.to_string(),
        },
        MapiNavigationShortcutMessage {
            id: OUTLOOK_COMMON_VIEWS_DEFAULT_TRASH_NAVIGATION_SHORTCUT_ID,
            folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x6d617069_776c_5472_8000_000000000001),
            subject: "Trash".to_string(),
            target_folder_id: Some(crate::mapi::identity::TRASH_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 129,
            group_header_id: Some(crate::mapi::properties::default_wlink_group_uuid()),
            group_name: OUTLOOK_MAIL_FAVORITES_GROUP_NAME.to_string(),
        },
    ]
}

fn is_transient_outlook_migration_associated_config_class(message_class: &str) -> bool {
    matches!(
        message_class,
        "IPM.Microsoft.MigrationStatus"
            | "IPM.Microsoft.PendingChange.MigrateCategoriesList"
            | "IPM.Microsoft.PendingChange.MigrateFlags"
            | "IPM.Microsoft.PendingChange.MigrateLabels"
    )
}

fn outlook_contact_sync_associated_config_default(
    folder_id: u64,
) -> Option<MapiAssociatedConfigMessage> {
    let (id, canonical_id) = match folder_id {
        crate::mapi::identity::CONTACTS_FOLDER_ID => (
            OUTLOOK_CONTACTS_OSC_CONTACT_SYNC_ID,
            Uuid::from_u128(0x6d617069_6f73_6343_8000_000000000001),
        ),
        crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID => (
            OUTLOOK_SUGGESTED_CONTACTS_OSC_CONTACT_SYNC_ID,
            Uuid::from_u128(0x6d617069_6f73_6343_8000_000000000002),
        ),
        crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID => (
            OUTLOOK_QUICK_CONTACTS_OSC_CONTACT_SYNC_ID,
            Uuid::from_u128(0x6d617069_6f73_6343_8000_000000000003),
        ),
        crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID => (
            OUTLOOK_IM_CONTACT_LIST_OSC_CONTACT_SYNC_ID,
            Uuid::from_u128(0x6d617069_6f73_6343_8000_000000000004),
        ),
        _ => (
            outlook_dynamic_contact_sync_config_id(folder_id)?,
            outlook_dynamic_contact_sync_canonical_id(folder_id),
        ),
    };
    Some(MapiAssociatedConfigMessage {
        id,
        folder_id,
        canonical_id,
        message_class: OUTLOOK_CONTACT_SYNC_CONFIG_CLASS.to_string(),
        subject: OUTLOOK_CONTACT_SYNC_CONFIG_CLASS.to_string(),
        properties_json: serde_json::json!({}),
    })
}

fn outlook_contact_link_timestamp_associated_config_default(
    folder_id: u64,
) -> Option<MapiAssociatedConfigMessage> {
    let (id, canonical_id) = match folder_id {
        crate::mapi::identity::CONTACTS_FOLDER_ID => (
            OUTLOOK_CONTACTS_CONTACT_LINK_TIMESTAMP_ID,
            Uuid::from_u128(0x6d617069_636c_7453_8000_000000000001),
        ),
        crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID => (
            OUTLOOK_SUGGESTED_CONTACTS_CONTACT_LINK_TIMESTAMP_ID,
            Uuid::from_u128(0x6d617069_636c_7453_8000_000000000002),
        ),
        crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID => (
            OUTLOOK_QUICK_CONTACTS_CONTACT_LINK_TIMESTAMP_ID,
            Uuid::from_u128(0x6d617069_636c_7453_8000_000000000003),
        ),
        crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID => (
            OUTLOOK_IM_CONTACT_LIST_CONTACT_LINK_TIMESTAMP_ID,
            Uuid::from_u128(0x6d617069_636c_7453_8000_000000000004),
        ),
        _ => (
            outlook_dynamic_contact_link_timestamp_config_id(folder_id)?,
            outlook_dynamic_contact_link_timestamp_canonical_id(folder_id),
        ),
    };
    Some(MapiAssociatedConfigMessage {
        id,
        folder_id,
        canonical_id,
        message_class: OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS.to_string(),
        subject: OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS.to_string(),
        properties_json: serde_json::json!({}),
    })
}

fn outlook_contact_associated_config_defaults(folder_id: u64) -> Vec<MapiAssociatedConfigMessage> {
    let contact_sync = (folder_id != crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID)
        .then(|| outlook_contact_sync_associated_config_default(folder_id))
        .flatten();
    contact_sync
        .into_iter()
        .chain(outlook_contact_link_timestamp_associated_config_default(
            folder_id,
        ))
        .collect()
}

fn outlook_dynamic_contact_sync_config_id(folder_id: u64) -> Option<u64> {
    let folder_counter = crate::mapi::identity::global_counter_from_store_id(folder_id)?;
    if folder_counter == 0 || folder_counter >= 0x00FF_FF00 {
        return None;
    }
    Some(crate::mapi::identity::mapi_store_id(
        OUTLOOK_DYNAMIC_CONTACT_SYNC_CONFIG_COUNTER_BASE | folder_counter,
    ))
}

fn outlook_dynamic_contact_link_timestamp_config_id(folder_id: u64) -> Option<u64> {
    let folder_counter = crate::mapi::identity::global_counter_from_store_id(folder_id)?;
    if folder_counter == 0 || folder_counter >= 0x00FF_FF00 {
        return None;
    }
    Some(crate::mapi::identity::mapi_store_id(
        OUTLOOK_DYNAMIC_CONTACT_LINK_TIMESTAMP_COUNTER_BASE | folder_counter,
    ))
}

fn outlook_dynamic_contact_sync_canonical_id(folder_id: u64) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"lpe:mapi:outlook-osc-contact-sync:v1");
    hash.update(folder_id.to_le_bytes());
    let digest = hash.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn outlook_dynamic_contact_link_timestamp_canonical_id(folder_id: u64) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"lpe:mapi:outlook-contact-link-timestamp:v1");
    hash.update(folder_id.to_le_bytes());
    let digest = hash.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn outlook_default_conversation_action() -> MapiConversationActionMessage {
    MapiConversationActionMessage {
        id: OUTLOOK_DEFAULT_CONVERSATION_ACTION_ID,
        folder_id: crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x6d617069_6361_4466_8000_000000000001),
        action: ConversationAction {
            id: Uuid::from_u128(0x6d617069_6361_4466_8000_000000000001),
            conversation_id: Uuid::nil(),
            subject: "IPM.ConversationAction".to_string(),
            categories_json: "[]".to_string(),
            move_folder_entry_id: None,
            move_store_entry_id: None,
            move_target_mailbox_id: None,
            max_delivery_time: None,
            last_applied_time: None,
            version: lpe_storage::CONVERSATION_ACTION_VERSION,
            processed: 0,
            created_at: "1970-01-01T00:00:00Z".to_string(),
            updated_at: "1970-01-01T00:00:00Z".to_string(),
        },
    }
}

fn virtual_local_freebusy_message() -> MapiDelegateFreeBusyMessage {
    let canonical_id = Uuid::from_u128(0x6d617069_6672_4266_8000_000000000001);
    MapiDelegateFreeBusyMessage {
        id: OUTLOOK_LOCAL_FREEBUSY_MESSAGE_ID,
        folder_id: crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
        canonical_id,
        message: DelegateFreeBusyMessageObject {
            id: canonical_id,
            account_id: Uuid::nil(),
            owner_account_id: Uuid::nil(),
            owner_email: String::new(),
            message_kind: "freebusy".to_string(),
            subject: "LocalFreebusy".to_string(),
            body_text: String::new(),
            starts_at: None,
            ends_at: None,
            busy_status: None,
            payload_json: "{}".to_string(),
            updated_at: "1970-01-01T00:00:00Z".to_string(),
        },
    }
}

pub(crate) enum MapiCommonViewsMessage {
    NavigationShortcut(MapiNavigationShortcutMessage),
    #[allow(dead_code)]
    NamedView(MapiCommonViewNamedViewMessage),
    SearchFolderDefinition(SearchFolderDefinition),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiConversationActionMessage {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) action: ConversationAction,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiDelegateFreeBusyMessage {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) message: DelegateFreeBusyMessageObject,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiRecoverableItemMessage {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) item: RecoverableItem,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiAttachment {
    pub(crate) attach_num: u32,
    pub(crate) canonical_id: Uuid,
    pub(crate) file_reference: String,
    pub(crate) file_name: String,
    pub(crate) media_type: String,
    pub(crate) disposition: Option<String>,
    pub(crate) content_id: Option<String>,
    pub(crate) size_octets: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiContentTableWindow {
    pub(crate) folder_id: u64,
    pub(crate) view_signature: u64,
    pub(crate) offset: usize,
    pub(crate) total: usize,
    pub(crate) message_ids: Vec<Uuid>,
}

impl MapiMailStoreSnapshot {
    pub(crate) fn empty() -> Self {
        Self::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }

    pub(crate) fn new(
        mailboxes: Vec<JmapMailbox>,
        emails: Vec<JmapEmail>,
        attachments: Vec<(Uuid, Vec<ActiveSyncAttachment>)>,
        contact_collections: Vec<CollaborationCollection>,
        calendar_collections: Vec<CollaborationCollection>,
        task_collections: Vec<CollaborationCollection>,
        contacts: Vec<AccessibleContact>,
        events: Vec<AccessibleEvent>,
        tasks: Vec<ClientTask>,
        folder_permissions: Vec<MapiFolderPermission>,
    ) -> Self {
        let folders = mailboxes
            .into_iter()
            .map(|mailbox| MapiFolder {
                id: mapi_folder_id(&mailbox),
                canonical_id: mailbox.id,
                mailbox,
            })
            .collect::<Vec<_>>();
        let messages = emails
            .into_iter()
            .map(|email| {
                let folder_id = mapi_message_folder_id(&email, &folders);
                let message_attachments = attachments
                    .iter()
                    .find(|(message_id, _)| *message_id == email.id)
                    .map(|(_, attachments)| attachments.as_slice())
                    .unwrap_or_default()
                    .iter()
                    .enumerate()
                    .map(|(index, attachment)| MapiAttachment {
                        attach_num: index as u32,
                        canonical_id: attachment.id,
                        file_reference: attachment.file_reference.clone(),
                        file_name: attachment.file_name.clone(),
                        media_type: attachment.media_type.clone(),
                        disposition: attachment.disposition.clone(),
                        content_id: attachment.content_id.clone(),
                        size_octets: attachment.size_octets,
                    })
                    .collect::<Vec<_>>();
                MapiMessage {
                    id: mapi_message_id(&email),
                    folder_id,
                    canonical_id: email.id,
                    email,
                    attachments: message_attachments,
                }
            })
            .collect();
        let mut collaboration_folders = Vec::new();
        collaboration_folders.extend(contact_collections.into_iter().map(|collection| {
            let id =
                mapi_collaboration_folder_id(MapiCollaborationFolderKind::Contacts, &collection);
            let item_count = contacts
                .iter()
                .filter(|contact| contact.collection_id == collection.id)
                .count()
                .min(u32::MAX as usize) as u32;
            MapiCollaborationFolder {
                id,
                kind: MapiCollaborationFolderKind::Contacts,
                collection,
                item_count,
            }
        }));
        collaboration_folders.extend(calendar_collections.into_iter().map(|collection| {
            let id =
                mapi_collaboration_folder_id(MapiCollaborationFolderKind::Calendar, &collection);
            let item_count = events
                .iter()
                .filter(|event| event.collection_id == collection.id)
                .count()
                .min(u32::MAX as usize) as u32;
            MapiCollaborationFolder {
                id,
                kind: MapiCollaborationFolderKind::Calendar,
                collection,
                item_count,
            }
        }));
        collaboration_folders.extend(task_collections.into_iter().map(|collection| {
            let id = mapi_collaboration_folder_id(MapiCollaborationFolderKind::Task, &collection);
            let item_count = tasks
                .iter()
                .filter(|task| task_collection_matches(task, &collection.id))
                .count()
                .min(u32::MAX as usize) as u32;
            MapiCollaborationFolder {
                id,
                kind: MapiCollaborationFolderKind::Task,
                collection,
                item_count,
            }
        }));
        let contacts = contacts
            .into_iter()
            .filter_map(|contact| {
                let folder_id = collaboration_folders
                    .iter()
                    .find(|folder| {
                        folder.kind == MapiCollaborationFolderKind::Contacts
                            && folder.collection.id == contact.collection_id
                    })
                    .map(|folder| folder.id)?;
                Some(MapiContact {
                    id: mapi_item_id(&contact.id),
                    folder_id,
                    canonical_id: contact.id,
                    contact,
                })
            })
            .collect();
        let events = events
            .into_iter()
            .filter_map(|event| {
                let folder_id = collaboration_folders
                    .iter()
                    .find(|folder| {
                        folder.kind == MapiCollaborationFolderKind::Calendar
                            && folder.collection.id == event.collection_id
                    })
                    .map(|folder| folder.id)
                    .or_else(|| {
                        matches!(event.collection_id.as_str(), "default" | "calendar")
                            .then_some(crate::mapi::identity::CALENDAR_FOLDER_ID)
                    })?;
                Some(MapiEvent {
                    id: mapi_item_id(&event.id),
                    folder_id,
                    canonical_id: event.id,
                    event,
                    attachments: Vec::new(),
                })
            })
            .collect();
        let tasks = tasks
            .into_iter()
            .filter_map(|task| {
                let folder_id = collaboration_folders
                    .iter()
                    .find(|folder| {
                        folder.kind == MapiCollaborationFolderKind::Task
                            && task_collection_matches(&task, &folder.collection.id)
                    })
                    .map(|folder| folder.id)?;
                Some(MapiTask {
                    id: mapi_item_id(&task.id),
                    folder_id,
                    canonical_id: task.id,
                    task,
                })
            })
            .collect();
        Self {
            folders,
            public_folders: Vec::new(),
            public_folder_items: Vec::new(),
            public_folder_replicas: Vec::new(),
            collaboration_folders,
            messages,
            contacts,
            events,
            tasks,
            notes: Vec::new(),
            journal_entries: Vec::new(),
            search_folder_definitions: Vec::new(),
            rules: Vec::new(),
            navigation_shortcuts: Vec::new(),
            associated_configs: Vec::new(),
            associated_config_identity_ids: Vec::new(),
            conversation_actions: Vec::new(),
            delegate_freebusy_messages: Vec::new(),
            recoverable_items: Vec::new(),
            reminders: Vec::new(),
            folder_permissions,
            public_folder_permissions: Vec::new(),
            content_windows: Vec::new(),
        }
    }

    pub(crate) fn with_search_folder_definitions(
        mut self,
        search_folder_definitions: Vec<SearchFolderDefinition>,
    ) -> Self {
        self.folders
            .retain(|folder| !folder.mailbox.role.starts_with("__mapi_search_folder_"));
        let mut projected_user_saved_keys = HashSet::new();
        self.folders
            .extend(search_folder_definitions.iter().filter_map(|definition| {
                if !user_saved_search_folder_is_projectable(definition) {
                    return None;
                }
                if definition.is_builtin || definition.definition_kind != "user_saved" {
                    return mapi_search_folder_definition_to_folder(definition);
                }
                let key = user_saved_search_folder_projection_key(definition);
                if !projected_user_saved_keys.insert(key) {
                    return None;
                }
                mapi_search_folder_definition_to_folder(definition)
            }));
        self.search_folder_definitions = search_folder_definitions;
        self
    }

    pub(crate) fn with_rules(mut self, rules: Vec<MailboxRule>) -> Self {
        self.rules = rules
            .into_iter()
            .map(|rule| {
                let id = mapi_item_id(&rule.id);
                MapiRule {
                    id,
                    canonical_id: rule.id,
                    name: rule.name,
                    is_active: rule.is_active,
                    condition_summary: rule.condition_summary,
                    action_summary: rule.action_summary,
                    updated_at: rule.updated_at,
                }
            })
            .collect();
        self
    }

    pub(crate) fn with_conversation_actions(
        mut self,
        conversation_actions: Vec<ConversationAction>,
    ) -> Self {
        self.conversation_actions = conversation_actions
            .into_iter()
            .map(|action| MapiConversationActionMessage {
                id: mapi_item_id(&action.id),
                folder_id: crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
                canonical_id: action.id,
                action,
            })
            .collect();
        self
    }

    pub(crate) fn with_delegate_freebusy_messages(
        mut self,
        messages: Vec<DelegateFreeBusyMessageObject>,
    ) -> Self {
        self.delegate_freebusy_messages = messages
            .into_iter()
            .map(|message| MapiDelegateFreeBusyMessage {
                id: mapi_item_id(&message.id),
                folder_id: crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
                canonical_id: message.id,
                message,
            })
            .collect();
        if self.delegate_freebusy_messages.is_empty() {
            self.delegate_freebusy_messages
                .push(virtual_local_freebusy_message());
        }
        self
    }

    pub(crate) fn with_recoverable_items(
        mut self,
        recoverable_items: Vec<RecoverableItem>,
    ) -> Self {
        self.recoverable_items = recoverable_items
            .into_iter()
            .filter_map(|item| {
                let folder_id = recoverable_mapi_folder_id(&item.recoverable_folder)?;
                Some(MapiRecoverableItemMessage {
                    id: mapi_recoverable_item_id(&item.id),
                    folder_id,
                    canonical_id: item.id,
                    item,
                })
            })
            .collect();
        self
    }

    pub(crate) fn with_navigation_shortcuts(
        mut self,
        navigation_shortcuts: Vec<MapiNavigationShortcutRecord>,
    ) -> Self {
        let account_id = navigation_shortcuts
            .first()
            .map(|shortcut| shortcut.account_id);
        let persisted_shortcut_count = navigation_shortcuts.len();
        let dropped_synthetic_group_header_count = navigation_shortcuts
            .iter()
            .filter(|shortcut| is_synthetic_common_views_group_header(shortcut))
            .count();
        let persisted_navigation_shortcuts = navigation_shortcuts
            .into_iter()
            .filter(|shortcut| !is_synthetic_common_views_group_header(shortcut))
            .map(|shortcut| {
                let group_header_id = Some(
                    shortcut
                        .group_header_id
                        .unwrap_or_else(crate::mapi::properties::default_wlink_group_uuid),
                );
                MapiNavigationShortcutMessage {
                    id: mapi_item_id(&shortcut.id),
                    folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
                    canonical_id: shortcut.id,
                    subject: shortcut.subject,
                    target_folder_id: shortcut.target_folder_id,
                    shortcut_type: shortcut.shortcut_type,
                    flags: shortcut.flags,
                    save_stamp: shortcut.save_stamp,
                    section: shortcut.section,
                    ordinal: shortcut.ordinal,
                    group_header_id,
                    group_name: normalize_navigation_shortcut_group_name(
                        shortcut.section,
                        group_header_id,
                        &shortcut.group_name,
                    ),
                }
            })
            .collect::<Vec<_>>();
        let persisted_navigation_shortcut_summary =
            format_navigation_shortcut_debug_summary(&persisted_navigation_shortcuts);
        self.navigation_shortcuts = persisted_navigation_shortcuts;
        let deduped_shortcuts = self.navigation_shortcut_messages();
        let table_messages = self.common_views_table_messages().collect::<Vec<_>>();
        let table_shortcut_count = table_messages
            .iter()
            .filter(|message| matches!(message, MapiCommonViewsMessage::NavigationShortcut(_)))
            .count();
        let default_table_shortcut_count = table_messages
            .iter()
            .filter(|message| {
                matches!(
                    message,
                    MapiCommonViewsMessage::NavigationShortcut(shortcut)
                        if is_outlook_common_views_default_navigation_shortcut_id(shortcut.id)
                )
            })
            .count();
        tracing::debug!(
            rca_debug = true,
            adapter = "mapi",
            account_id = %account_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            persisted_navigation_shortcut_count = persisted_shortcut_count,
            materialized_navigation_shortcut_count = table_shortcut_count,
            deduped_navigation_shortcut_count = deduped_shortcuts.len(),
            common_views_table_shortcut_count = table_shortcut_count,
            common_views_default_table_shortcut_count = default_table_shortcut_count,
            dropped_synthetic_group_header_count,
            common_views_named_view_count = table_messages.len().saturating_sub(table_shortcut_count),
            persisted_navigation_shortcuts =
                %persisted_navigation_shortcut_summary,
            materialized_navigation_shortcuts =
                %format_common_views_table_shortcut_debug_summary(&table_messages),
            deduped_navigation_shortcuts =
                %format_navigation_shortcut_debug_summary(&deduped_shortcuts),
            common_views_table_shortcuts =
                %format_common_views_table_shortcut_debug_summary(&table_messages),
            "rca debug mapi navigation shortcut snapshot contract"
        );
        self
    }

    pub(crate) fn with_associated_configs(
        mut self,
        configs: Vec<MapiAssociatedConfigRecord>,
    ) -> Self {
        self.associated_configs = deduplicate_associated_config_messages(
            configs
                .into_iter()
                .filter(|config| {
                    !is_transient_outlook_migration_associated_config_class(&config.message_class)
                        && !is_empty_synthetic_inbox_associated_config(config)
                        && !is_empty_outlook_inbox_named_view_placeholder(config)
                })
                .map(|config| MapiAssociatedConfigMessage {
                    id: mapi_item_id(&config.id),
                    folder_id: config.folder_id,
                    canonical_id: config.id,
                    message_class: config.message_class,
                    subject: config.subject,
                    properties_json: config.properties_json,
                })
                .collect(),
        );
        self
    }

    pub(crate) fn with_associated_config_identity_ids(
        mut self,
        ids: Vec<MapiAssociatedConfigIdentity>,
    ) -> Self {
        self.associated_config_identity_ids = ids;
        self
    }

    pub(crate) fn with_reminders(mut self, reminders: Vec<ClientReminder>) -> Self {
        self.reminders = reminders;
        self
    }

    pub(crate) fn with_public_folders(
        mut self,
        folders: Vec<PublicFolder>,
        items: Vec<PublicFolderItem>,
        permissions: Vec<PublicFolderPermission>,
    ) -> Self {
        let all_folders = folders.clone();
        self.public_folders = folders
            .into_iter()
            .map(|folder| {
                let item_count = items
                    .iter()
                    .filter(|item| item.public_folder_id == folder.id)
                    .count()
                    .min(u32::MAX as usize) as u32;
                let child_count = all_folders
                    .iter()
                    .filter(|candidate| candidate.parent_folder_id == Some(folder.id))
                    .count()
                    .min(u32::MAX as usize) as u32;
                MapiPublicFolder {
                    id: mapi_public_folder_id(&folder),
                    folder,
                    item_count,
                    child_count,
                }
            })
            .collect();
        self.public_folder_items = items
            .into_iter()
            .filter_map(|item| {
                let folder_id =
                    crate::mapi::identity::mapped_mapi_object_id(&item.public_folder_id)?;
                Some(MapiPublicFolderItem {
                    id: mapi_item_id(&item.id),
                    folder_id,
                    item,
                })
            })
            .collect();
        self.public_folder_permissions = permissions
            .into_iter()
            .map(mapi_public_folder_permission)
            .collect();
        self
    }

    pub(crate) fn with_public_folder_replicas(
        mut self,
        replicas: Vec<PublicFolderReplica>,
    ) -> Self {
        let mut replicas = replicas;
        replicas.sort_by(|left, right| {
            left.sort_order
                .cmp(&right.sort_order)
                .then_with(|| left.server_name.cmp(&right.server_name))
        });
        self.public_folder_replicas = replicas
            .into_iter()
            .filter_map(|replica| {
                let folder_id =
                    crate::mapi::identity::mapped_mapi_object_id(&replica.public_folder_id)?;
                Some(MapiPublicFolderReplica { folder_id, replica })
            })
            .collect();
        self
    }

    pub(crate) fn with_notes_and_journal(
        mut self,
        notes: Vec<ClientNote>,
        journal_entries: Vec<JournalEntry>,
    ) -> Self {
        self.notes = notes
            .into_iter()
            .map(|note| MapiNote {
                id: mapi_item_id(&note.id),
                folder_id: crate::mapi::identity::NOTES_FOLDER_ID,
                canonical_id: note.id,
                note,
            })
            .collect();
        self.journal_entries = journal_entries
            .into_iter()
            .map(|entry| MapiJournalEntry {
                id: mapi_item_id(&entry.id),
                folder_id: crate::mapi::identity::JOURNAL_FOLDER_ID,
                canonical_id: entry.id,
                entry,
            })
            .collect();
        self
    }

    pub(crate) fn with_content_windows(
        mut self,
        content_windows: Vec<MapiContentTableWindow>,
    ) -> Self {
        self.content_windows = content_windows;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_collaboration_folder_item_count(
        mut self,
        folder_id: u64,
        item_count: u32,
    ) -> Self {
        if let Some(folder) = self
            .collaboration_folders
            .iter_mut()
            .find(|folder| folder.id == folder_id)
        {
            folder.item_count = item_count;
        }
        self
    }

    pub(crate) fn with_calendar_attachments(
        mut self,
        calendar_attachments: Vec<(Uuid, Vec<CalendarEventAttachment>)>,
    ) -> Self {
        for event in &mut self.events {
            event.attachments = calendar_attachments
                .iter()
                .find(|(event_id, _)| *event_id == event.canonical_id)
                .map(|(_, attachments)| attachments.as_slice())
                .unwrap_or_default()
                .iter()
                .enumerate()
                .map(|(index, attachment)| MapiAttachment {
                    attach_num: index as u32,
                    canonical_id: attachment.id,
                    file_reference: attachment.file_reference.clone(),
                    file_name: attachment.file_name.clone(),
                    media_type: attachment.media_type.clone(),
                    disposition: None,
                    content_id: None,
                    size_octets: attachment.size_octets,
                })
                .collect();
        }
        self
    }

    pub(crate) fn mailboxes(&self) -> Vec<JmapMailbox> {
        self.folders
            .iter()
            .map(|folder| folder.mailbox.clone())
            .collect()
    }

    pub(crate) fn emails(&self) -> Vec<JmapEmail> {
        self.messages
            .iter()
            .map(|message| message.email.clone())
            .collect()
    }

    pub(crate) fn content_table_window_emails(
        &self,
        folder_id: u64,
        view_signature: u64,
        offset: usize,
        limit: usize,
    ) -> Option<(usize, Vec<&JmapEmail>)> {
        self.content_windows
            .iter()
            .filter(|window| {
                window.folder_id == folder_id
                    && window.view_signature == view_signature
                    && offset >= window.offset
                    && offset <= window.offset.saturating_add(window.message_ids.len())
            })
            .find_map(|window| {
                let emails = window
                    .message_ids
                    .iter()
                    .filter_map(|id| {
                        self.messages
                            .iter()
                            .find(|message| message.canonical_id == *id)
                            .map(|message| &message.email)
                    })
                    .collect::<Vec<_>>();
                if emails.len() != window.message_ids.len() {
                    return None;
                }
                let local_offset = offset.saturating_sub(window.offset);
                let available = emails.len().saturating_sub(local_offset);
                let window_reaches_end = window.offset.saturating_add(emails.len()) >= window.total;
                if available < limit && !window_reaches_end {
                    return None;
                }
                let row_count = available.min(limit);
                Some((
                    window.total,
                    emails[local_offset..local_offset + row_count].to_vec(),
                ))
            })
    }

    pub(crate) fn content_table_window_emails_containing(
        &self,
        folder_id: u64,
        view_signature: u64,
        position: usize,
    ) -> Option<(usize, usize, Vec<&JmapEmail>)> {
        self.content_windows
            .iter()
            .filter(|window| {
                window.folder_id == folder_id
                    && window.view_signature == view_signature
                    && position >= window.offset
                    && position <= window.offset.saturating_add(window.message_ids.len())
            })
            .filter_map(|window| {
                let emails = window
                    .message_ids
                    .iter()
                    .filter_map(|id| {
                        self.messages
                            .iter()
                            .find(|message| message.canonical_id == *id)
                            .map(|message| &message.email)
                    })
                    .collect::<Vec<_>>();
                if emails.len() != window.message_ids.len() {
                    return None;
                }
                let local_offset = position.saturating_sub(window.offset);
                let tail_len = emails.len().saturating_sub(local_offset);
                Some((window.offset, window.total, emails, tail_len))
            })
            .max_by_key(|(offset, _, _, tail_len)| (*tail_len, *offset))
            .map(|(offset, total, emails, _)| (offset, total, emails))
    }

    pub(crate) fn content_table_total(&self, folder_id: u64, view_signature: u64) -> Option<usize> {
        self.content_windows
            .iter()
            .find(|window| {
                window.folder_id == folder_id
                    && window.view_signature == view_signature
                    && (window.message_ids.is_empty()
                        || window.message_ids.iter().all(|id| {
                            self.messages
                                .iter()
                                .any(|message| message.canonical_id == *id)
                        }))
            })
            .map(|window| window.total)
    }

    pub(crate) fn attachments_for_message(
        &self,
        folder_id: u64,
        message_id: u64,
    ) -> Option<&[MapiAttachment]> {
        if let Some(message) = self
            .messages
            .iter()
            .find(|message| message.folder_id == folder_id && message.id == message_id)
        {
            return Some(message.attachments.as_slice());
        }
        self.events
            .iter()
            .find(|event| event.folder_id == folder_id && event.id == message_id)
            .map(|event| event.attachments.as_slice())
    }

    pub(crate) fn attachment_for_message(
        &self,
        folder_id: u64,
        message_id: u64,
        attach_num: u32,
    ) -> Option<&MapiAttachment> {
        self.attachments_for_message(folder_id, message_id)?
            .iter()
            .find(|attachment| attachment.attach_num == attach_num)
    }

    pub(crate) fn collaboration_folders(&self) -> &[MapiCollaborationFolder] {
        &self.collaboration_folders
    }

    pub(crate) fn public_folders(&self) -> &[MapiPublicFolder] {
        &self.public_folders
    }

    pub(crate) fn public_folder_for_id(&self, folder_id: u64) -> Option<&MapiPublicFolder> {
        self.public_folders
            .iter()
            .find(|folder| folder.id == folder_id)
    }

    pub(crate) fn public_folder_replica_server_names(&self, folder_id: u64) -> Vec<String> {
        self.public_folder_replicas
            .iter()
            .filter(|replica| {
                replica.folder_id == folder_id && replica.replica.lifecycle_state == "active"
            })
            .map(|replica| replica.replica.server_name.clone())
            .collect()
    }

    pub(crate) fn public_folder_items_for_folder(
        &self,
        folder_id: u64,
    ) -> Vec<&MapiPublicFolderItem> {
        self.public_folder_items
            .iter()
            .filter(|item| item.folder_id == folder_id)
            .collect()
    }

    pub(crate) fn public_folder_item_for_id(
        &self,
        folder_id: u64,
        item_id: u64,
    ) -> Option<&MapiPublicFolderItem> {
        self.public_folder_items
            .iter()
            .find(|item| item.folder_id == folder_id && item.id == item_id)
    }

    pub(crate) fn collaboration_folder_for_id(
        &self,
        folder_id: u64,
    ) -> Option<&MapiCollaborationFolder> {
        self.collaboration_folders
            .iter()
            .find(|folder| folder.id == folder_id)
    }

    pub(crate) fn contacts_for_folder(&self, folder_id: u64) -> Vec<&MapiContact> {
        self.contacts
            .iter()
            .filter(|contact| contact.folder_id == folder_id)
            .collect()
    }

    pub(crate) fn contacts_search_results(&self) -> Vec<&MapiContact> {
        if self
            .search_folder_definition_for_role("contacts_search")
            .is_none()
        {
            return Vec::new();
        }
        self.contacts.iter().collect()
    }

    pub(crate) fn contact_for_id(&self, folder_id: u64, item_id: u64) -> Option<&MapiContact> {
        if folder_id == crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID {
            return self
                .contacts_search_results()
                .into_iter()
                .find(|contact| contact.id == item_id);
        }
        self.contacts
            .iter()
            .find(|contact| contact.folder_id == folder_id && contact.id == item_id)
    }

    pub(crate) fn events_for_folder(&self, folder_id: u64) -> Vec<&MapiEvent> {
        self.events
            .iter()
            .filter(|event| event.folder_id == folder_id)
            .collect()
    }

    pub(crate) fn event_for_id(&self, folder_id: u64, item_id: u64) -> Option<&MapiEvent> {
        if folder_id == crate::mapi::identity::REMINDERS_FOLDER_ID {
            return self
                .reminder_events()
                .into_iter()
                .find(|event| mapi_event_id_matches(event, item_id));
        }
        self.events
            .iter()
            .find(|event| event.folder_id == folder_id && mapi_event_id_matches(event, item_id))
    }

    pub(crate) fn tasks_for_folder(&self, folder_id: u64) -> Vec<&MapiTask> {
        self.tasks
            .iter()
            .filter(|task| task.folder_id == folder_id)
            .collect()
    }

    pub(crate) fn todo_search_results(&self) -> Vec<&MapiTask> {
        if self
            .search_folder_definition_for_role("todo_search")
            .is_none()
        {
            return Vec::new();
        }
        self.tasks.iter().collect()
    }

    pub(crate) fn todo_search_messages(&self) -> Vec<&MapiMessage> {
        let Some(definition) = self.search_folder_definition_for_role("todo_search") else {
            return Vec::new();
        };
        self.messages
            .iter()
            .filter(|message| {
                !definition
                    .excluded_folder_roles
                    .iter()
                    .any(|role| role == &message.email.mailbox_role)
            })
            .filter(|message| {
                message.email.followup_icon > 0
                    || message.email.todo_item_flags != 0
                    || matches!(
                        message.email.followup_flag_status.as_str(),
                        "flagged" | "complete"
                    )
            })
            .collect()
    }

    pub(crate) fn todo_search_message_for_id(&self, message_id: u64) -> Option<&MapiMessage> {
        self.todo_search_messages()
            .into_iter()
            .find(|message| message.id == message_id)
    }

    pub(crate) fn tracked_mail_processing_messages(&self) -> Vec<&MapiMessage> {
        let Some(definition) = self.search_folder_definition_for_role("tracked_mail_processing")
        else {
            return Vec::new();
        };
        self.messages
            .iter()
            .filter(|message| {
                !definition
                    .excluded_folder_roles
                    .iter()
                    .any(|role| role == &message.email.mailbox_role)
            })
            .filter(|message| message.email.swapped_todo_store_id.is_some())
            .collect()
    }

    pub(crate) fn tracked_mail_processing_message_for_id(
        &self,
        message_id: u64,
    ) -> Option<&MapiMessage> {
        self.tracked_mail_processing_messages()
            .into_iter()
            .find(|message| message.id == message_id)
    }

    pub(crate) fn reminder_events(&self) -> Vec<&MapiEvent> {
        if self
            .search_folder_definition_for_role("reminders")
            .is_none()
        {
            return Vec::new();
        }
        self.reminders
            .iter()
            .filter(|reminder| reminder.source_type == "calendar")
            .filter_map(|reminder| {
                self.events
                    .iter()
                    .find(|event| event.canonical_id == reminder.source_id)
            })
            .collect()
    }

    pub(crate) fn reminder_tasks(&self) -> Vec<&MapiTask> {
        if self
            .search_folder_definition_for_role("reminders")
            .is_none()
        {
            return Vec::new();
        }
        self.reminders
            .iter()
            .filter(|reminder| reminder.source_type == "task")
            .filter_map(|reminder| {
                self.tasks
                    .iter()
                    .find(|task| task.canonical_id == reminder.source_id)
            })
            .collect()
    }

    pub(crate) fn reminder_messages(&self) -> Vec<&MapiMessage> {
        let Some(definition) = self.search_folder_definition_for_role("reminders") else {
            return Vec::new();
        };
        self.reminders
            .iter()
            .filter(|reminder| reminder.source_type == "mail")
            .filter_map(|reminder| {
                self.messages
                    .iter()
                    .find(|message| message.canonical_id == reminder.source_id)
            })
            .filter(|message| {
                !definition
                    .excluded_folder_roles
                    .iter()
                    .any(|role| role == &message.email.mailbox_role)
            })
            .collect()
    }

    pub(crate) fn reminder_message_for_id(&self, message_id: u64) -> Option<&MapiMessage> {
        self.reminder_messages()
            .into_iter()
            .find(|message| message.id == message_id)
    }

    pub(crate) fn reminder_for_source(
        &self,
        source_type: &str,
        source_id: Uuid,
    ) -> Option<&ClientReminder> {
        self.reminders
            .iter()
            .find(|reminder| reminder.source_type == source_type && reminder.source_id == source_id)
    }

    pub(crate) fn task_for_id(&self, folder_id: u64, item_id: u64) -> Option<&MapiTask> {
        if folder_id == crate::mapi::identity::REMINDERS_FOLDER_ID {
            return self
                .reminder_tasks()
                .into_iter()
                .find(|task| task.id == item_id);
        }
        if folder_id == crate::mapi::identity::TODO_SEARCH_FOLDER_ID {
            return self
                .todo_search_results()
                .into_iter()
                .find(|task| task.id == item_id);
        }
        self.tasks
            .iter()
            .find(|task| task.folder_id == folder_id && task.id == item_id)
    }

    pub(crate) fn notes_for_folder(&self, folder_id: u64) -> Vec<&MapiNote> {
        self.notes
            .iter()
            .filter(|note| note.folder_id == folder_id)
            .collect()
    }

    pub(crate) fn note_for_id(&self, folder_id: u64, item_id: u64) -> Option<&MapiNote> {
        self.notes
            .iter()
            .find(|note| note.folder_id == folder_id && note.id == item_id)
    }

    pub(crate) fn journal_entries_for_folder(&self, folder_id: u64) -> Vec<&MapiJournalEntry> {
        self.journal_entries
            .iter()
            .filter(|entry| entry.folder_id == folder_id)
            .collect()
    }

    pub(crate) fn journal_entry_for_id(
        &self,
        folder_id: u64,
        item_id: u64,
    ) -> Option<&MapiJournalEntry> {
        self.journal_entries
            .iter()
            .find(|entry| entry.folder_id == folder_id && entry.id == item_id)
    }

    pub(crate) fn search_folder_definition_for_role(
        &self,
        role: &str,
    ) -> Option<&SearchFolderDefinition> {
        self.search_folder_definitions
            .iter()
            .find(|definition| definition.role == role)
    }

    pub(crate) fn search_folder_definition_for_folder_id(
        &self,
        folder_id: u64,
    ) -> Option<&SearchFolderDefinition> {
        self.search_folder_definitions
            .iter()
            .find(|definition| {
                crate::mapi::identity::mapped_mapi_object_id(&definition.id) == Some(folder_id)
            })
            .or_else(|| {
                fixed_search_folder_role(folder_id)
                    .and_then(|role| self.search_folder_definition_for_role(role))
            })
    }

    pub(crate) fn user_saved_search_folder_definition_by_display_name(
        &self,
        display_name: &str,
        result_object_kind: &str,
    ) -> Option<&SearchFolderDefinition> {
        let display_name = display_name.trim();
        self.search_folder_definitions.iter().find(|definition| {
            !definition.is_builtin
                && definition.definition_kind == "user_saved"
                && user_saved_search_folder_is_projectable(definition)
                && definition.result_object_kind == result_object_kind
                && definition
                    .display_name
                    .trim()
                    .eq_ignore_ascii_case(display_name)
        })
    }

    pub(crate) fn rules(&self) -> &[MapiRule] {
        &self.rules
    }

    pub(crate) fn navigation_shortcut_messages(&self) -> Vec<MapiNavigationShortcutMessage> {
        deduplicate_navigation_shortcuts(self.navigation_shortcuts.clone())
    }

    pub(crate) fn common_views_table_messages(
        &self,
    ) -> impl Iterator<Item = MapiCommonViewsMessage> {
        let shortcuts = self.navigation_shortcut_messages();
        let mut table_shortcuts = shortcuts
            .into_iter()
            .filter(common_views_table_projects_navigation_shortcut)
            .collect::<Vec<_>>();
        materialize_default_mail_group_header(&mut table_shortcuts);
        for default_shortcut in outlook_common_views_default_navigation_shortcuts() {
            if !table_shortcuts.iter().any(|shortcut| {
                shortcut.target_folder_id == default_shortcut.target_folder_id
                    && shortcut.shortcut_type == default_shortcut.shortcut_type
                    && shortcut.section == default_shortcut.section
                    && shortcut.group_name == default_shortcut.group_name
            }) {
                table_shortcuts.push(default_shortcut);
            }
        }
        let messages = table_shortcuts
            .into_iter()
            .map(MapiCommonViewsMessage::NavigationShortcut)
            .chain(
                outlook_common_views_default_named_views()
                    .into_iter()
                    .map(MapiCommonViewsMessage::NamedView),
            )
            .chain(
                self.search_folder_definitions
                    .clone()
                    .into_iter()
                    .filter(common_views_search_folder_definition_is_projectable)
                    .map(MapiCommonViewsMessage::SearchFolderDefinition),
            )
            .collect::<Vec<_>>();
        messages.into_iter()
    }

    pub(crate) fn common_views_messages(&self) -> impl Iterator<Item = MapiCommonViewsMessage> {
        let messages = self
            .navigation_shortcut_messages()
            .into_iter()
            .map(MapiCommonViewsMessage::NavigationShortcut)
            .collect::<Vec<_>>();
        messages.into_iter()
    }

    pub(crate) fn associated_config_sync_messages_for_folder(
        &self,
        folder_id: u64,
    ) -> Vec<MapiAssociatedConfigMessage> {
        self.associated_config_messages_for_folder(folder_id)
    }

    pub(crate) fn navigation_shortcut_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<MapiNavigationShortcutMessage> {
        self.navigation_shortcut_messages()
            .into_iter()
            .find(|message| message.id == item_id)
    }

    pub(crate) fn navigation_shortcut_table_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<MapiNavigationShortcutMessage> {
        self.navigation_shortcut_message_for_id(item_id)
            .filter(common_views_table_projects_navigation_shortcut)
    }

    pub(crate) fn common_view_named_view_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<MapiCommonViewNamedViewMessage> {
        outlook_common_views_default_named_views()
            .into_iter()
            .find(|message| message.id == item_id)
    }

    pub(crate) fn named_view_message_for_folder_and_id(
        &self,
        folder_id: u64,
        item_id: u64,
    ) -> Option<MapiCommonViewNamedViewMessage> {
        self.common_view_named_view_message_for_id(item_id)
            .or_else(|| self.default_folder_named_view_message(folder_id, item_id))
            .filter(|message| message.folder_id == folder_id)
    }

    pub(crate) fn default_folder_named_view_message(
        &self,
        folder_id: u64,
        item_id: u64,
    ) -> Option<MapiCommonViewNamedViewMessage> {
        is_outlook_default_folder_named_view_id(item_id).then(|| MapiCommonViewNamedViewMessage {
            id: OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
            folder_id,
            canonical_id: Uuid::from_u128(0x6d617069_6664_4e76_8000_000000000001),
            name: outlook_default_folder_named_view_name(folder_id).to_string(),
            view_flags: 14_745_605,
            view_type: 8,
        })
    }

    pub(crate) fn associated_config_messages_for_folder(
        &self,
        folder_id: u64,
    ) -> Vec<MapiAssociatedConfigMessage> {
        let mut messages = self
            .associated_configs
            .iter()
            .filter(|message| message.folder_id == folder_id)
            .cloned()
            .collect::<Vec<_>>();
        if folder_id == crate::mapi::identity::INBOX_FOLDER_ID {
            for default_message in outlook_inbox_associated_config_sync_defaults(folder_id) {
                if !messages
                    .iter()
                    .any(|message| message.message_class == default_message.message_class)
                {
                    messages.push(default_message);
                }
            }
        } else if folder_id == crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID {
            for default_message in outlook_quick_step_associated_config_defaults(folder_id) {
                if !messages
                    .iter()
                    .any(|message| message.message_class == default_message.message_class)
                {
                    messages.push(default_message);
                }
            }
        } else if self.contact_sync_default_supported_folder(folder_id) {
            for default_message in outlook_contact_associated_config_defaults(folder_id) {
                if !messages
                    .iter()
                    .any(|message| message.message_class == default_message.message_class)
                {
                    messages.push(default_message);
                }
            }
        }
        deduplicate_associated_config_messages(messages)
    }

    pub(crate) fn associated_config_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<MapiAssociatedConfigMessage> {
        self.associated_configs
            .iter()
            .find(|message| message.id == item_id)
            .cloned()
            .or_else(|| {
                outlook_inbox_associated_config_sync_defaults(
                    crate::mapi::identity::INBOX_FOLDER_ID,
                )
                .into_iter()
                .find(|message| message.id == item_id)
            })
            .or_else(|| outlook_inbox_exact_virtual_associated_config_for_id(item_id))
            .or_else(|| {
                outlook_quick_step_associated_config_defaults(
                    crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
                )
                .into_iter()
                .find(|message| message.id == item_id)
            })
            .or_else(|| {
                [
                    crate::mapi::identity::CONTACTS_FOLDER_ID,
                    crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
                    crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID,
                    crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID,
                ]
                .into_iter()
                .flat_map(outlook_contact_associated_config_defaults)
                .find(|message| message.id == item_id)
            })
            .or_else(|| {
                self.collaboration_folders
                    .iter()
                    .filter(|folder| folder.kind == MapiCollaborationFolderKind::Contacts)
                    .flat_map(|folder| outlook_contact_associated_config_defaults(folder.id))
                    .find(|message| message.id == item_id)
            })
            .or_else(|| {
                self.folders
                    .iter()
                    .filter(|folder| mailbox_contact_sync_default_supported(&folder.mailbox))
                    .flat_map(|folder| outlook_contact_associated_config_defaults(folder.id))
                    .find(|message| message.id == item_id)
            })
    }

    pub(crate) fn associated_config_message_for_identity_id(
        &self,
        item_id: u64,
    ) -> Option<MapiAssociatedConfigMessage> {
        let canonical_id = self
            .associated_config_identity_ids
            .iter()
            .find(|identity| identity.object_id == item_id)
            .map(|identity| identity.canonical_id)?;
        self.associated_config_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID)
            .into_iter()
            .chain(outlook_quick_step_associated_config_defaults(
                crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
            ))
            .chain(
                [
                    crate::mapi::identity::CONTACTS_FOLDER_ID,
                    crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
                    crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID,
                    crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID,
                ]
                .into_iter()
                .flat_map(outlook_contact_associated_config_defaults),
            )
            .find(|message| message.canonical_id == canonical_id)
    }

    pub(crate) fn associated_config_message_for_folder_and_source_key_id(
        &self,
        folder_id: u64,
        item_id: u64,
    ) -> Option<MapiAssociatedConfigMessage> {
        crate::mapi::identity::global_counter_from_store_id(item_id)?;
        let source_key = crate::mapi::identity::source_key_for_object_id(item_id);
        self.associated_config_messages_for_folder(folder_id)
            .into_iter()
            .find(|message| {
                message.folder_id == folder_id
                    && (message.id == item_id
                        || associated_config_source_key(&message.properties_json)
                            .is_some_and(|message_source_key| message_source_key == source_key))
            })
    }

    pub(crate) fn associated_config_identity_matches_folder(
        &self,
        folder_id: u64,
        item_id: u64,
    ) -> bool {
        self.associated_config_message_for_id(item_id)
            .or_else(|| self.associated_config_message_for_identity_id(item_id))
            .is_some_and(|message| message.folder_id == folder_id)
    }

    fn contact_sync_default_supported_folder(&self, folder_id: u64) -> bool {
        matches!(
            folder_id,
            crate::mapi::identity::CONTACTS_FOLDER_ID
                | crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID
                | crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID
                | crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID
        ) || self.collaboration_folders.iter().any(|folder| {
            folder.kind == MapiCollaborationFolderKind::Contacts && folder.id == folder_id
        }) || self.folders.iter().any(|folder| {
            folder.id == folder_id && mailbox_contact_sync_default_supported(&folder.mailbox)
        })
    }

    pub(crate) fn conversation_action_messages(&self) -> &[MapiConversationActionMessage] {
        &self.conversation_actions
    }

    pub(crate) fn conversation_action_table_messages(&self) -> Vec<MapiConversationActionMessage> {
        if self.conversation_actions.is_empty() {
            vec![outlook_default_conversation_action()]
        } else {
            self.conversation_actions.clone()
        }
    }

    pub(crate) fn conversation_action_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<&MapiConversationActionMessage> {
        self.conversation_actions
            .iter()
            .find(|message| message.id == item_id)
    }

    pub(crate) fn conversation_action_table_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<MapiConversationActionMessage> {
        self.conversation_action_message_for_id(item_id)
            .cloned()
            .or_else(|| {
                (self.conversation_actions.is_empty()
                    && item_id == OUTLOOK_DEFAULT_CONVERSATION_ACTION_ID)
                    .then(outlook_default_conversation_action)
            })
    }

    pub(crate) fn delegate_freebusy_messages(&self) -> &[MapiDelegateFreeBusyMessage] {
        &self.delegate_freebusy_messages
    }

    pub(crate) fn delegate_freebusy_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<&MapiDelegateFreeBusyMessage> {
        self.delegate_freebusy_messages
            .iter()
            .find(|message| message.id == item_id)
    }

    pub(crate) fn recoverable_items_for_folder(
        &self,
        folder_id: u64,
    ) -> Vec<&MapiRecoverableItemMessage> {
        self.recoverable_items
            .iter()
            .filter(|item| item.folder_id == folder_id)
            .collect()
    }

    pub(crate) fn recoverable_item_for_id(
        &self,
        folder_id: u64,
        item_id: u64,
    ) -> Option<&MapiRecoverableItemMessage> {
        self.recoverable_items
            .iter()
            .find(|item| item.folder_id == folder_id && item.id == item_id)
    }

    pub(crate) fn permissions_for_folder(&self, folder_id: u64) -> Vec<MapiFolderPermission> {
        if let Some(folder) = self.folders.iter().find(|folder| folder.id == folder_id) {
            let mut permissions = reserved_permission_rows(folder.canonical_id);
            permissions.extend(
                self.folder_permissions
                    .iter()
                    .filter(|permission| permission.mailbox_id == folder.canonical_id)
                    .cloned(),
            );
            return permissions;
        }
        if let Some(folder) = self
            .public_folders
            .iter()
            .find(|folder| folder.id == folder_id)
        {
            let mut permissions = reserved_permission_rows(folder.folder.id);
            permissions.extend(
                self.public_folder_permissions
                    .iter()
                    .filter(|permission| permission.mailbox_id == folder.folder.id)
                    .cloned(),
            );
            return permissions;
        }
        Vec::new()
    }

    pub(crate) fn folder_access_for_principal(
        &self,
        folder_id: u64,
        principal_account_id: Uuid,
    ) -> Option<MapiFolderAccess> {
        if let Some(folder) = self.folders.iter().find(|folder| folder.id == folder_id) {
            let permission = self.folder_permissions.iter().find(|permission| {
                permission.mailbox_id == folder.canonical_id
                    && permission.member_account_id == Some(principal_account_id)
            })?;
            return Some(access_from_rights(permission.rights));
        }
        let folder = self
            .collaboration_folders
            .iter()
            .find(|folder| folder.id == folder_id)?;
        if folder.collection.owner_account_id == principal_account_id || folder.collection.is_owned
        {
            return Some(access_from_rights(rights_from_grant(
                true, true, true, true,
            )));
        }
        Some(access_from_rights(rights_from_grant(
            folder.collection.rights.may_read,
            folder.collection.rights.may_write,
            folder.collection.rights.may_delete,
            folder.collection.rights.may_share,
        )))
    }

    #[cfg(test)]
    pub(crate) fn folders(&self) -> &[MapiFolder] {
        &self.folders
    }

    #[cfg(test)]
    pub(crate) fn messages(&self) -> &[MapiMessage] {
        &self.messages
    }
}

fn fixed_search_folder_role(folder_id: u64) -> Option<&'static str> {
    match folder_id {
        crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID => Some("contacts_search"),
        crate::mapi::identity::TODO_SEARCH_FOLDER_ID => Some("todo_search"),
        crate::mapi::identity::REMINDERS_FOLDER_ID => Some("reminders"),
        crate::mapi::identity::TRACKED_MAIL_PROCESSING_FOLDER_ID => Some("tracked_mail_processing"),
        _ => None,
    }
}

fn associated_config_source_key(properties_json: &serde_json::Value) -> Option<Vec<u8>> {
    let value = properties_json
        .get("0x65e00102")
        .filter(|value| value.get("type").and_then(serde_json::Value::as_str) == Some("binary"))?
        .get("value")?
        .as_str()?;
    hex_to_bytes(value)
}

fn hex_to_bytes(value: &str) -> Option<Vec<u8>> {
    if value.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for chunk in value.as_bytes().chunks(2) {
        let high = hex_digit(chunk[0])?;
        let low = hex_digit(chunk[1])?;
        bytes.push((high << 4) | low);
    }
    Some(bytes)
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn mapi_search_folder_definition_to_folder(
    definition: &SearchFolderDefinition,
) -> Option<MapiFolder> {
    if definition.is_builtin || definition.definition_kind != "user_saved" {
        return None;
    }
    let id = crate::mapi::identity::mapped_mapi_object_id(&definition.id)?;
    Some(MapiFolder {
        id,
        canonical_id: definition.id,
        mailbox: JmapMailbox {
            id: definition.id,
            parent_id: None,
            role: mapi_search_folder_role(&definition.result_object_kind).to_string(),
            name: definition.display_name.clone(),
            sort_order: i32::MAX,
            modseq: crate::mapi::identity::global_counter_from_store_id(id).unwrap_or(1),
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        },
    })
}

fn user_saved_search_folder_projection_key(definition: &SearchFolderDefinition) -> String {
    format!(
        "{}\x1f{}",
        definition.display_name.trim().to_ascii_lowercase(),
        definition.result_object_kind
    )
}

fn user_saved_search_folder_is_projectable(definition: &SearchFolderDefinition) -> bool {
    if definition.is_builtin || definition.definition_kind != "user_saved" {
        return true;
    }
    if definition
        .restriction_json
        .get("kind")
        .and_then(serde_json::Value::as_str)
        != Some("mapi_bounded")
    {
        return true;
    }
    definition
        .restriction_json
        .get("all")
        .and_then(serde_json::Value::as_array)
        .map(|clauses| !clauses.is_empty())
        .unwrap_or(true)
}

fn mapi_search_folder_role(result_object_kind: &str) -> &'static str {
    match result_object_kind {
        "contact" => "__mapi_search_folder_contact",
        "task" => "__mapi_search_folder_task",
        "mixed" => "__mapi_search_folder_mixed",
        _ => "__mapi_search_folder_message",
    }
}

pub(crate) trait MapiStore: ExchangeStore {
    fn load_mapi_mail_store<'a>(
        &'a self,
        account_id: Uuid,
        message_limit: u64,
    ) -> StoreFuture<'a, MapiMailStoreSnapshot>;
}

impl<T: ExchangeStore> MapiStore for T {
    fn load_mapi_mail_store<'a>(
        &'a self,
        account_id: Uuid,
        message_limit: u64,
    ) -> StoreFuture<'a, MapiMailStoreSnapshot> {
        Box::pin(async move {
            let mailboxes = self.ensure_jmap_system_mailboxes(account_id).await?;
            let mut message_ids = self.fetch_all_jmap_email_ids(account_id).await?;
            message_ids.truncate(message_limit.min(usize::MAX as u64) as usize);
            let emails = self.fetch_jmap_emails(account_id, &message_ids).await?;
            let mut attachments = Vec::with_capacity(emails.len());
            for email in &emails {
                let message_attachments =
                    self.fetch_message_attachments(account_id, email.id).await?;
                attachments.push((email.id, message_attachments));
            }
            let contact_collections = self
                .fetch_accessible_contact_collections(account_id)
                .await?;
            let calendar_collections = self
                .fetch_accessible_calendar_collections(account_id)
                .await?;
            let task_collections = self.fetch_accessible_task_collections(account_id).await?;
            let mut contacts = Vec::new();
            for collection in &contact_collections {
                contacts.extend(
                    self.fetch_accessible_contacts_in_collection(account_id, &collection.id)
                        .await?,
                );
            }
            let mut events = Vec::new();
            if calendar_collections.is_empty() {
                events.extend(
                    self.fetch_accessible_events_in_collection(account_id, "default")
                        .await
                        .unwrap_or_default(),
                );
            } else {
                for collection in &calendar_collections {
                    events.extend(
                        self.fetch_accessible_events_in_collection(account_id, &collection.id)
                            .await?,
                    );
                }
            }
            let mut tasks = Vec::new();
            for collection in &task_collections {
                tasks.extend(
                    self.fetch_accessible_tasks_in_collection(account_id, &collection.id)
                        .await?,
                );
            }
            let notes = self.fetch_mapi_notes(account_id).await?;
            let journal_entries = self.fetch_mapi_journal_entries(account_id).await?;
            let search_folder_definitions = self.fetch_search_folders(account_id).await?;
            let rules = self.list_mailbox_rules(account_id).await?;
            let navigation_shortcuts = self.fetch_mapi_navigation_shortcuts(account_id).await?;
            let mut associated_configs = self.fetch_mapi_associated_configs(account_id).await?;
            let dropped_empty_synthetic_inbox_configs = associated_configs
                .iter()
                .filter(|config| is_empty_synthetic_inbox_associated_config(config))
                .count();
            if dropped_empty_synthetic_inbox_configs > 0 {
                associated_configs
                    .retain(|config| !is_empty_synthetic_inbox_associated_config(config));
                tracing::debug!(
                rca_debug = true,
                adapter = "mapi",
                        account_id = %account_id,
                        folder_id = crate::mapi::identity::INBOX_FOLDER_ID,
                        dropped_empty_synthetic_inbox_configs,
                        "rca debug mapi dropped empty synthetic inbox associated configs"
                    );
            }
            let dropped_empty_named_view_configs = associated_configs
                .iter()
                .filter(|config| is_empty_outlook_inbox_named_view_placeholder(config))
                .count();
            if dropped_empty_named_view_configs > 0 {
                associated_configs
                    .retain(|config| !is_empty_outlook_inbox_named_view_placeholder(config));
                tracing::debug!(
                    rca_debug = true,
                    adapter = "mapi",
                    account_id = %account_id,
                    folder_id = crate::mapi::identity::INBOX_FOLDER_ID,
                    dropped_empty_named_view_configs,
                    message_class = OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS,
                    "rca debug mapi dropped empty inbox named view associated config"
                );
            }
            let dropped_empty_rule_organizer_configs = associated_configs
                .iter()
                .filter(|config| is_empty_outlook_rule_organizer_placeholder(config))
                .count();
            if dropped_empty_rule_organizer_configs > 0 {
                associated_configs
                    .retain(|config| !is_empty_outlook_rule_organizer_placeholder(config));
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    account_id = %account_id,
                    folder_id = crate::mapi::identity::INBOX_FOLDER_ID,
                    dropped_empty_rule_organizer_configs,
                    message_class = OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS,
                    stream_property = OUTLOOK_RULE_ORGANIZER_BINARY_6802_JSON_KEY,
                    "rca debug mapi dropped empty inbox rule organizer associated config"
                );
            }
            log_outlook_inbox_associated_config_bootstrap(
                account_id,
                &associated_configs,
                &[],
                &[],
            );
            let conversation_actions = self.fetch_conversation_actions(account_id).await?;
            let delegate_freebusy_messages =
                self.fetch_delegate_freebusy_messages(account_id).await?;
            let public_trees = self.fetch_public_folder_trees(account_id).await?;
            let mut public_folders = Vec::new();
            let mut pending_public_folder_ids = public_trees
                .iter()
                .filter_map(|tree| tree.root_folder_id)
                .collect::<Vec<_>>();
            while let Some(folder_id) = pending_public_folder_ids.pop() {
                let folder = self.fetch_public_folder(account_id, folder_id).await?;
                pending_public_folder_ids.extend(
                    self.fetch_public_folder_children(account_id, folder_id)
                        .await?
                        .into_iter()
                        .map(|child| child.id),
                );
                public_folders.push(folder);
            }
            let mut public_folder_items = Vec::new();
            let mut public_folder_permissions = Vec::new();
            let mut public_folder_replicas = Vec::new();
            for folder in &public_folders {
                public_folder_items.extend(
                    self.fetch_public_folder_items(account_id, folder.id)
                        .await?,
                );
                public_folder_permissions.extend(
                    self.fetch_public_folder_permissions(account_id, folder.id)
                        .await?,
                );
                public_folder_replicas.extend(
                    self.fetch_public_folder_replicas(account_id, folder.id)
                        .await?,
                );
            }
            let mut recoverable_items = Vec::new();
            for folder in ["deletions", "versions", "purges"] {
                recoverable_items.extend(
                    self.list_recoverable_items(account_id, Some(folder))
                        .await?,
                );
            }
            let reminders = self
                .query_client_reminders(
                    account_id,
                    ReminderQuery {
                        include_inactive: false,
                    },
                )
                .await?;
            let identity_requests = mapi_identity_requests(
                &mailboxes,
                &emails,
                &contact_collections,
                &calendar_collections,
                &task_collections,
                &contacts,
                &events,
                &tasks,
                &notes,
                &journal_entries,
                &search_folder_definitions,
                &rules,
                &navigation_shortcuts,
                &associated_configs,
                &conversation_actions,
                &delegate_freebusy_messages,
                &public_folders,
                &public_folder_items,
            );
            for identity in self
                .fetch_or_allocate_mapi_identities(account_id, &identity_requests)
                .await?
            {
                crate::mapi::identity::remember_mapi_identity_with_source_key(
                    identity.canonical_id,
                    identity.object_id,
                    Some(identity.source_key),
                );
            }
            let mailbox_ids = mailboxes
                .iter()
                .map(|mailbox| mailbox.id)
                .collect::<Vec<_>>();
            let folder_permissions = self
                .fetch_mapi_folder_permissions(account_id, &mailbox_ids)
                .await?;
            Ok(MapiMailStoreSnapshot::new(
                mailboxes,
                emails,
                attachments,
                contact_collections,
                calendar_collections,
                task_collections,
                contacts,
                events,
                tasks,
                folder_permissions,
            ))
            .map(|snapshot| snapshot.with_notes_and_journal(notes, journal_entries))
            .map(|snapshot| snapshot.with_search_folder_definitions(search_folder_definitions))
            .map(|snapshot| snapshot.with_rules(rules))
            .map(|snapshot| snapshot.with_navigation_shortcuts(navigation_shortcuts))
            .map(|snapshot| snapshot.with_associated_configs(associated_configs))
            .map(|snapshot| snapshot.with_conversation_actions(conversation_actions))
            .map(|snapshot| snapshot.with_delegate_freebusy_messages(delegate_freebusy_messages))
            .map(|snapshot| snapshot.with_recoverable_items(recoverable_items))
            .map(|snapshot| snapshot.with_reminders(reminders))
            .map(|snapshot| {
                snapshot.with_public_folders(
                    public_folders,
                    public_folder_items,
                    public_folder_permissions,
                )
            })
            .map(|snapshot| snapshot.with_public_folder_replicas(public_folder_replicas))
        })
    }
}

fn mapi_public_folder_permission(permission: PublicFolderPermission) -> MapiFolderPermission {
    MapiFolderPermission {
        mailbox_id: permission.public_folder_id,
        member_account_id: Some(permission.principal_account_id),
        member_name: permission.principal_display_name,
        rights: rights_from_grant(
            permission.rights.may_read,
            permission.rights.may_write,
            permission.rights.may_delete,
            permission.rights.may_share,
        ),
    }
}

fn mapi_identity_requests(
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    contact_collections: &[CollaborationCollection],
    calendar_collections: &[CollaborationCollection],
    task_collections: &[CollaborationCollection],
    contacts: &[AccessibleContact],
    events: &[AccessibleEvent],
    tasks: &[ClientTask],
    notes: &[ClientNote],
    journal_entries: &[JournalEntry],
    search_folder_definitions: &[SearchFolderDefinition],
    rules: &[MailboxRule],
    navigation_shortcuts: &[MapiNavigationShortcutRecord],
    associated_configs: &[MapiAssociatedConfigRecord],
    conversation_actions: &[ConversationAction],
    delegate_freebusy_messages: &[DelegateFreeBusyMessageObject],
    public_folders: &[PublicFolder],
    public_folder_items: &[PublicFolderItem],
) -> Vec<MapiIdentityRequest> {
    let mut requests = Vec::new();
    requests.extend(
        mailboxes
            .iter()
            .filter(|mailbox| !is_virtual_special_mailbox(mailbox))
            .map(|mailbox| MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::Mailbox,
                canonical_id: mailbox.id,
                reserved_global_counter: reserved_folder_counter_for_role(&mailbox.role),
                source_key: None,
            }),
    );
    requests.extend(collaboration_folder_identity_requests(
        contact_collections,
        calendar_collections,
        task_collections,
    ));
    requests.extend(emails.iter().map(|email| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::Message,
        canonical_id: email.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(contacts.iter().map(|contact| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::Contact,
        canonical_id: contact.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(events.iter().map(|event| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::CalendarEvent,
        canonical_id: event.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(tasks.iter().map(|task| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::Task,
        canonical_id: task.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(notes.iter().map(|note| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::Note,
        canonical_id: note.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(journal_entries.iter().map(|entry| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::JournalEntry,
        canonical_id: entry.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(
        search_folder_definitions
            .iter()
            .map(|definition| MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::SearchFolderDefinition,
                canonical_id: definition.id,
                reserved_global_counter: None,
                source_key: None,
            }),
    );
    requests.extend(rules.iter().map(|rule| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::Rule,
        canonical_id: rule.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(
        conversation_actions
            .iter()
            .map(|action| MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::ConversationAction,
                canonical_id: action.id,
                reserved_global_counter: None,
                source_key: None,
            }),
    );
    requests.extend(
        navigation_shortcuts
            .iter()
            .map(|shortcut| MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::NavigationShortcut,
                canonical_id: shortcut.id,
                reserved_global_counter: None,
                source_key: None,
            }),
    );
    requests.extend(associated_configs.iter().map(|config| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::AssociatedConfig,
        canonical_id: config.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(
        delegate_freebusy_messages
            .iter()
            .map(|message| MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::DelegateFreeBusyMessage,
                canonical_id: message.id,
                reserved_global_counter: None,
                source_key: None,
            }),
    );
    requests.extend(public_folders.iter().map(|folder| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::PublicFolder,
        canonical_id: folder.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(public_folder_items.iter().map(|item| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::PublicFolderItem,
        canonical_id: item.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests
}

pub(crate) fn collaboration_folder_identity_requests(
    contact_collections: &[CollaborationCollection],
    calendar_collections: &[CollaborationCollection],
    task_collections: &[CollaborationCollection],
) -> Vec<MapiIdentityRequest> {
    let mut requests = contact_collections
        .iter()
        .filter_map(|collection| {
            collaboration_folder_identity_canonical_id(
                MapiCollaborationFolderKind::Contacts,
                collection,
            )
        })
        .chain(calendar_collections.iter().filter_map(|collection| {
            collaboration_folder_identity_canonical_id(
                MapiCollaborationFolderKind::Calendar,
                collection,
            )
        }))
        .chain(task_collections.iter().filter_map(|collection| {
            collaboration_folder_identity_canonical_id(
                MapiCollaborationFolderKind::Task,
                collection,
            )
        }))
        .map(|canonical_id| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Mailbox,
            canonical_id,
            reserved_global_counter: None,
            source_key: None,
        })
        .collect::<Vec<_>>();

    if calendar_collections
        .iter()
        .any(|collection| matches!(collection.id.as_str(), "default" | "calendar"))
    {
        let mailbox = crate::mapi_mailstore::virtual_special_mailbox(
            crate::mapi::identity::CALENDAR_FOLDER_ID,
        )
        .expect("default Calendar virtual mailbox");
        requests.push(MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Mailbox,
            canonical_id: mailbox.id,
            reserved_global_counter: Some(crate::mapi::identity::CALENDAR_FOLDER_COUNTER),
            source_key: None,
        });
    }

    requests
}

pub(crate) fn is_virtual_special_mailbox(mailbox: &JmapMailbox) -> bool {
    reserved_folder_counter_for_role(&mailbox.role).is_some_and(|counter| {
        crate::mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::mapi_store_id(
            counter,
        ))
        .is_some_and(|virtual_mailbox| virtual_mailbox.id == mailbox.id)
    })
}

fn mapi_message_folder_id(email: &JmapEmail, folders: &[MapiFolder]) -> u64 {
    folders
        .iter()
        .find(|folder| folder.canonical_id == email.mailbox_id)
        .map(|folder| folder.id)
        .unwrap_or_else(|| mapi_folder_id_for_role(&email.mailbox_role))
}

fn mapi_folder_id(mailbox: &JmapMailbox) -> u64 {
    reserved_folder_id_for_role(&mailbox.role)
        .or_else(|| crate::mapi::identity::mapped_mapi_object_id(&mailbox.id))
        .expect("MAPI folder identity mapping missing")
}

fn mapi_folder_id_for_role(role: &str) -> u64 {
    reserved_folder_id_for_role(role).unwrap_or(crate::mapi::identity::ROOT_FOLDER_ID)
}

fn mapi_message_id(email: &JmapEmail) -> u64 {
    mapi_item_id(&email.id)
}

fn mapi_item_id(id: &Uuid) -> u64 {
    crate::mapi::identity::mapped_mapi_object_id(id).expect("MAPI item identity mapping missing")
}

fn mapi_event_id_matches(event: &MapiEvent, object_id: u64) -> bool {
    event.id == object_id
        || crate::mapi::identity::object_id_matches(&event.canonical_id, object_id)
}

fn mapi_public_folder_id(folder: &PublicFolder) -> u64 {
    crate::mapi::identity::mapped_mapi_object_id(&folder.id)
        .expect("MAPI public folder identity mapping missing")
}

pub(crate) fn mapi_recoverable_item_id(id: &Uuid) -> u64 {
    crate::mapi::identity::legacy_migration_object_id(id)
}

pub(crate) fn recoverable_mapi_folder_id(folder: &str) -> Option<u64> {
    match folder {
        "deletions" => Some(crate::mapi::identity::RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID),
        "versions" => Some(crate::mapi::identity::RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID),
        "purges" => Some(crate::mapi::identity::RECOVERABLE_ITEMS_PURGES_FOLDER_ID),
        _ => None,
    }
}

pub(crate) fn recoverable_storage_folder(folder_id: u64) -> Option<&'static str> {
    match folder_id {
        crate::mapi::identity::RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID => Some("deletions"),
        crate::mapi::identity::RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID => Some("versions"),
        crate::mapi::identity::RECOVERABLE_ITEMS_PURGES_FOLDER_ID => Some("purges"),
        _ => None,
    }
}

fn mapi_collaboration_folder_id(
    kind: MapiCollaborationFolderKind,
    collection: &CollaborationCollection,
) -> u64 {
    match (kind, collection.id.as_str()) {
        (MapiCollaborationFolderKind::Contacts, "default" | "contacts") => {
            crate::mapi::identity::CONTACTS_FOLDER_ID
        }
        (MapiCollaborationFolderKind::Contacts, "suggested_contacts") => {
            crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID
        }
        (MapiCollaborationFolderKind::Contacts, "quick_contacts") => {
            crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID
        }
        (MapiCollaborationFolderKind::Contacts, "im_contact_list") => {
            crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID
        }
        (MapiCollaborationFolderKind::Calendar, "default" | "calendar") => {
            crate::mapi::identity::CALENDAR_FOLDER_ID
        }
        (MapiCollaborationFolderKind::Task, "default" | "tasks") => {
            crate::mapi::identity::TASKS_FOLDER_ID
        }
        _ => collaboration_folder_identity_canonical_id(kind, collection)
            .and_then(|id| crate::mapi::identity::mapped_mapi_object_id(&id))
            .expect("MAPI collaboration folder identity mapping missing"),
    }
}

pub(crate) fn collaboration_folder_identity_canonical_id(
    kind: MapiCollaborationFolderKind,
    collection: &CollaborationCollection,
) -> Option<Uuid> {
    collaboration_collection_identity_key(kind, &collection.id)
}

fn collaboration_collection_identity_key(
    kind: MapiCollaborationFolderKind,
    collection_id: &str,
) -> Option<Uuid> {
    match (kind, collection_id) {
        (MapiCollaborationFolderKind::Contacts, "default" | "contacts")
        | (MapiCollaborationFolderKind::Contacts, "suggested_contacts")
        | (MapiCollaborationFolderKind::Contacts, "quick_contacts")
        | (MapiCollaborationFolderKind::Contacts, "im_contact_list")
        | (MapiCollaborationFolderKind::Calendar, "default" | "calendar")
        | (MapiCollaborationFolderKind::Task, "default" | "tasks") => None,
        _ => Some(deterministic_collaboration_folder_uuid(kind, collection_id)),
    }
}

fn deterministic_collaboration_folder_uuid(
    kind: MapiCollaborationFolderKind,
    collection_id: &str,
) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"lpe:mapi:collaboration-folder:v1");
    hash.update([0]);
    hash.update(match kind {
        MapiCollaborationFolderKind::Contacts => b"contacts".as_slice(),
        MapiCollaborationFolderKind::Calendar => b"calendar".as_slice(),
        MapiCollaborationFolderKind::Task => b"task".as_slice(),
    });
    hash.update([0]);
    hash.update(collection_id.as_bytes());
    let digest = hash.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn task_collection_matches(task: &ClientTask, collection_id: &str) -> bool {
    matches!(collection_id, "tasks" | "default") || task.task_list_id.to_string() == collection_id
}

fn mailbox_contact_sync_default_supported(mailbox: &JmapMailbox) -> bool {
    if matches!(
        mailbox.role.as_str(),
        "contacts" | "suggested_contacts" | "quick_contacts" | "im_contact_list"
    ) {
        return true;
    }
    if mailbox.parent_id.is_some() {
        return false;
    }
    matches!(
        mailbox.name.trim().to_ascii_lowercase().as_str(),
        "contacts" | "suggested contacts" | "quick contacts" | "im contact list"
    )
}

pub(crate) fn reserved_folder_counter_for_role(role: &str) -> Option<u64> {
    match role {
        "__mapi_deferred_action" => Some(crate::mapi::identity::DEFERRED_ACTION_FOLDER_COUNTER),
        "__mapi_spooler_queue" => Some(crate::mapi::identity::SPOOLER_QUEUE_FOLDER_COUNTER),
        "__mapi_ipm_subtree" => Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_COUNTER),
        "inbox" => Some(crate::mapi::identity::INBOX_FOLDER_COUNTER),
        "outbox" => Some(crate::mapi::identity::OUTBOX_FOLDER_COUNTER),
        "drafts" => Some(crate::mapi::identity::DRAFTS_FOLDER_COUNTER),
        "sent" => Some(crate::mapi::identity::SENT_FOLDER_COUNTER),
        "trash" => Some(crate::mapi::identity::TRASH_FOLDER_COUNTER),
        "__mapi_common_views" => Some(crate::mapi::identity::COMMON_VIEWS_FOLDER_COUNTER),
        "__mapi_schedule" => Some(crate::mapi::identity::SCHEDULE_FOLDER_COUNTER),
        "__mapi_search" => Some(crate::mapi::identity::SEARCH_FOLDER_COUNTER),
        "__mapi_views" => Some(crate::mapi::identity::VIEWS_FOLDER_COUNTER),
        "__mapi_shortcuts" => Some(crate::mapi::identity::SHORTCUTS_FOLDER_COUNTER),
        "contacts" => Some(crate::mapi::identity::CONTACTS_FOLDER_COUNTER),
        "suggested_contacts" => Some(crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_COUNTER),
        "quick_contacts" => Some(crate::mapi::identity::QUICK_CONTACTS_FOLDER_COUNTER),
        "im_contact_list" => Some(crate::mapi::identity::IM_CONTACT_LIST_FOLDER_COUNTER),
        "contacts_search" => Some(crate::mapi::identity::CONTACTS_SEARCH_FOLDER_COUNTER),
        "document_libraries" => Some(crate::mapi::identity::DOCUMENT_LIBRARIES_FOLDER_COUNTER),
        "calendar" => Some(crate::mapi::identity::CALENDAR_FOLDER_COUNTER),
        "journal" => Some(crate::mapi::identity::JOURNAL_FOLDER_COUNTER),
        "notes" => Some(crate::mapi::identity::NOTES_FOLDER_COUNTER),
        "tasks" => Some(crate::mapi::identity::TASKS_FOLDER_COUNTER),
        "reminders" => Some(crate::mapi::identity::REMINDERS_FOLDER_COUNTER),
        "sync_issues" => Some(crate::mapi::identity::SYNC_ISSUES_FOLDER_COUNTER),
        "conflicts" => Some(crate::mapi::identity::CONFLICTS_FOLDER_COUNTER),
        "local_failures" => Some(crate::mapi::identity::LOCAL_FAILURES_FOLDER_COUNTER),
        "server_failures" => Some(crate::mapi::identity::SERVER_FAILURES_FOLDER_COUNTER),
        "junk" => Some(crate::mapi::identity::JUNK_FOLDER_COUNTER),
        "rss_feeds" => Some(crate::mapi::identity::RSS_FEEDS_FOLDER_COUNTER),
        "tracked_mail_processing" => {
            Some(crate::mapi::identity::TRACKED_MAIL_PROCESSING_FOLDER_COUNTER)
        }
        "todo_search" => Some(crate::mapi::identity::TODO_SEARCH_FOLDER_COUNTER),
        "conversation_action_settings" => {
            Some(crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_COUNTER)
        }
        "conversation_actions" => {
            Some(crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_COUNTER)
        }
        "archive" => Some(crate::mapi::identity::ARCHIVE_FOLDER_COUNTER),
        "__mapi_freebusy_data" => Some(crate::mapi::identity::FREEBUSY_DATA_FOLDER_COUNTER),
        "freebusy_data" => Some(crate::mapi::identity::FREEBUSY_DATA_FOLDER_COUNTER),
        "conversation_history" => Some(crate::mapi::identity::CONVERSATION_HISTORY_FOLDER_COUNTER),
        _ => None,
    }
}

fn reserved_folder_id_for_role(role: &str) -> Option<u64> {
    reserved_folder_counter_for_role(role).map(crate::mapi::identity::mapi_store_id)
}

fn deduplicate_navigation_shortcuts(
    shortcuts: Vec<MapiNavigationShortcutMessage>,
) -> Vec<MapiNavigationShortcutMessage> {
    let mut seen_links = HashSet::new();
    let mut seen_headers = HashSet::new();
    shortcuts
        .into_iter()
        .filter(|shortcut| {
            if let Some(target_folder_id) = shortcut.target_folder_id {
                seen_links.insert((target_folder_id, shortcut.shortcut_type, shortcut.section))
            } else {
                seen_headers.insert((
                    shortcut.subject.clone(),
                    shortcut.shortcut_type,
                    shortcut.section,
                    shortcut.group_header_id,
                    shortcut.group_name.clone(),
                ))
            }
        })
        .collect()
}

fn common_views_table_projects_navigation_shortcut(
    shortcut: &MapiNavigationShortcutMessage,
) -> bool {
    if shortcut.section != 1 || shortcut.group_name != OUTLOOK_MAIL_FAVORITES_GROUP_NAME {
        return false;
    }
    if shortcut.shortcut_type == 4 {
        return true;
    }
    matches!(
        shortcut.target_folder_id,
        Some(crate::mapi::identity::INBOX_FOLDER_ID)
            | Some(crate::mapi::identity::SENT_FOLDER_ID)
            | Some(crate::mapi::identity::TRASH_FOLDER_ID)
    )
}

fn normalize_navigation_shortcut_group_name(
    section: u32,
    group_header_id: Option<Uuid>,
    group_name: &str,
) -> String {
    if section == 1
        && group_header_id == Some(crate::mapi::properties::default_wlink_group_uuid())
        && (group_name.trim().is_empty()
            || group_name.eq_ignore_ascii_case("Mail")
            || group_name.eq_ignore_ascii_case(OUTLOOK_MAIL_FAVORITES_GROUP_NAME))
    {
        OUTLOOK_MAIL_FAVORITES_GROUP_NAME.to_string()
    } else {
        group_name.to_string()
    }
}

fn is_synthetic_common_views_group_header(shortcut: &MapiNavigationShortcutRecord) -> bool {
    let outlook_calendar_group_id =
        Uuid::parse_str("b7f00600-0000-0000-c000-000000000046").expect("valid Outlook GUID");
    shortcut.target_folder_id.is_none()
        && shortcut.shortcut_type == 4
        && shortcut.subject == "My Calendars"
        && shortcut.group_name == "My Calendars"
        && shortcut.group_header_id == Some(outlook_calendar_group_id)
}

fn materialize_default_mail_group_header(shortcuts: &mut Vec<MapiNavigationShortcutMessage>) {
    let default_group_id = crate::mapi::properties::default_wlink_group_uuid();
    let has_default_mail_link = shortcuts.iter().any(|shortcut| {
        shortcut.shortcut_type != 4
            && shortcut.section == 1
            && shortcut.group_header_id == Some(default_group_id)
            && shortcut.group_name == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
    });
    let has_default_mail_header = shortcuts.iter().any(|shortcut| {
        shortcut.shortcut_type == 4
            && shortcut.section == 1
            && shortcut.group_header_id == Some(default_group_id)
            && shortcut.group_name == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
    });

    if has_default_mail_link && !has_default_mail_header {
        if let Some(header) = outlook_common_views_default_navigation_shortcuts()
            .into_iter()
            .find(|shortcut| {
                shortcut.shortcut_type == 4
                    && shortcut.group_name == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
            })
        {
            shortcuts.push(header);
        }
    }
}

fn format_navigation_shortcut_debug_summary(shortcuts: &[MapiNavigationShortcutMessage]) -> String {
    shortcuts
        .iter()
        .take(8)
        .map(format_navigation_shortcut_debug_entry)
        .collect::<Vec<_>>()
        .join("|")
}

fn format_common_views_table_shortcut_debug_summary(messages: &[MapiCommonViewsMessage]) -> String {
    messages
        .iter()
        .filter_map(|message| match message {
            MapiCommonViewsMessage::NavigationShortcut(shortcut) => Some(shortcut),
            MapiCommonViewsMessage::NamedView(_)
            | MapiCommonViewsMessage::SearchFolderDefinition(_) => None,
        })
        .take(8)
        .map(format_navigation_shortcut_debug_entry)
        .collect::<Vec<_>>()
        .join("|")
}

fn common_views_search_folder_definition_is_projectable(
    definition: &SearchFolderDefinition,
) -> bool {
    definition
        .restriction_json
        .get("pidTagSearchFolderDefinition")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| BASE64_STANDARD.decode(value).ok())
        .is_some_and(|blob| search_folder_definition_blob_has_required_blocks(&blob))
}

fn search_folder_definition_blob_has_required_blocks(blob: &[u8]) -> bool {
    if blob.len() < 30 || blob.get(0..4) != Some(&0x0000_1004u32.to_le_bytes()) {
        return false;
    }
    let storage_type = u32::from_le_bytes(blob[4..8].try_into().unwrap());
    let mut offset = 12usize;
    let Some(text_len) = blob.get(offset).copied() else {
        return false;
    };
    offset += 1;
    let text_len = if text_len == u8::MAX {
        let Some(bytes) = blob.get(offset..offset + 2) else {
            return false;
        };
        offset += 2;
        u16::from_le_bytes(bytes.try_into().unwrap()) as usize
    } else {
        text_len as usize
    };
    offset = match offset.checked_add(text_len) {
        Some(offset) if offset <= blob.len() => offset,
        _ => return false,
    };
    if blob.get(offset..offset + 9).is_none() {
        return false;
    }
    offset += 8;
    let folder_list_1_len = blob[offset] as usize;
    offset += 1 + folder_list_1_len;
    let Some(bytes) = blob.get(offset..offset + 4) else {
        return false;
    };
    let folder_list_2_len = u32::from_le_bytes(bytes.try_into().unwrap()) as usize;
    offset += 4;
    if storage_type & 0x40 != 0 && folder_list_2_len == 0 {
        return false;
    }
    offset = match offset.checked_add(folder_list_2_len) {
        Some(offset) if offset <= blob.len() => offset,
        _ => return false,
    };
    if blob.get(offset..offset + 4).is_none() {
        return false;
    }
    offset += 4;
    if storage_type & 0x08 != 0 && blob.len().saturating_sub(offset) <= 4 {
        return false;
    }
    blob.get(blob.len().saturating_sub(4)..) == Some(&0u32.to_le_bytes())
}

fn format_navigation_shortcut_debug_entry(shortcut: &MapiNavigationShortcutMessage) -> String {
    format!(
        "id=0x{:016x};canonical_id={};subject={};target={};type={};flags=0x{:08x};section={};ordinal={};group_header={};group_name={}",
        shortcut.id,
        shortcut.canonical_id,
        shortcut.subject,
        shortcut
            .target_folder_id
            .map(|target| format!("0x{target:016x}"))
            .unwrap_or_else(|| "none".to_string()),
        shortcut.shortcut_type,
        shortcut.flags,
        shortcut.section,
        shortcut.ordinal,
        shortcut
            .group_header_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string()),
        shortcut.group_name,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapi::properties::default_wlink_group_uuid;
    use lpe_storage::{
        AccessibleContact, CollaborationCollection, CollaborationRights, JmapEmailAddress,
        JmapEmailMailboxState,
    };

    fn exchange_builtin_excluded_folder_roles() -> Vec<String> {
        [
            "trash",
            "junk",
            "drafts",
            "outbox",
            "conflicts",
            "local_failures",
            "server_failures",
            "sync_issues",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    }

    fn test_mailbox(id: Uuid) -> JmapMailbox {
        JmapMailbox {
            id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 3,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        }
    }

    fn test_email(id: Uuid, mailbox_id: Uuid, subject: &str) -> JmapEmail {
        JmapEmail {
            id,
            thread_id: Uuid::from_u128(0x12121212_1212_4212_8212_121212121212),
            mailbox_id,
            mailbox_role: "inbox".to_string(),
            mailbox_name: "Inbox".to_string(),
            modseq: 2,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![JmapEmailMailboxState {
                mailbox_id,
                role: "inbox".to_string(),
                name: "Inbox".to_string(),
                modseq: 2,
                unread: false,
                flagged: false,
                followup_flag_status: "none".to_string(),
                followup_icon: 0,
                todo_item_flags: 0,
                followup_request: String::new(),
                followup_start_at: None,
                followup_due_at: None,
                followup_completed_at: None,
                reminder_set: false,
                reminder_at: None,
                reminder_dismissed_at: None,
                swapped_todo_store_id: None,
                swapped_todo_data: None,
                categories: Vec::new(),
                draft: false,
            }],
            received_at: "2026-05-20T12:00:00Z".to_string(),
            sent_at: None,
            from_address: "alice@example.test".to_string(),
            from_display: Some("Alice".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: Uuid::nil(),
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: subject.to_string(),
            preview: subject.to_string(),
            body_text: subject.to_string(),
            body_html_sanitized: None,
            unread: false,
            flagged: false,
            followup_flag_status: "none".to_string(),
            followup_icon: 0,
            todo_item_flags: 0,
            followup_request: String::new(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            has_attachments: false,
            size_octets: 42,
            internet_message_id: None,
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        }
    }

    #[test]
    fn content_table_window_emails_reuses_wider_window_slice() {
        let mailbox_id = Uuid::from_u128(0x44444444_4444_4444_8444_444444444444);
        let first_id = Uuid::from_u128(0x55555555_5555_4555_8555_555555555555);
        let second_id = Uuid::from_u128(0x66666666_6666_4666_8666_666666666666);
        let third_id = Uuid::from_u128(0x77777777_7777_4777_8777_777777777777);
        crate::mapi::identity::remember_mapi_identity(
            mailbox_id,
            crate::mapi::identity::INBOX_FOLDER_ID,
        );
        crate::mapi::identity::remember_mapi_identity(
            first_id,
            crate::mapi::identity::mapi_store_id(101),
        );
        crate::mapi::identity::remember_mapi_identity(
            second_id,
            crate::mapi::identity::mapi_store_id(102),
        );
        crate::mapi::identity::remember_mapi_identity(
            third_id,
            crate::mapi::identity::mapi_store_id(103),
        );
        let snapshot = MapiMailStoreSnapshot::new(
            vec![test_mailbox(mailbox_id)],
            vec![
                test_email(first_id, mailbox_id, "First"),
                test_email(second_id, mailbox_id, "Second"),
                test_email(third_id, mailbox_id, "Third"),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_content_windows(vec![MapiContentTableWindow {
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            view_signature: 42,
            offset: 0,
            total: 3,
            message_ids: vec![first_id, second_id, third_id],
        }]);

        let (total, emails) = snapshot
            .content_table_window_emails(crate::mapi::identity::INBOX_FOLDER_ID, 42, 1, 2)
            .expect("wider window should satisfy subrange");

        assert_eq!(total, 3);
        assert_eq!(emails.len(), 2);
        assert_eq!(emails[0].subject, "Second");
        assert_eq!(emails[1].subject, "Third");
    }

    #[test]
    fn content_table_window_emails_skips_insufficient_containing_window() {
        let mailbox_id = Uuid::from_u128(0x44444444_4444_4444_8444_444444444445);
        let first_id = Uuid::from_u128(0x55555555_5555_4555_8555_555555555556);
        let second_id = Uuid::from_u128(0x66666666_6666_4666_8666_666666666667);
        let third_id = Uuid::from_u128(0x77777777_7777_4777_8777_777777777778);
        crate::mapi::identity::remember_mapi_identity(
            mailbox_id,
            crate::mapi::identity::INBOX_FOLDER_ID,
        );
        crate::mapi::identity::remember_mapi_identity(
            first_id,
            crate::mapi::identity::mapi_store_id(104),
        );
        crate::mapi::identity::remember_mapi_identity(
            second_id,
            crate::mapi::identity::mapi_store_id(105),
        );
        crate::mapi::identity::remember_mapi_identity(
            third_id,
            crate::mapi::identity::mapi_store_id(106),
        );
        let snapshot = MapiMailStoreSnapshot::new(
            vec![test_mailbox(mailbox_id)],
            vec![
                test_email(first_id, mailbox_id, "First"),
                test_email(second_id, mailbox_id, "Second"),
                test_email(third_id, mailbox_id, "Third"),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_content_windows(vec![
            MapiContentTableWindow {
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                view_signature: 42,
                offset: 0,
                total: 4,
                message_ids: vec![first_id, second_id],
            },
            MapiContentTableWindow {
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                view_signature: 42,
                offset: 1,
                total: 4,
                message_ids: vec![second_id, third_id],
            },
        ]);

        let (total, emails) = snapshot
            .content_table_window_emails(crate::mapi::identity::INBOX_FOLDER_ID, 42, 1, 2)
            .expect("later sufficient window should satisfy subrange");

        assert_eq!(total, 4);
        assert_eq!(emails.len(), 2);
        assert_eq!(emails[0].subject, "Second");
        assert_eq!(emails[1].subject, "Third");
    }

    #[test]
    fn content_table_window_emails_containing_skips_incomplete_window() {
        let mailbox_id = Uuid::from_u128(0x44444444_4444_4444_8444_444444444446);
        let first_id = Uuid::from_u128(0x55555555_5555_4555_8555_555555555557);
        let second_id = Uuid::from_u128(0x66666666_6666_4666_8666_666666666668);
        let third_id = Uuid::from_u128(0x77777777_7777_4777_8777_777777777779);
        let missing_id = Uuid::from_u128(0x88888888_8888_4888_8888_888888888889);
        crate::mapi::identity::remember_mapi_identity(
            mailbox_id,
            crate::mapi::identity::INBOX_FOLDER_ID,
        );
        crate::mapi::identity::remember_mapi_identity(
            first_id,
            crate::mapi::identity::mapi_store_id(107),
        );
        crate::mapi::identity::remember_mapi_identity(
            second_id,
            crate::mapi::identity::mapi_store_id(108),
        );
        crate::mapi::identity::remember_mapi_identity(
            third_id,
            crate::mapi::identity::mapi_store_id(109),
        );
        let snapshot = MapiMailStoreSnapshot::new(
            vec![test_mailbox(mailbox_id)],
            vec![
                test_email(first_id, mailbox_id, "First"),
                test_email(second_id, mailbox_id, "Second"),
                test_email(third_id, mailbox_id, "Third"),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_content_windows(vec![
            MapiContentTableWindow {
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                view_signature: 42,
                offset: 0,
                total: 4,
                message_ids: vec![first_id, missing_id],
            },
            MapiContentTableWindow {
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                view_signature: 42,
                offset: 1,
                total: 4,
                message_ids: vec![second_id, third_id],
            },
        ]);

        let (offset, total, emails) = snapshot
            .content_table_window_emails_containing(crate::mapi::identity::INBOX_FOLDER_ID, 42, 1)
            .expect("later complete window should satisfy position");

        assert_eq!(offset, 1);
        assert_eq!(total, 4);
        assert_eq!(emails.len(), 2);
        assert_eq!(emails[0].subject, "Second");
        assert_eq!(emails[1].subject, "Third");
    }

    #[test]
    fn content_table_window_emails_containing_prefers_boundary_window() {
        let mailbox_id = Uuid::from_u128(0x44444444_4444_4444_8444_444444448888);
        let first_id = Uuid::from_u128(0x55555555_5555_4555_8555_555555558888);
        let second_id = Uuid::from_u128(0x66666666_6666_4666_8666_666666668888);
        let third_id = Uuid::from_u128(0x77777777_7777_4777_8777_777777778888);
        crate::mapi::identity::remember_mapi_identity(
            mailbox_id,
            crate::mapi::identity::INBOX_FOLDER_ID,
        );
        crate::mapi::identity::remember_mapi_identity(
            first_id,
            crate::mapi::identity::mapi_store_id(111),
        );
        crate::mapi::identity::remember_mapi_identity(
            second_id,
            crate::mapi::identity::mapi_store_id(112),
        );
        crate::mapi::identity::remember_mapi_identity(
            third_id,
            crate::mapi::identity::mapi_store_id(113),
        );
        let snapshot = MapiMailStoreSnapshot::new(
            vec![test_mailbox(mailbox_id)],
            vec![
                test_email(first_id, mailbox_id, "First"),
                test_email(second_id, mailbox_id, "Second"),
                test_email(third_id, mailbox_id, "Third"),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_content_windows(vec![
            MapiContentTableWindow {
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                view_signature: 42,
                offset: 0,
                total: 3,
                message_ids: vec![first_id, second_id],
            },
            MapiContentTableWindow {
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                view_signature: 42,
                offset: 2,
                total: 3,
                message_ids: vec![third_id],
            },
        ]);

        let (offset, total, emails) = snapshot
            .content_table_window_emails_containing(crate::mapi::identity::INBOX_FOLDER_ID, 42, 2)
            .expect("boundary window should satisfy position");

        assert_eq!(offset, 2);
        assert_eq!(total, 3);
        assert_eq!(emails.len(), 1);
        assert_eq!(emails[0].subject, "Third");
    }

    #[test]
    fn content_table_window_emails_containing_prefers_longer_tail_window() {
        let mailbox_id = Uuid::from_u128(0x44444444_4444_4444_8444_444444449999);
        let first_id = Uuid::from_u128(0x55555555_5555_4555_8555_555555559999);
        let second_id = Uuid::from_u128(0x66666666_6666_4666_8666_666666669999);
        let third_id = Uuid::from_u128(0x77777777_7777_4777_8777_777777779999);
        crate::mapi::identity::remember_mapi_identity(
            mailbox_id,
            crate::mapi::identity::INBOX_FOLDER_ID,
        );
        crate::mapi::identity::remember_mapi_identity(
            first_id,
            crate::mapi::identity::mapi_store_id(114),
        );
        crate::mapi::identity::remember_mapi_identity(
            second_id,
            crate::mapi::identity::mapi_store_id(115),
        );
        crate::mapi::identity::remember_mapi_identity(
            third_id,
            crate::mapi::identity::mapi_store_id(116),
        );
        let snapshot = MapiMailStoreSnapshot::new(
            vec![test_mailbox(mailbox_id)],
            vec![
                test_email(first_id, mailbox_id, "First"),
                test_email(second_id, mailbox_id, "Second"),
                test_email(third_id, mailbox_id, "Third"),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_content_windows(vec![
            MapiContentTableWindow {
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                view_signature: 42,
                offset: 0,
                total: 3,
                message_ids: vec![first_id, second_id, third_id],
            },
            MapiContentTableWindow {
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                view_signature: 42,
                offset: 1,
                total: 3,
                message_ids: vec![second_id],
            },
        ]);

        let (offset, total, emails) = snapshot
            .content_table_window_emails_containing(crate::mapi::identity::INBOX_FOLDER_ID, 42, 1)
            .expect("longer complete window should satisfy position");

        assert_eq!(offset, 0);
        assert_eq!(total, 3);
        assert_eq!(emails.len(), 3);
        assert_eq!(emails[1].subject, "Second");
        assert_eq!(emails[2].subject, "Third");
    }

    #[test]
    fn content_table_total_survives_total_only_window_without_rows() {
        let mailbox_id = Uuid::from_u128(0x44444444_4444_4444_8444_444444444447);
        crate::mapi::identity::remember_mapi_identity(
            mailbox_id,
            crate::mapi::identity::INBOX_FOLDER_ID,
        );
        let snapshot = MapiMailStoreSnapshot::new(
            vec![test_mailbox(mailbox_id)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_content_windows(vec![MapiContentTableWindow {
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            view_signature: 42,
            offset: 0,
            total: 2,
            message_ids: Vec::new(),
        }]);

        assert_eq!(
            snapshot.content_table_total(crate::mapi::identity::INBOX_FOLDER_ID, 42),
            Some(2)
        );
    }

    #[test]
    fn advertised_special_mailbox_roles_have_reserved_mapi_counters() {
        let cases = [
            (
                "sync_issues",
                crate::mapi::identity::SYNC_ISSUES_FOLDER_COUNTER,
            ),
            ("conflicts", crate::mapi::identity::CONFLICTS_FOLDER_COUNTER),
            (
                "local_failures",
                crate::mapi::identity::LOCAL_FAILURES_FOLDER_COUNTER,
            ),
            (
                "server_failures",
                crate::mapi::identity::SERVER_FAILURES_FOLDER_COUNTER,
            ),
            ("junk", crate::mapi::identity::JUNK_FOLDER_COUNTER),
            ("rss_feeds", crate::mapi::identity::RSS_FEEDS_FOLDER_COUNTER),
            ("archive", crate::mapi::identity::ARCHIVE_FOLDER_COUNTER),
            (
                "conversation_history",
                crate::mapi::identity::CONVERSATION_HISTORY_FOLDER_COUNTER,
            ),
        ];

        for (role, counter) in cases {
            assert_eq!(reserved_folder_counter_for_role(role), Some(counter));
        }
    }

    #[test]
    fn inbox_associated_configs_do_not_emit_synthetic_defaults() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let messages =
            snapshot.associated_config_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID);

        assert!(messages.is_empty());
        for suppressed_id in [
            OUTLOOK_INBOX_ACCOUNT_PREFS_CONFIG_ID,
            OUTLOOK_INBOX_EAS_CONFIG_ID,
            OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_ID,
            OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_ID,
        ] {
            assert!(snapshot
                .associated_config_message_for_id(suppressed_id)
                .is_none());
            assert!(!snapshot.associated_config_identity_matches_folder(
                crate::mapi::identity::INBOX_FOLDER_ID,
                suppressed_id
            ));
        }
        for exact_virtual_id in [
            OUTLOOK_INBOX_ELC_CONFIG_ID,
            OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_ID,
            OUTLOOK_INBOX_SHARING_CONFIGURATION_ID,
            OUTLOOK_INBOX_SHARING_INDEX_ID,
            OUTLOOK_INBOX_AGGREGATION_ID,
        ] {
            let message = snapshot
                .associated_config_message_for_id(exact_virtual_id)
                .expect("exact virtual Inbox FAI row should open by MID");
            assert_eq!(message.id, exact_virtual_id);
            assert!(snapshot.associated_config_identity_matches_folder(
                crate::mapi::identity::INBOX_FOLDER_ID,
                exact_virtual_id
            ));
        }
        assert!(snapshot
            .associated_config_message_for_id(OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_ID)
            .is_none());
        assert!(!snapshot.associated_config_identity_matches_folder(
            crate::mapi::identity::INBOX_FOLDER_ID,
            OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_ID
        ));

        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let persisted_id = Uuid::from_u128(0x6d617069_6561_7343_8000_000000000002);
        let duplicate_id = Uuid::from_u128(0x6d617069_6561_7343_8000_000000000003);
        crate::mapi::identity::remember_mapi_identity(
            persisted_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 71,
            ),
        );
        crate::mapi::identity::remember_mapi_identity(
            duplicate_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 72,
            ),
        );
        let persisted = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: persisted_id,
                account_id,
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                message_class: OUTLOOK_INBOX_EAS_CONFIG_CLASS.to_string(),
                subject: "Client EAS config".to_string(),
                properties_json: serde_json::json!({}),
            },
            crate::store::MapiAssociatedConfigRecord {
                id: duplicate_id,
                account_id,
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                message_class: OUTLOOK_INBOX_EAS_CONFIG_CLASS.to_string(),
                subject: "Duplicate EAS config".to_string(),
                properties_json: serde_json::json!({}),
            },
        ]);

        let persisted_messages =
            persisted.associated_config_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID);
        assert_eq!(
            persisted_messages
                .iter()
                .filter(|message| message.message_class == OUTLOOK_INBOX_EAS_CONFIG_CLASS)
                .count(),
            1
        );
        assert_eq!(
            persisted_messages
                .iter()
                .find(|message| message.message_class == OUTLOOK_INBOX_EAS_CONFIG_CLASS)
                .map(|message| message.subject.as_str()),
            Some("Client EAS config")
        );
    }

    #[test]
    fn inbox_associated_config_bootstrap_persists_no_outlook_defaults() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        log_outlook_inbox_associated_config_bootstrap(account_id, &[], &[], &[]);
    }

    #[test]
    fn empty_inbox_compact_named_view_placeholder_is_suppressed() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let stale_id = Uuid::from_u128(0x6d617069_696e_4e76_8000_000000000077);
        crate::mapi::identity::remember_mapi_identity(
            stale_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 77,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: stale_id,
                account_id,
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                message_class: OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS.to_string(),
                subject: "Compact".to_string(),
                properties_json: serde_json::json!({}),
            },
        ]);

        let messages =
            snapshot.associated_config_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID);
        assert!(messages
            .iter()
            .all(|message| { message.message_class != OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS }));
        assert!(!snapshot.associated_config_identity_matches_folder(
            crate::mapi::identity::INBOX_FOLDER_ID,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 77,
            )
        ));
    }

    #[test]
    fn associated_config_sync_messages_use_persisted_rows_before_narrow_defaults() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let persisted_umolk_id = Uuid::from_u128(0x6d617069_756d_6f6c_8000_000000000002);
        let persisted_named_view_id = Uuid::from_u128(0x6d617069_696e_4e76_8000_000000000002);
        let persisted_account_prefs_id = Uuid::from_u128(0x6d617069_6163_6350_8000_000000000002);
        for (offset, id) in [
            persisted_umolk_id,
            persisted_named_view_id,
            persisted_account_prefs_id,
        ]
        .into_iter()
        .enumerate()
        {
            crate::mapi::identity::remember_mapi_identity(
                id,
                crate::mapi::identity::mapi_store_id(
                    crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 132 + offset as u64,
                ),
            );
        }
        let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: persisted_umolk_id,
                account_id,
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                message_class: OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS.to_string(),
                subject: "Persisted UMOLK".to_string(),
                properties_json: serde_json::json!({}),
            },
            crate::store::MapiAssociatedConfigRecord {
                id: persisted_named_view_id,
                account_id,
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                message_class: OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS.to_string(),
                subject: "Persisted Compact".to_string(),
                properties_json: serde_json::json!({}),
            },
            crate::store::MapiAssociatedConfigRecord {
                id: persisted_account_prefs_id,
                account_id,
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                message_class: OUTLOOK_INBOX_ACCOUNT_PREFS_CONFIG_CLASS.to_string(),
                subject: "Persisted AccountPrefs".to_string(),
                properties_json: serde_json::json!({}),
            },
        ]);

        let table_messages =
            snapshot.associated_config_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID);
        let sync_messages = snapshot
            .associated_config_sync_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID);

        assert_eq!(sync_messages, table_messages);
        assert_eq!(
            sync_messages
                .iter()
                .find(|message| {
                    message.message_class == OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS
                })
                .map(|message| message.canonical_id),
            Some(persisted_umolk_id)
        );
        assert_eq!(
            sync_messages
                .iter()
                .find(|message| message.message_class == OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS)
                .map(|message| message.canonical_id),
            Some(persisted_named_view_id)
        );
        assert_eq!(
            sync_messages
                .iter()
                .find(|message| message.message_class == OUTLOOK_INBOX_ACCOUNT_PREFS_CONFIG_CLASS)
                .map(|message| message.canonical_id),
            Some(persisted_account_prefs_id)
        );
        assert!(!sync_messages
            .iter()
            .any(|message| is_outlook_inbox_virtual_only_associated_config_id(message.id)));
    }

    #[test]
    fn empty_rule_organizer_placeholder_is_not_modeled_state() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let empty = crate::store::MapiAssociatedConfigRecord {
            id: Uuid::from_u128(0x6d617069_7275_6c65_8000_000000000001),
            account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS.to_string(),
            subject: OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS.to_string(),
            properties_json: serde_json::json!({}),
        };
        let non_empty = crate::store::MapiAssociatedConfigRecord {
            properties_json: serde_json::json!({
                OUTLOOK_RULE_ORGANIZER_BINARY_6802_JSON_KEY: {
                    "type": "binary",
                    "value": "0102"
                }
            }),
            ..empty.clone()
        };

        assert!(is_empty_outlook_rule_organizer_placeholder(&empty));
        assert!(!is_empty_outlook_rule_organizer_placeholder(&non_empty));
    }

    #[test]
    fn associated_configs_drop_transient_outlook_migration_markers() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let kept_id = Uuid::from_u128(0x6d617069_6b65_6570_8000_000000000001);
        let dropped_id = Uuid::from_u128(0x6d617069_6472_6f70_8000_000000000001);
        crate::mapi::identity::remember_mapi_identity(
            kept_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 81,
            ),
        );
        crate::mapi::identity::remember_mapi_identity(
            dropped_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 82,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: kept_id,
                account_id,
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                message_class: OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_CLASS.to_string(),
                subject: OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_CLASS.to_string(),
                properties_json: serde_json::json!({}),
            },
            crate::store::MapiAssociatedConfigRecord {
                id: dropped_id,
                account_id,
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                message_class: "IPM.Microsoft.PendingChange.MigrateFlags".to_string(),
                subject: "IPM.Microsoft.PendingChange.MigrateFlags".to_string(),
                properties_json: serde_json::json!({}),
            },
        ]);

        let messages =
            snapshot.associated_config_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID);

        assert!(messages.iter().any(|message| {
            message.message_class == OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_CLASS
        }));
        assert!(!messages
            .iter()
            .any(|message| message.message_class == "IPM.Microsoft.PendingChange.MigrateFlags"));
    }

    #[test]
    fn quick_step_settings_include_default_custom_action_without_duplicate() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let messages = snapshot.associated_config_messages_for_folder(
            crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
        );

        assert_eq!(
            messages
                .iter()
                .filter(|message| message.message_class == OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS)
                .count(),
            1
        );
        assert_eq!(
            snapshot
                .associated_config_message_for_id(OUTLOOK_QUICK_STEP_CUSTOM_ACTION_ID)
                .map(|message| message.message_class),
            Some(OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS.to_string())
        );
        assert_eq!(
            snapshot
                .associated_config_message_for_folder_and_source_key_id(
                    crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
                    OUTLOOK_QUICK_STEP_CUSTOM_ACTION_ID,
                )
                .map(|message| message.message_class),
            Some(OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS.to_string())
        );
        assert!(snapshot.associated_config_identity_matches_folder(
            crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
            OUTLOOK_QUICK_STEP_CUSTOM_ACTION_ID
        ));
        assert!(!snapshot.associated_config_identity_matches_folder(
            crate::mapi::identity::INBOX_FOLDER_ID,
            OUTLOOK_QUICK_STEP_CUSTOM_ACTION_ID
        ));
        assert!(is_outlook_quick_step_default_associated_config_id(
            OUTLOOK_QUICK_STEP_CUSTOM_ACTION_ID
        ));

        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let persisted_id = Uuid::from_u128(0x6d617069_7173_4361_8000_000000000002);
        crate::mapi::identity::remember_mapi_identity(
            persisted_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 171,
            ),
        );
        let persisted = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: persisted_id,
                account_id,
                folder_id: crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
                message_class: OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS.to_string(),
                subject: "Client custom action".to_string(),
                properties_json: serde_json::json!({}),
            },
        ]);

        let persisted_messages = persisted.associated_config_messages_for_folder(
            crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
        );
        assert_eq!(
            persisted_messages
                .iter()
                .filter(|message| message.message_class == OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS)
                .count(),
            1
        );
        assert_eq!(
            persisted_messages
                .iter()
                .find(|message| message.message_class == OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS)
                .map(|message| message.subject.as_str()),
            Some("Client custom action")
        );
    }

    #[test]
    fn contacts_include_default_osc_contact_sync_without_duplicate() {
        let snapshot = MapiMailStoreSnapshot::empty();

        for (folder_id, sync_message_id, timestamp_message_id) in [(
            crate::mapi::identity::CONTACTS_FOLDER_ID,
            OUTLOOK_CONTACTS_OSC_CONTACT_SYNC_ID,
            OUTLOOK_CONTACTS_CONTACT_LINK_TIMESTAMP_ID,
        )] {
            let messages = snapshot.associated_config_messages_for_folder(folder_id);
            assert_eq!(
                messages
                    .iter()
                    .filter(|message| message.message_class == OUTLOOK_CONTACT_SYNC_CONFIG_CLASS)
                    .count(),
                1
            );
            assert_eq!(
                messages
                    .iter()
                    .filter(|message| {
                        message.message_class == OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS
                    })
                    .count(),
                1
            );
            assert_eq!(
                snapshot
                    .associated_config_message_for_id(sync_message_id)
                    .map(|message| message.message_class),
                Some(OUTLOOK_CONTACT_SYNC_CONFIG_CLASS.to_string())
            );
            assert_eq!(
                snapshot
                    .associated_config_message_for_id(timestamp_message_id)
                    .map(|message| message.message_class),
                Some(OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS.to_string())
            );
            assert_eq!(
                snapshot
                    .associated_config_message_for_folder_and_source_key_id(
                        folder_id,
                        timestamp_message_id
                    )
                    .map(|message| message.message_class),
                Some(OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS.to_string())
            );
            assert!(snapshot.associated_config_identity_matches_folder(folder_id, sync_message_id));
            assert!(
                snapshot.associated_config_identity_matches_folder(folder_id, timestamp_message_id)
            );
            assert!(!snapshot.associated_config_identity_matches_folder(
                crate::mapi::identity::INBOX_FOLDER_ID,
                timestamp_message_id
            ));
        }

        let messages = snapshot.associated_config_messages_for_folder(
            crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
        );
        assert_eq!(
            messages
                .iter()
                .filter(|message| message.message_class == OUTLOOK_CONTACT_SYNC_CONFIG_CLASS)
                .count(),
            0
        );
        assert_eq!(
            messages
                .iter()
                .filter(|message| {
                    message.message_class == OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS
                })
                .count(),
            1
        );
        assert_eq!(
            snapshot
                .associated_config_message_for_id(OUTLOOK_SUGGESTED_CONTACTS_OSC_CONTACT_SYNC_ID)
                .map(|message| message.message_class),
            None
        );
        assert_eq!(
            snapshot
                .associated_config_message_for_id(
                    OUTLOOK_SUGGESTED_CONTACTS_CONTACT_LINK_TIMESTAMP_ID
                )
                .map(|message| message.message_class),
            Some(OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS.to_string())
        );
    }

    #[test]
    fn dynamic_contact_folder_includes_default_osc_contact_sync() {
        let folder_id = crate::mapi::identity::mapi_store_id(0x4e);
        let collection = CollaborationCollection {
            id: "outlook-log-dynamic-contacts".to_string(),
            kind: "contacts".to_string(),
            display_name: "Contacts".to_string(),
            owner_account_id: Uuid::from_u128(0x4e),
            owner_email: "owner@example.test".to_string(),
            owner_display_name: "Owner".to_string(),
            is_owned: true,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
        };
        crate::mapi::identity::remember_mapi_identity(
            collaboration_folder_identity_canonical_id(
                MapiCollaborationFolderKind::Contacts,
                &collection,
            )
            .unwrap(),
            folder_id,
        );
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![collection],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        let message_id = outlook_dynamic_contact_sync_config_id(folder_id).unwrap();
        let timestamp_message_id =
            outlook_dynamic_contact_link_timestamp_config_id(folder_id).unwrap();
        let messages = snapshot.associated_config_messages_for_folder(folder_id);

        assert_eq!(
            messages
                .iter()
                .filter(|message| message.message_class == OUTLOOK_CONTACT_SYNC_CONFIG_CLASS)
                .count(),
            1
        );
        assert_eq!(
            messages
                .iter()
                .filter(
                    |message| message.message_class == OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS
                )
                .count(),
            1
        );
        assert_eq!(
            snapshot
                .associated_config_message_for_id(message_id)
                .map(|message| (message.folder_id, message.message_class)),
            Some((folder_id, OUTLOOK_CONTACT_SYNC_CONFIG_CLASS.to_string()))
        );
        assert_eq!(
            snapshot
                .associated_config_message_for_id(timestamp_message_id)
                .map(|message| (message.folder_id, message.message_class)),
            Some((
                folder_id,
                OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS.to_string()
            ))
        );
        assert_eq!(
            snapshot
                .associated_config_message_for_folder_and_source_key_id(folder_id, message_id)
                .map(|message| message.message_class),
            Some(OUTLOOK_CONTACT_SYNC_CONFIG_CLASS.to_string())
        );
        assert!(snapshot.associated_config_identity_matches_folder(folder_id, message_id));
        assert!(snapshot.associated_config_identity_matches_folder(folder_id, timestamp_message_id));
        assert!(!snapshot.associated_config_identity_matches_folder(
            crate::mapi::identity::INBOX_FOLDER_ID,
            timestamp_message_id
        ));
        assert!(is_outlook_contact_default_associated_config_id(message_id));
        assert!(is_outlook_contact_default_associated_config_id(
            timestamp_message_id
        ));
    }

    #[test]
    fn mailbox_backed_contact_folder_includes_default_osc_contact_sync() {
        let folder_id = crate::mapi::identity::mapi_store_id(0x55);
        let mailbox_id = Uuid::parse_str("aaaaaaaa-7777-4111-8111-aaaaaaaaaaaa").unwrap();
        crate::mapi::identity::remember_mapi_identity(mailbox_id, folder_id);
        let snapshot = MapiMailStoreSnapshot::new(
            vec![JmapMailbox {
                id: mailbox_id,
                parent_id: None,
                role: String::new(),
                name: "Quick Contacts".to_string(),
                sort_order: 0,
                modseq: 1,
                total_emails: 0,
                unread_emails: 0,
                size_octets: 0,
                is_subscribed: true,
            }],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        let message_id = outlook_dynamic_contact_sync_config_id(folder_id).unwrap();
        let messages = snapshot.associated_config_messages_for_folder(folder_id);

        assert_eq!(
            messages
                .iter()
                .filter(|message| message.message_class == OUTLOOK_CONTACT_SYNC_CONFIG_CLASS)
                .count(),
            1
        );
        assert_eq!(
            snapshot
                .associated_config_message_for_id(message_id)
                .map(|message| (message.folder_id, message.message_class)),
            Some((folder_id, OUTLOOK_CONTACT_SYNC_CONFIG_CLASS.to_string()))
        );
        assert!(snapshot.associated_config_identity_matches_folder(folder_id, message_id));
    }

    #[test]
    fn mailbox_backed_suggested_contacts_includes_default_osc_contact_sync() {
        let folder_id = crate::mapi::identity::mapi_store_id(0x54);
        let mailbox_id = Uuid::parse_str("aaaaaaaa-7777-4111-8111-aaaaaaaaaaab").unwrap();
        crate::mapi::identity::remember_mapi_identity(mailbox_id, folder_id);
        let snapshot = MapiMailStoreSnapshot::new(
            vec![JmapMailbox {
                id: mailbox_id,
                parent_id: None,
                role: String::new(),
                name: "Suggested Contacts".to_string(),
                sort_order: 0,
                modseq: 1,
                total_emails: 0,
                unread_emails: 0,
                size_octets: 0,
                is_subscribed: true,
            }],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        let message_id = outlook_dynamic_contact_sync_config_id(folder_id).unwrap();

        assert_eq!(
            snapshot
                .associated_config_message_for_id(message_id)
                .map(|message| (message.folder_id, message.message_class)),
            Some((folder_id, OUTLOOK_CONTACT_SYNC_CONFIG_CLASS.to_string()))
        );
        assert!(snapshot.associated_config_identity_matches_folder(folder_id, message_id));
    }

    #[test]
    fn associated_config_identity_only_placeholder_does_not_open_without_backing_message() {
        let object_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 901,
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_associated_config_identity_ids(vec![
            MapiAssociatedConfigIdentity {
                canonical_id: Uuid::from_u128(0xaabbccdd_0000_0000_0000_000000000001),
                object_id,
            },
        ]);

        assert!(!snapshot.associated_config_identity_matches_folder(
            crate::mapi::identity::INBOX_FOLDER_ID,
            object_id
        ));
        assert!(!snapshot.associated_config_identity_matches_folder(
            crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
            object_id
        ));
    }

    #[test]
    fn modeled_virtual_associated_config_identity_opens_via_dynamic_id() {
        let object_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 902,
        );
        let canonical_id = Uuid::from_u128(0x6d617069_756d_6f6c_8000_000000000902);
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        crate::mapi::identity::remember_mapi_identity(canonical_id, object_id);
        let snapshot = MapiMailStoreSnapshot::empty()
            .with_associated_config_identity_ids(vec![MapiAssociatedConfigIdentity {
                canonical_id,
                object_id,
            }])
            .with_associated_configs(vec![crate::store::MapiAssociatedConfigRecord {
                id: canonical_id,
                account_id,
                folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
                message_class: OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS.to_string(),
                subject: "Persisted UMOLK".to_string(),
                properties_json: serde_json::json!({
                    "0x7c060003": {"type": "u32", "value": 4}
                }),
            }]);

        assert_eq!(
            snapshot
                .associated_config_message_for_identity_id(object_id)
                .map(|message| (message.folder_id, message.message_class)),
            Some((
                crate::mapi::identity::INBOX_FOLDER_ID,
                OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS.to_string()
            ))
        );
        assert!(snapshot.associated_config_identity_matches_folder(
            crate::mapi::identity::INBOX_FOLDER_ID,
            object_id
        ));
    }

    #[test]
    fn empty_conversation_action_settings_exposes_default_table_row_only() {
        let snapshot = MapiMailStoreSnapshot::empty();

        assert!(snapshot.conversation_action_messages().is_empty());

        let messages = snapshot.conversation_action_table_messages();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, OUTLOOK_DEFAULT_CONVERSATION_ACTION_ID);
        assert_eq!(
            messages[0].folder_id,
            crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID
        );
        assert_eq!(messages[0].action.subject, "IPM.ConversationAction");
        assert_eq!(
            snapshot
                .conversation_action_table_message_for_id(OUTLOOK_DEFAULT_CONVERSATION_ACTION_ID)
                .map(|message| message.action.subject),
            Some("IPM.ConversationAction".to_string())
        );
        assert!(snapshot
            .conversation_action_message_for_id(OUTLOOK_DEFAULT_CONVERSATION_ACTION_ID)
            .is_none());
    }

    #[test]
    fn common_views_projects_default_named_views_for_table_only() {
        let snapshot = MapiMailStoreSnapshot::empty();
        assert!(snapshot.navigation_shortcut_messages().is_empty());
        assert_eq!(snapshot.common_views_messages().count(), 0);
        let messages = snapshot.common_views_table_messages().collect::<Vec<_>>();

        assert_eq!(messages.len(), 6);
        let default_header = messages
            .iter()
            .find_map(|message| match message {
                MapiCommonViewsMessage::NavigationShortcut(shortcut)
                    if shortcut.subject == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
                        && shortcut.shortcut_type == 4 =>
                {
                    Some(shortcut)
                }
                _ => None,
            })
            .expect("default mail navigation group header");
        assert_eq!(default_header.target_folder_id, None);
        assert_eq!(default_header.group_name, OUTLOOK_MAIL_FAVORITES_GROUP_NAME);
        assert_eq!(
            default_header.group_header_id,
            Some(default_wlink_group_uuid())
        );
        let default_shortcut = messages
            .iter()
            .find_map(|message| match message {
                MapiCommonViewsMessage::NavigationShortcut(shortcut)
                    if shortcut.id == OUTLOOK_COMMON_VIEWS_DEFAULT_NAVIGATION_SHORTCUT_ID =>
                {
                    Some(shortcut)
                }
                _ => None,
            })
            .expect("default inbox navigation shortcut");
        assert_eq!(default_shortcut.subject, "Inbox");
        assert_eq!(
            default_shortcut.target_folder_id,
            Some(crate::mapi::identity::INBOX_FOLDER_ID)
        );
        assert_eq!(
            default_shortcut.group_name,
            OUTLOOK_MAIL_FAVORITES_GROUP_NAME
        );
        for (subject, target_folder_id) in [
            ("Sent", crate::mapi::identity::SENT_FOLDER_ID),
            ("Trash", crate::mapi::identity::TRASH_FOLDER_ID),
        ] {
            let shortcut = messages
                .iter()
                .find_map(|message| match message {
                    MapiCommonViewsMessage::NavigationShortcut(shortcut)
                        if shortcut.subject == subject =>
                    {
                        Some(shortcut)
                    }
                    _ => None,
                })
                .expect("default mail navigation shortcut");
            assert_eq!(shortcut.target_folder_id, Some(target_folder_id));
            assert_eq!(shortcut.group_name, OUTLOOK_MAIL_FAVORITES_GROUP_NAME);
        }
        let named_views = messages
            .iter()
            .filter_map(|message| match message {
                MapiCommonViewsMessage::NamedView(view) => Some(view),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(named_views.len(), 2);
        assert!(named_views
            .iter()
            .any(|view| view.name == "Compact" && view.view_flags == 14_745_605));
        assert!(named_views
            .iter()
            .any(|view| view.name == "Sent To" && view.view_flags == 15_269_893));
        assert!(named_views.iter().all(|view| view.view_type == 8));
        assert!(snapshot
            .navigation_shortcut_table_message_for_id(0)
            .is_none());
        assert!(snapshot
            .navigation_shortcut_table_message_for_id(
                OUTLOOK_COMMON_VIEWS_DEFAULT_NAVIGATION_SHORTCUT_ID
            )
            .is_none());
        assert!(snapshot
            .navigation_shortcut_table_message_for_id(
                OUTLOOK_COMMON_VIEWS_DEFAULT_SENT_NAVIGATION_SHORTCUT_ID
            )
            .is_none());
        assert!(snapshot
            .navigation_shortcut_table_message_for_id(
                OUTLOOK_COMMON_VIEWS_DEFAULT_TRASH_NAVIGATION_SHORTCUT_ID
            )
            .is_none());
        for named_view in named_views {
            assert!(snapshot
                .common_view_named_view_message_for_id(named_view.id)
                .is_some());
        }
    }

    #[test]
    fn default_folder_named_views_use_folder_family_names() {
        let snapshot = MapiMailStoreSnapshot::empty();

        for (folder_id, expected_name) in [
            (crate::mapi::identity::INBOX_FOLDER_ID, "Compact"),
            (crate::mapi::identity::CALENDAR_FOLDER_ID, "Calendar"),
            (crate::mapi::identity::TASKS_FOLDER_ID, "Tasks"),
            (crate::mapi::identity::NOTES_FOLDER_ID, "Notes"),
            (crate::mapi::identity::JOURNAL_FOLDER_ID, "Journal"),
        ] {
            let view = snapshot
                .default_folder_named_view_message(folder_id, OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID)
                .expect("default folder named view");
            assert_eq!(view.folder_id, folder_id);
            assert_eq!(view.name, expected_name);
            assert_eq!(view.view_flags, 14_745_605);
            assert_eq!(view.view_type, 8);
        }
    }

    #[test]
    fn common_views_skips_search_folder_definition_without_protocol_blob() {
        let definition_id = Uuid::from_u128(0xaaaaaaaa_1111_4111_8111_aaaaaaaaaaaa);
        let snapshot = MapiMailStoreSnapshot::empty().with_search_folder_definitions(vec![
            SearchFolderDefinition {
                id: definition_id,
                account_id: Uuid::nil(),
                role: "reminders".to_string(),
                display_name: "Reminders".to_string(),
                definition_kind: "exchange_builtin".to_string(),
                result_object_kind: "mixed".to_string(),
                scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
                restriction_json: serde_json::json!({"kind": "exchange_reminders"}),
                excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
                is_builtin: true,
            },
        ]);

        assert!(snapshot
            .common_views_table_messages()
            .all(|message| !matches!(message, MapiCommonViewsMessage::SearchFolderDefinition(_))));
    }

    #[test]
    fn common_views_projects_search_folder_definition_with_protocol_blob() {
        let definition_id = Uuid::from_u128(0xbbbbbbbb_1111_4111_8111_bbbbbbbbbbbb);
        let mut definition_blob = vec![
            0x04, 0x10, 0x00, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
        ];
        definition_blob.extend_from_slice(&1u32.to_le_bytes());
        definition_blob.push(0xAA);
        definition_blob.extend_from_slice(&0u32.to_le_bytes());
        definition_blob.push(0xBB);
        definition_blob.extend_from_slice(&0u32.to_le_bytes());
        let snapshot = MapiMailStoreSnapshot::empty().with_search_folder_definitions(vec![
            SearchFolderDefinition {
                id: definition_id,
                account_id: Uuid::nil(),
                role: "reminders".to_string(),
                display_name: "Reminders".to_string(),
                definition_kind: "exchange_builtin".to_string(),
                result_object_kind: "mixed".to_string(),
                scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
                restriction_json: serde_json::json!({
                    "kind": "exchange_reminders",
                    "pidTagSearchFolderDefinition": BASE64_STANDARD.encode(&definition_blob)
                }),
                excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
                is_builtin: true,
            },
        ]);

        assert!(snapshot
            .common_views_table_messages()
            .any(|message| matches!(message, MapiCommonViewsMessage::SearchFolderDefinition(_))));
    }

    #[test]
    fn common_views_preserves_persisted_navigation_shortcuts() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let persisted_id = Uuid::from_u128(0x6d617069_776c_416c_8000_000000000001);
        crate::mapi::identity::remember_mapi_identity(
            persisted_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 72,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
            MapiNavigationShortcutRecord {
                id: persisted_id,
                account_id,
                subject: "Alpha".to_string(),
                target_folder_id: Some(crate::mapi::identity::SENT_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 1,
                group_header_id: None,
                group_name: "Mail".to_string(),
            },
        ]);

        let messages = snapshot.common_views_table_messages().collect::<Vec<_>>();
        let shortcut = messages
            .iter()
            .find_map(|message| match message {
                MapiCommonViewsMessage::NavigationShortcut(shortcut)
                    if shortcut.subject == "Alpha" =>
                {
                    Some(shortcut)
                }
                _ => None,
            })
            .expect("persisted shortcut");
        assert_eq!(shortcut.subject, "Alpha");
        assert_eq!(shortcut.group_header_id, Some(default_wlink_group_uuid()));
        assert_eq!(shortcut.group_name, OUTLOOK_MAIL_FAVORITES_GROUP_NAME);
        assert_eq!(messages.len(), 6);
        assert_eq!(
            messages
                .iter()
                .filter(|message| matches!(
                    message,
                    MapiCommonViewsMessage::NavigationShortcut(shortcut)
                        if shortcut.shortcut_type == 4
                            && shortcut.subject == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
                ))
                .count(),
            1
        );
        assert!(snapshot
            .navigation_shortcut_table_message_for_id(0)
            .is_none());
        assert!(snapshot.common_view_named_view_message_for_id(0).is_none());
    }

    #[test]
    fn common_views_deduplicates_repeated_persisted_navigation_shortcuts() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let inbox_first_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000010);
        let inbox_duplicate_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000011);
        let sent_id = Uuid::from_u128(0x6d617069_776c_5365_8000_000000000010);
        for (offset, id) in [inbox_first_id, inbox_duplicate_id, sent_id]
            .into_iter()
            .enumerate()
        {
            crate::mapi::identity::remember_mapi_identity(
                id,
                crate::mapi::identity::mapi_store_id(
                    crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 80 + offset as u64,
                ),
            );
        }
        let group_uuid = Uuid::from_u128(0x5ba943d8_daaa_462c_a63e_9136f65c8681);
        let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
            MapiNavigationShortcutRecord {
                id: inbox_first_id,
                account_id,
                subject: "Inbox".to_string(),
                target_folder_id: Some(crate::mapi::identity::INBOX_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 127,
                group_header_id: Some(group_uuid),
                group_name: "Mail".to_string(),
            },
            MapiNavigationShortcutRecord {
                id: inbox_duplicate_id,
                account_id,
                subject: "Inbox".to_string(),
                target_folder_id: Some(crate::mapi::identity::INBOX_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 127,
                group_header_id: Some(group_uuid),
                group_name: "Mail".to_string(),
            },
            MapiNavigationShortcutRecord {
                id: sent_id,
                account_id,
                subject: "Sent".to_string(),
                target_folder_id: Some(crate::mapi::identity::SENT_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 191,
                group_header_id: Some(group_uuid),
                group_name: "Mail".to_string(),
            },
        ]);

        let shortcuts = snapshot.navigation_shortcut_messages();
        assert_eq!(shortcuts.len(), 2);
        assert_eq!(shortcuts[0].canonical_id, inbox_first_id);
        assert!(!shortcuts.iter().any(|shortcut| {
            shortcut.shortcut_type == 4
                && shortcut.subject == "Mail"
                && shortcut.group_header_id == Some(group_uuid)
        }));
        let table_messages = snapshot.common_views_table_messages().collect::<Vec<_>>();
        assert_eq!(
            table_messages
                .iter()
                .filter(|message| matches!(
                    message,
                    MapiCommonViewsMessage::NavigationShortcut(shortcut)
                        if shortcut.subject == "Inbox"
                            && shortcut.target_folder_id
                                == Some(crate::mapi::identity::INBOX_FOLDER_ID)
                ))
                .count(),
            1
        );
    }

    #[test]
    fn common_views_materializes_mail_group_header_for_persisted_favorite_links() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let inbox_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000020);
        crate::mapi::identity::remember_mapi_identity(
            inbox_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 91,
            ),
        );
        let group_uuid = default_wlink_group_uuid();
        let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
            MapiNavigationShortcutRecord {
                id: inbox_id,
                account_id,
                subject: "Inbox".to_string(),
                target_folder_id: Some(crate::mapi::identity::INBOX_FOLDER_ID),
                shortcut_type: 0,
                flags: 0x0010_8000,
                save_stamp: 0,
                section: 1,
                ordinal: 127,
                group_header_id: Some(group_uuid),
                group_name: "Mail".to_string(),
            },
        ]);

        let shortcuts = snapshot.navigation_shortcut_messages();
        assert!(!shortcuts.iter().any(|shortcut| {
            shortcut.id == OUTLOOK_COMMON_VIEWS_DEFAULT_MAIL_GROUP_HEADER_ID
                && shortcut.shortcut_type == 4
                && shortcut.subject == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
                && shortcut.group_header_id == Some(group_uuid)
        }));
        assert!(!snapshot.common_views_messages().any(|message| matches!(
            message,
            MapiCommonViewsMessage::NavigationShortcut(shortcut)
                if shortcut.shortcut_type == 4
                    && shortcut.subject == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
        )));
        assert!(snapshot
            .common_views_table_messages()
            .any(|message| matches!(
                message,
                MapiCommonViewsMessage::NavigationShortcut(shortcut)
                    if shortcut.shortcut_type == 4
                        && shortcut.subject == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
            )));
        assert_eq!(
            snapshot
                .common_views_table_messages()
                .filter(|message| matches!(
                    message,
                    MapiCommonViewsMessage::NavigationShortcut(shortcut)
                        if shortcut.shortcut_type == 4
                            && shortcut.subject == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
                ))
                .count(),
            1
        );
    }

    #[test]
    fn common_views_keeps_non_mail_shortcuts_out_of_startup_table() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let first_calendar_id = Uuid::from_u128(0x6d617069_776c_4361_8000_000000000020);
        let second_calendar_id = Uuid::from_u128(0x6d617069_776c_4361_8000_000000000021);
        for (offset, id) in [first_calendar_id, second_calendar_id]
            .into_iter()
            .enumerate()
        {
            crate::mapi::identity::remember_mapi_identity(
                id,
                crate::mapi::identity::mapi_store_id(
                    crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 90 + offset as u64,
                ),
            );
        }

        let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
            MapiNavigationShortcutRecord {
                id: first_calendar_id,
                account_id,
                subject: "Calendar".to_string(),
                target_folder_id: Some(crate::mapi::identity::CALENDAR_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 255,
                group_header_id: Some(default_wlink_group_uuid()),
                group_name: "Mail".to_string(),
            },
            MapiNavigationShortcutRecord {
                id: second_calendar_id,
                account_id,
                subject: "Calendar".to_string(),
                target_folder_id: Some(crate::mapi::identity::CALENDAR_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 511,
                group_header_id: Some(Uuid::from_u128(0x5ba943d8_daaa_462c_a63e_9136f65c8681)),
                group_name: "My Calendars".to_string(),
            },
        ]);

        assert_eq!(snapshot.navigation_shortcut_messages().len(), 1);
        assert_eq!(
            snapshot
                .navigation_shortcut_messages()
                .first()
                .and_then(|shortcut| shortcut.target_folder_id),
            Some(crate::mapi::identity::CALENDAR_FOLDER_ID)
        );
        assert!(snapshot
            .navigation_shortcut_message_for_id(crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 90
            ))
            .is_some());
        assert!(snapshot
            .navigation_shortcut_table_message_for_id(crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 90
            ))
            .is_none());
        let table_messages = snapshot.common_views_table_messages().collect::<Vec<_>>();
        assert_eq!(
            table_messages
                .iter()
                .filter(|message| matches!(
                    message,
                    MapiCommonViewsMessage::NavigationShortcut(shortcut)
                        if shortcut.target_folder_id
                            == Some(crate::mapi::identity::CALENDAR_FOLDER_ID)
                ))
                .count(),
            0
        );
    }

    #[test]
    fn snapshot_projects_canonical_mailbox_message_and_attachment_ids() {
        let mailbox_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap();
        let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
        let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            mailbox_id,
            crate::mapi::identity::mapi_store_id(17),
        );
        crate::mapi::identity::remember_mapi_identity(
            message_id,
            crate::mapi::identity::mapi_store_id(18),
        );
        let mailbox = JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "custom".to_string(),
            name: "RCA Sync".to_string(),
            sort_order: 10,
            modseq: 40,
            total_emails: 1,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        };
        let email = JmapEmail {
            id: message_id,
            thread_id: Uuid::parse_str("12121212-1212-1212-1212-121212121212").unwrap(),
            mailbox_id,
            mailbox_role: "custom".to_string(),
            mailbox_name: "RCA Sync".to_string(),
            modseq: 41,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![JmapEmailMailboxState {
                mailbox_id,
                role: "custom".to_string(),
                name: "RCA Sync".to_string(),
                modseq: 41,
                unread: false,
                flagged: false,
                followup_flag_status: "none".to_string(),
                followup_icon: 0,
                todo_item_flags: 0,
                followup_request: String::new(),
                followup_start_at: None,
                followup_due_at: None,
                followup_completed_at: None,
                reminder_set: false,
                reminder_at: None,
                reminder_dismissed_at: None,
                swapped_todo_store_id: None,
                swapped_todo_data: None,
                categories: Vec::new(),
                draft: false,
            }],
            received_at: "2026-05-03T12:00:00Z".to_string(),
            sent_at: None,
            from_address: "alice@example.test".to_string(),
            from_display: Some("Alice".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: Uuid::nil(),
            to: vec![JmapEmailAddress {
                address: "bob@example.test".to_string(),
                display_name: Some("Bob".to_string()),
            }],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Hello".to_string(),
            preview: "Hello".to_string(),
            body_text: "Hello".to_string(),
            body_html_sanitized: None,
            unread: false,
            flagged: false,
            followup_flag_status: "none".to_string(),
            followup_icon: 0,
            todo_item_flags: 0,
            followup_request: String::new(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            has_attachments: true,
            size_octets: 42,
            internet_message_id: None,
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        };
        let attachment = ActiveSyncAttachment {
            id: attachment_id,
            message_id,
            file_name: "brief.pdf".to_string(),
            media_type: "application/pdf".to_string(),
            disposition: None,
            content_id: None,
            size_octets: 5,
            file_reference: "attachment-ref".to_string(),
        };

        let snapshot = MapiMailStoreSnapshot::new(
            vec![mailbox],
            vec![email],
            vec![(message_id, vec![attachment])],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        assert_eq!(snapshot.folders().len(), 1);
        assert_eq!(snapshot.messages().len(), 1);
        assert_eq!(snapshot.messages()[0].canonical_id, message_id);
        assert_eq!(snapshot.messages()[0].folder_id, snapshot.folders()[0].id);
        assert_eq!(
            snapshot.messages()[0].attachments[0].canonical_id,
            attachment_id
        );
        assert_eq!(snapshot.messages()[0].attachments[0].attach_num, 0);
    }

    #[test]
    fn snapshot_projects_outlook_contact_books_into_fixed_mapi_folders() {
        let account_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        let rights = CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: false,
        };
        let cases = [
            (
                "suggested_contacts",
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "Suggested Contacts",
                crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
            ),
            (
                "quick_contacts",
                "cccccccc-cccc-cccc-cccc-cccccccccccc",
                "Quick Contacts",
                crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID,
            ),
            (
                "im_contact_list",
                "dddddddd-dddd-dddd-dddd-dddddddddddd",
                "IM Contact List",
                crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID,
            ),
        ];
        let collections = cases
            .iter()
            .map(
                |(collection_id, _, display_name, _)| CollaborationCollection {
                    id: (*collection_id).to_string(),
                    kind: "contacts".to_string(),
                    owner_account_id: account_id,
                    owner_email: "alice@example.test".to_string(),
                    owner_display_name: "Alice".to_string(),
                    display_name: (*display_name).to_string(),
                    is_owned: true,
                    rights: rights.clone(),
                },
            )
            .collect::<Vec<_>>();
        let contacts = cases
            .iter()
            .enumerate()
            .map(|(index, (collection_id, contact_id, _, _))| {
                let contact_id = Uuid::parse_str(contact_id).unwrap();
                crate::mapi::identity::remember_mapi_identity(
                    contact_id,
                    crate::mapi::identity::mapi_store_id(92 + index as u64),
                );
                AccessibleContact {
                    id: contact_id,
                    collection_id: (*collection_id).to_string(),
                    owner_account_id: account_id,
                    owner_email: "alice@example.test".to_string(),
                    owner_display_name: "Alice".to_string(),
                    rights: rights.clone(),
                    name: "Outlook Contact".to_string(),
                    role: String::new(),
                    email: "contact@example.test".to_string(),
                    phone: String::new(),
                    team: String::new(),
                    notes: String::new(),
                    ..Default::default()
                }
            })
            .collect::<Vec<_>>();

        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            collections,
            Vec::new(),
            Vec::new(),
            contacts,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        for (_, contact_id, _, folder_id) in cases {
            assert!(snapshot
                .collaboration_folders()
                .iter()
                .any(|folder| folder.id == folder_id));
            assert_eq!(
                snapshot.contacts_for_folder(folder_id)[0].canonical_id,
                Uuid::parse_str(contact_id).unwrap()
            );
        }

        let definition_id = Uuid::parse_str("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            definition_id,
            crate::mapi::identity::mapi_store_id(95),
        );
        let snapshot = snapshot.with_search_folder_definitions(vec![SearchFolderDefinition {
            id: definition_id,
            account_id,
            role: "contacts_search".to_string(),
            display_name: "Contacts Search".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "contact".to_string(),
            scope_json: serde_json::json!({"scope": "contacts_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_contacts_search"}),
            excluded_folder_roles: Vec::new(),
            is_builtin: true,
        }]);
        assert_eq!(snapshot.contacts_search_results().len(), 3);
        assert!(snapshot
            .contact_for_id(
                crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID,
                crate::mapi::identity::mapi_store_id(92)
            )
            .is_some());
    }

    #[test]
    fn collaboration_folder_identity_requests_cover_custom_and_shared_collections() {
        let owner_id = Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap();
        let custom_calendar_id = Uuid::parse_str("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa").unwrap();
        let rights = CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: true,
        };
        let contact_collections = vec![CollaborationCollection {
            id: format!("shared-contacts-{owner_id}"),
            kind: "contacts".to_string(),
            owner_account_id: owner_id,
            owner_email: "owner@example.test".to_string(),
            owner_display_name: "Owner".to_string(),
            display_name: "Owner Contacts".to_string(),
            is_owned: false,
            rights: rights.clone(),
        }];
        let calendar_collections = vec![
            CollaborationCollection {
                id: custom_calendar_id.to_string(),
                kind: "calendar".to_string(),
                owner_account_id: owner_id,
                owner_email: "owner@example.test".to_string(),
                owner_display_name: "Owner".to_string(),
                display_name: "Custom".to_string(),
                is_owned: true,
                rights: rights.clone(),
            },
            CollaborationCollection {
                id: format!("shared-calendar-{owner_id}"),
                kind: "calendar".to_string(),
                owner_account_id: owner_id,
                owner_email: "owner@example.test".to_string(),
                owner_display_name: "Owner".to_string(),
                display_name: "Owner Calendar".to_string(),
                is_owned: false,
                rights: rights.clone(),
            },
        ];
        let task_collections = vec![CollaborationCollection {
            id: format!("shared-tasks-{owner_id}"),
            kind: "tasks".to_string(),
            owner_account_id: owner_id,
            owner_email: "owner@example.test".to_string(),
            owner_display_name: "Owner".to_string(),
            display_name: "Owner Tasks".to_string(),
            is_owned: false,
            rights,
        }];

        let requests = collaboration_folder_identity_requests(
            &contact_collections,
            &calendar_collections,
            &task_collections,
        );
        let canonical_ids = requests
            .iter()
            .map(|request| request.canonical_id)
            .collect::<Vec<_>>();

        assert_eq!(requests.len(), 4);
        assert!(requests
            .iter()
            .all(|request| request.object_kind == MapiIdentityObjectKind::Mailbox));
        assert_eq!(
            canonical_ids
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            4
        );
        assert!(!canonical_ids.contains(&owner_id));
        assert!(!canonical_ids.contains(&custom_calendar_id));
    }

    #[test]
    fn snapshot_uses_allocated_identities_for_custom_and_shared_collaboration_folders() {
        let owner_id = Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap();
        let rights = CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: true,
        };
        let contact_collection = CollaborationCollection {
            id: format!("shared-contacts-{owner_id}"),
            kind: "contacts".to_string(),
            owner_account_id: owner_id,
            owner_email: "owner@example.test".to_string(),
            owner_display_name: "Owner".to_string(),
            display_name: "Owner Contacts".to_string(),
            is_owned: false,
            rights: rights.clone(),
        };
        let calendar_collection = CollaborationCollection {
            id: format!("shared-calendar-{owner_id}"),
            kind: "calendar".to_string(),
            owner_account_id: owner_id,
            owner_email: "owner@example.test".to_string(),
            owner_display_name: "Owner".to_string(),
            display_name: "Owner Calendar".to_string(),
            is_owned: false,
            rights: rights.clone(),
        };
        let task_collection = CollaborationCollection {
            id: format!("shared-tasks-{owner_id}"),
            kind: "tasks".to_string(),
            owner_account_id: owner_id,
            owner_email: "owner@example.test".to_string(),
            owner_display_name: "Owner".to_string(),
            display_name: "Owner Tasks".to_string(),
            is_owned: false,
            rights,
        };
        let cases = [
            (
                MapiCollaborationFolderKind::Contacts,
                &contact_collection,
                crate::mapi::identity::mapi_store_id(201),
            ),
            (
                MapiCollaborationFolderKind::Calendar,
                &calendar_collection,
                crate::mapi::identity::mapi_store_id(202),
            ),
            (
                MapiCollaborationFolderKind::Task,
                &task_collection,
                crate::mapi::identity::mapi_store_id(203),
            ),
        ];
        for (kind, collection, object_id) in cases {
            let canonical_id =
                collaboration_folder_identity_canonical_id(kind, collection).unwrap();
            crate::mapi::identity::remember_mapi_identity(canonical_id, object_id);
        }

        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![contact_collection],
            vec![calendar_collection],
            vec![task_collection],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        let folder_ids = snapshot
            .collaboration_folders()
            .iter()
            .map(|folder| folder.id)
            .collect::<Vec<_>>();

        assert!(folder_ids.contains(&crate::mapi::identity::mapi_store_id(201)));
        assert!(folder_ids.contains(&crate::mapi::identity::mapi_store_id(202)));
        assert!(folder_ids.contains(&crate::mapi::identity::mapi_store_id(203)));
        assert_eq!(
            folder_ids
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            3
        );
    }

    #[test]
    fn snapshot_projects_canonical_notes_and_journal_into_default_mapi_folders() {
        let note_id = Uuid::parse_str("51515151-5151-5151-5151-515151515151").unwrap();
        let journal_id = Uuid::parse_str("61616161-6161-6161-6161-616161616161").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            note_id,
            crate::mapi::identity::mapi_store_id(90),
        );
        crate::mapi::identity::remember_mapi_identity(
            journal_id,
            crate::mapi::identity::mapi_store_id(91),
        );

        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_notes_and_journal(
            vec![ClientNote {
                id: note_id,
                title: "Sticky note".to_string(),
                body_text: "Remember Outlook content tables".to_string(),
                color: "yellow".to_string(),
                categories_json: "[]".to_string(),
                created_at: "2026-05-19T12:00:00Z".to_string(),
                updated_at: "2026-05-19T12:30:00Z".to_string(),
            }],
            vec![JournalEntry {
                id: journal_id,
                subject: "Support call".to_string(),
                body_text: "Call notes".to_string(),
                entry_type: "phone-call".to_string(),
                message_class: "IPM.Activity".to_string(),
                starts_at: Some("2026-05-19T13:00:00Z".to_string()),
                ends_at: Some("2026-05-19T13:15:00Z".to_string()),
                occurred_at: None,
                companies_json: "[]".to_string(),
                contacts_json: "[]".to_string(),
                created_at: "2026-05-19T12:55:00Z".to_string(),
                updated_at: "2026-05-19T13:15:00Z".to_string(),
            }],
        );

        let notes = snapshot.notes_for_folder(crate::mapi::identity::NOTES_FOLDER_ID);
        let journal = snapshot.journal_entries_for_folder(crate::mapi::identity::JOURNAL_FOLDER_ID);
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].id, crate::mapi::identity::mapi_store_id(90));
        assert_eq!(notes[0].folder_id, crate::mapi::identity::NOTES_FOLDER_ID);
        assert_eq!(journal.len(), 1);
        assert_eq!(journal[0].id, crate::mapi::identity::mapi_store_id(91));
        assert_eq!(
            journal[0].folder_id,
            crate::mapi::identity::JOURNAL_FOLDER_ID
        );
    }

    #[test]
    fn snapshot_carries_persisted_search_folder_definitions() {
        let definition_id = Uuid::parse_str("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            definition_id,
            crate::mapi::identity::mapi_store_id(96),
        );
        let definition = SearchFolderDefinition {
            id: definition_id,
            account_id: Uuid::parse_str("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb").unwrap(),
            role: "reminders".to_string(),
            display_name: "Reminders".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "mixed".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_reminders"}),
            excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
            is_builtin: true,
        };
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_search_folder_definitions(vec![definition]);

        let reminders = snapshot
            .search_folder_definition_for_role("reminders")
            .expect("persisted reminders definition");
        assert_eq!(reminders.definition_kind, "exchange_builtin");
        assert_eq!(reminders.result_object_kind, "mixed");
        assert_eq!(
            reminders.excluded_folder_roles,
            exchange_builtin_excluded_folder_roles()
        );
        assert!(snapshot
            .search_folder_definition_for_role("todo_search")
            .is_none());
    }

    #[test]
    fn snapshot_resolves_tracked_mail_processing_by_advertised_folder_id() {
        let definition = SearchFolderDefinition {
            id: Uuid::parse_str("aaaaaaaa-1212-4111-8111-aaaaaaaaaaaa").unwrap(),
            account_id: Uuid::parse_str("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb").unwrap(),
            role: "tracked_mail_processing".to_string(),
            display_name: "Tracked Mail Processing".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_tracked_mail_processing"}),
            excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
            is_builtin: true,
        };
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_search_folder_definitions(vec![definition]);

        let definition = snapshot
            .search_folder_definition_for_folder_id(
                crate::mapi::identity::TRACKED_MAIL_PROCESSING_FOLDER_ID,
            )
            .expect("tracked mail processing definition");

        assert_eq!(definition.role, "tracked_mail_processing");
        assert!(definition.is_builtin);
    }

    #[test]
    fn snapshot_projects_user_saved_search_folder_as_mapi_folder() {
        let definition_id = Uuid::parse_str("aaaaaaaa-2222-4111-8111-aaaaaaaaaaaa").unwrap();
        let folder_id = crate::mapi::identity::mapi_store_id(122);
        crate::mapi::identity::remember_mapi_identity(definition_id, folder_id);
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_search_folder_definitions(vec![SearchFolderDefinition {
            id: definition_id,
            account_id: Uuid::parse_str("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb").unwrap(),
            role: "custom".to_string(),
            display_name: "Unread from Alice".to_string(),
            definition_kind: "user_saved".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "text", "query": "alice"}),
            excluded_folder_roles: vec!["trash".to_string()],
            is_builtin: false,
        }]);

        let folder = snapshot
            .folders()
            .iter()
            .find(|folder| folder.canonical_id == definition_id)
            .expect("user search folder projected");
        assert_eq!(folder.id, folder_id);
        assert_eq!(folder.mailbox.name, "Unread from Alice");
        assert_eq!(folder.mailbox.role, "__mapi_search_folder_message");
    }

    #[test]
    fn snapshot_deduplicates_user_saved_search_folder_projection_by_name() {
        let first_id = Uuid::parse_str("aaaaaaaa-3333-4111-8111-aaaaaaaaaaaa").unwrap();
        let second_id = Uuid::parse_str("aaaaaaaa-4444-4111-8111-aaaaaaaaaaaa").unwrap();
        let first_folder_id = crate::mapi::identity::mapi_store_id(0x7FFF_1000_0123);
        let second_folder_id = crate::mapi::identity::mapi_store_id(0x7FFF_1000_0124);
        crate::mapi::identity::remember_mapi_identity(first_id, first_folder_id);
        crate::mapi::identity::remember_mapi_identity(second_id, second_folder_id);

        let account_id = Uuid::parse_str("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb").unwrap();
        let duplicate_name = "Categories Rename Search Folder";
        let definition = |id| SearchFolderDefinition {
            id,
            account_id,
            role: "custom".to_string(),
            display_name: duplicate_name.to_string(),
            definition_kind: "user_saved".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({"scope": "folders"}),
            restriction_json: serde_json::json!({"kind": "mapi_bounded"}),
            excluded_folder_roles: Vec::new(),
            is_builtin: false,
        };
        let snapshot = MapiMailStoreSnapshot::empty()
            .with_search_folder_definitions(vec![definition(first_id), definition(second_id)]);

        let projected = snapshot
            .folders()
            .into_iter()
            .filter(|folder| folder.mailbox.name == duplicate_name)
            .collect::<Vec<_>>();
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].id, first_folder_id);
        assert_eq!(
            snapshot
                .user_saved_search_folder_definition_by_display_name(duplicate_name, "message")
                .map(|definition| definition.id),
            Some(first_id)
        );
    }

    #[test]
    fn snapshot_ignores_blank_mapi_bounded_user_saved_search_folder() {
        let definition_id = Uuid::parse_str("aaaaaaaa-3434-4111-8111-aaaaaaaaaaaa").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            definition_id,
            crate::mapi::identity::mapi_store_id(0x7FFF_1000_0125),
        );
        let account_id = Uuid::parse_str("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb").unwrap();
        let snapshot = MapiMailStoreSnapshot::empty().with_search_folder_definitions(vec![
            SearchFolderDefinition {
                id: definition_id,
                account_id,
                role: "custom".to_string(),
                display_name: "Categories Rename Search Folder".to_string(),
                definition_kind: "user_saved".to_string(),
                result_object_kind: "message".to_string(),
                scope_json: serde_json::json!({
                    "kind": "mapi_bounded",
                    "scope": "folders",
                    "folderIds": [],
                    "folderRoles": ["inbox"],
                    "recursive": true
                }),
                restriction_json: serde_json::json!({
                    "kind": "mapi_bounded",
                    "all": []
                }),
                excluded_folder_roles: Vec::new(),
                is_builtin: false,
            },
        ]);

        assert!(snapshot
            .folders()
            .iter()
            .all(|folder| folder.canonical_id != definition_id));
        assert!(snapshot
            .user_saved_search_folder_definition_by_display_name(
                "Categories Rename Search Folder",
                "message"
            )
            .is_none());
    }

    #[test]
    fn snapshot_projects_canonical_tasks_into_todo_search_results() {
        let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
        let task_id = Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap();
        let definition_id = Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            task_id,
            crate::mapi::identity::mapi_store_id(97),
        );
        crate::mapi::identity::remember_mapi_identity(
            definition_id,
            crate::mapi::identity::mapi_store_id(98),
        );
        let rights = CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: false,
        };
        let task_list_id = Uuid::parse_str("12121212-3434-4565-8787-909090909090").unwrap();
        let task = ClientTask {
            id: task_id,
            owner_account_id: account_id,
            owner_email: "alice@example.test".to_string(),
            owner_display_name: "Alice".to_string(),
            is_owned: true,
            rights: rights.clone(),
            task_list_id,
            task_list_sort_order: 0,
            title: "Follow up".to_string(),
            description: String::new(),
            status: "needs-action".to_string(),
            due_at: Some("2026-05-21T09:00:00Z".to_string()),
            completed_at: None,
            recurrence_rule: String::new(),
            sort_order: 0,
            updated_at: "2026-05-20T09:00:00Z".to_string(),
        };
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![CollaborationCollection {
                id: "default".to_string(),
                kind: "tasks".to_string(),
                owner_account_id: account_id,
                owner_email: "alice@example.test".to_string(),
                owner_display_name: "Alice".to_string(),
                display_name: "Tasks".to_string(),
                is_owned: true,
                rights,
            }],
            Vec::new(),
            Vec::new(),
            vec![task],
            Vec::new(),
        )
        .with_search_folder_definitions(vec![SearchFolderDefinition {
            id: definition_id,
            account_id,
            role: "todo_search".to_string(),
            display_name: "To-Do".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "mixed".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_todo"}),
            excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
            is_builtin: true,
        }]);

        assert_eq!(snapshot.todo_search_results().len(), 1);
        assert!(snapshot
            .task_for_id(
                crate::mapi::identity::TODO_SEARCH_FOLDER_ID,
                crate::mapi::identity::mapi_store_id(97)
            )
            .is_some());
    }

    #[test]
    fn snapshot_projects_followup_mail_into_todo_search_results() {
        let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
        let mailbox_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
        let message_id = Uuid::parse_str("55555555-5555-4555-8555-555555555555").unwrap();
        let definition_id = Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            mailbox_id,
            crate::mapi::identity::mapi_store_id(18),
        );
        crate::mapi::identity::remember_mapi_identity(
            message_id,
            crate::mapi::identity::mapi_store_id(19),
        );
        crate::mapi::identity::remember_mapi_identity(
            definition_id,
            crate::mapi::identity::mapi_store_id(20),
        );
        let mailbox = JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 1,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        };
        let email = JmapEmail {
            id: message_id,
            thread_id: Uuid::parse_str("12121212-1212-4212-8212-121212121212").unwrap(),
            mailbox_id,
            mailbox_role: "inbox".to_string(),
            mailbox_name: "Inbox".to_string(),
            modseq: 2,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![JmapEmailMailboxState {
                mailbox_id,
                role: "inbox".to_string(),
                name: "Inbox".to_string(),
                modseq: 2,
                unread: false,
                flagged: true,
                followup_flag_status: "flagged".to_string(),
                followup_icon: 6,
                todo_item_flags: 8,
                followup_request: "Follow up".to_string(),
                followup_start_at: None,
                followup_due_at: None,
                followup_completed_at: None,
                reminder_set: false,
                reminder_at: None,
                reminder_dismissed_at: None,
                swapped_todo_store_id: None,
                swapped_todo_data: None,
                categories: Vec::new(),
                draft: false,
            }],
            received_at: "2026-05-20T12:00:00Z".to_string(),
            sent_at: None,
            from_address: "alice@example.test".to_string(),
            from_display: Some("Alice".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: account_id,
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Flagged mail".to_string(),
            preview: "Flagged mail".to_string(),
            body_text: "Flagged mail".to_string(),
            body_html_sanitized: None,
            unread: false,
            flagged: true,
            followup_flag_status: "flagged".to_string(),
            followup_icon: 6,
            todo_item_flags: 8,
            followup_request: "Follow up".to_string(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            has_attachments: false,
            size_octets: 42,
            internet_message_id: None,
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        };
        let snapshot = MapiMailStoreSnapshot::new(
            vec![mailbox],
            vec![email],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_search_folder_definitions(vec![SearchFolderDefinition {
            id: definition_id,
            account_id,
            role: "todo_search".to_string(),
            display_name: "To-Do".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "mixed".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_todo"}),
            excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
            is_builtin: true,
        }]);

        assert_eq!(snapshot.todo_search_messages().len(), 1);
        let message_id = snapshot.todo_search_messages()[0].id;
        assert!(snapshot.todo_search_message_for_id(message_id).is_some());
    }

    #[test]
    fn snapshot_projects_swapped_todo_mail_into_tracked_mail_processing_results() {
        let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
        let mailbox_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
        let message_id = Uuid::parse_str("66666666-6666-4666-8666-666666666666").unwrap();
        let store_id = Uuid::parse_str("77777777-7777-4777-8777-777777777777").unwrap();
        let definition_id = Uuid::parse_str("88888888-8888-4888-8888-888888888888").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            mailbox_id,
            crate::mapi::identity::mapi_store_id(20),
        );
        crate::mapi::identity::remember_mapi_identity(
            message_id,
            crate::mapi::identity::mapi_store_id(21),
        );
        crate::mapi::identity::remember_mapi_identity(
            definition_id,
            crate::mapi::identity::mapi_store_id(22),
        );
        let mailbox = JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "sent".to_string(),
            name: "Sent".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 1,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        };
        let email = JmapEmail {
            id: message_id,
            thread_id: Uuid::parse_str("12121212-1212-4212-8212-121212121212").unwrap(),
            mailbox_id,
            mailbox_role: "sent".to_string(),
            mailbox_name: "Sent".to_string(),
            modseq: 2,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![JmapEmailMailboxState {
                mailbox_id,
                role: "sent".to_string(),
                name: "Sent".to_string(),
                modseq: 2,
                unread: false,
                flagged: false,
                followup_flag_status: "none".to_string(),
                followup_icon: 0,
                todo_item_flags: 0,
                followup_request: String::new(),
                followup_start_at: None,
                followup_due_at: None,
                followup_completed_at: None,
                reminder_set: false,
                reminder_at: None,
                reminder_dismissed_at: None,
                swapped_todo_store_id: Some(store_id),
                swapped_todo_data: Some(vec![9, 8, 7]),
                categories: Vec::new(),
                draft: false,
            }],
            received_at: "2026-05-20T12:00:00Z".to_string(),
            sent_at: Some("2026-05-20T12:00:00Z".to_string()),
            from_address: "alice@example.test".to_string(),
            from_display: Some("Alice".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: account_id,
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Tracked mail".to_string(),
            preview: "Tracked mail".to_string(),
            body_text: "Tracked mail".to_string(),
            body_html_sanitized: None,
            unread: false,
            flagged: false,
            followup_flag_status: "none".to_string(),
            followup_icon: 0,
            todo_item_flags: 0,
            followup_request: String::new(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: Some(store_id),
            swapped_todo_data: Some(vec![9, 8, 7]),
            categories: Vec::new(),
            has_attachments: false,
            size_octets: 42,
            internet_message_id: None,
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        };
        let snapshot = MapiMailStoreSnapshot::new(
            vec![mailbox],
            vec![email],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_search_folder_definitions(vec![SearchFolderDefinition {
            id: definition_id,
            account_id,
            role: "tracked_mail_processing".to_string(),
            display_name: "Tracked Mail Processing".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_tracked_mail_processing"}),
            excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
            is_builtin: true,
        }]);

        assert_eq!(snapshot.tracked_mail_processing_messages().len(), 1);
        assert!(snapshot
            .tracked_mail_processing_message_for_id(crate::mapi::identity::mapi_store_id(21))
            .is_some());
    }

    #[test]
    fn snapshot_projects_reminders_as_underlying_calendar_and_task_links() {
        let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
        let mailbox_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        let message_id = Uuid::parse_str("11112222-3333-4444-8555-666677778888").unwrap();
        let excluded_message_id = Uuid::parse_str("11112222-3333-4444-8555-666677778889").unwrap();
        let event_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let task_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
        let search_definition_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            message_id,
            crate::mapi::identity::mapi_store_id(97),
        );
        crate::mapi::identity::remember_mapi_identity(
            excluded_message_id,
            crate::mapi::identity::mapi_store_id(101),
        );
        crate::mapi::identity::remember_mapi_identity(
            event_id,
            crate::mapi::identity::mapi_store_id(98),
        );
        crate::mapi::identity::remember_mapi_identity(
            task_id,
            crate::mapi::identity::mapi_store_id(99),
        );
        crate::mapi::identity::remember_mapi_identity(
            search_definition_id,
            crate::mapi::identity::mapi_store_id(100),
        );
        let rights = CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: false,
        };
        let event = AccessibleEvent {
            id: event_id,
            uid: "event-uid".to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account_id,
            owner_email: "alice@example.test".to_string(),
            owner_display_name: "Alice".to_string(),
            rights: rights.clone(),
            date: "2026-05-21".to_string(),
            time: "09:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Standup".to_string(),
            location: "Room 1".to_string(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "[]".to_string(),
            notes: String::new(),
            body_html: String::new(),
        };
        let task_list_id = Uuid::parse_str("12121212-3434-4565-8787-909090909090").unwrap();
        let task = ClientTask {
            id: task_id,
            owner_account_id: account_id,
            owner_email: "alice@example.test".to_string(),
            owner_display_name: "Alice".to_string(),
            is_owned: true,
            rights: rights.clone(),
            task_list_id,
            task_list_sort_order: 0,
            title: "Follow up".to_string(),
            description: String::new(),
            status: "needs-action".to_string(),
            due_at: Some("2026-05-21T12:00:00Z".to_string()),
            completed_at: None,
            recurrence_rule: String::new(),
            sort_order: 0,
            updated_at: "2026-05-20T09:00:00Z".to_string(),
        };
        let mailbox = JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 10,
            modseq: 1,
            total_emails: 1,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        };
        let email = JmapEmail {
            id: message_id,
            thread_id: Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap(),
            mailbox_id,
            mailbox_role: "inbox".to_string(),
            mailbox_name: "Inbox".to_string(),
            modseq: 2,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![JmapEmailMailboxState {
                mailbox_id,
                role: "inbox".to_string(),
                name: "Inbox".to_string(),
                modseq: 2,
                unread: false,
                flagged: true,
                followup_flag_status: "flagged".to_string(),
                followup_icon: 6,
                todo_item_flags: 8,
                followup_request: "Follow up".to_string(),
                followup_start_at: None,
                followup_due_at: Some("2026-05-21T17:00:00Z".to_string()),
                followup_completed_at: None,
                reminder_set: true,
                reminder_at: Some("2026-05-21T16:45:00Z".to_string()),
                reminder_dismissed_at: None,
                swapped_todo_store_id: None,
                swapped_todo_data: None,
                categories: Vec::new(),
                draft: false,
            }],
            received_at: "2026-05-20T12:00:00Z".to_string(),
            sent_at: None,
            from_address: "alice@example.test".to_string(),
            from_display: Some("Alice".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: account_id,
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Mail reminder".to_string(),
            preview: "Mail reminder".to_string(),
            body_text: "Mail reminder".to_string(),
            body_html_sanitized: None,
            unread: false,
            flagged: true,
            followup_flag_status: "flagged".to_string(),
            followup_icon: 6,
            todo_item_flags: 8,
            followup_request: "Follow up".to_string(),
            followup_start_at: None,
            followup_due_at: Some("2026-05-21T17:00:00Z".to_string()),
            followup_completed_at: None,
            reminder_set: true,
            reminder_at: Some("2026-05-21T16:45:00Z".to_string()),
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            has_attachments: false,
            size_octets: 42,
            internet_message_id: None,
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        };
        let mut excluded_email = email.clone();
        excluded_email.id = excluded_message_id;
        excluded_email.mailbox_role = "drafts".to_string();
        excluded_email.mailbox_name = "Drafts".to_string();
        excluded_email.mailbox_states[0].role = "drafts".to_string();
        excluded_email.mailbox_states[0].name = "Drafts".to_string();
        let snapshot = MapiMailStoreSnapshot::new(
            vec![mailbox],
            vec![email, excluded_email],
            Vec::new(),
            Vec::new(),
            vec![CollaborationCollection {
                id: "default".to_string(),
                kind: "calendar".to_string(),
                owner_account_id: account_id,
                owner_email: "alice@example.test".to_string(),
                owner_display_name: "Alice".to_string(),
                display_name: "Calendar".to_string(),
                is_owned: true,
                rights: rights.clone(),
            }],
            vec![CollaborationCollection {
                id: "default".to_string(),
                kind: "tasks".to_string(),
                owner_account_id: account_id,
                owner_email: "alice@example.test".to_string(),
                owner_display_name: "Alice".to_string(),
                display_name: "Tasks".to_string(),
                is_owned: true,
                rights,
            }],
            Vec::new(),
            vec![event],
            vec![task],
            Vec::new(),
        )
        .with_search_folder_definitions(vec![SearchFolderDefinition {
            id: search_definition_id,
            account_id,
            role: "reminders".to_string(),
            display_name: "Reminders".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "mixed".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_reminders"}),
            excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
            is_builtin: true,
        }])
        .with_reminders(vec![
            ClientReminder {
                source_type: "mail".to_string(),
                source_id: excluded_message_id,
                occurrence_start_at: None,
                title: "Draft reminder".to_string(),
                due_at: Some("2026-05-21T17:00:00Z".to_string()),
                reminder_at: "2026-05-21T16:45:00Z".to_string(),
                dismissed_at: None,
                completed_at: None,
                status: "pending".to_string(),
            },
            ClientReminder {
                source_type: "mail".to_string(),
                source_id: message_id,
                occurrence_start_at: None,
                title: "Mail reminder".to_string(),
                due_at: Some("2026-05-21T17:00:00Z".to_string()),
                reminder_at: "2026-05-21T16:45:00Z".to_string(),
                dismissed_at: None,
                completed_at: None,
                status: "pending".to_string(),
            },
            ClientReminder {
                source_type: "calendar".to_string(),
                source_id: event_id,
                occurrence_start_at: None,
                title: "Standup".to_string(),
                due_at: Some("2026-05-21T09:30:00Z".to_string()),
                reminder_at: "2026-05-21T09:00:00Z".to_string(),
                dismissed_at: None,
                completed_at: None,
                status: "pending".to_string(),
            },
            ClientReminder {
                source_type: "task".to_string(),
                source_id: task_id,
                occurrence_start_at: None,
                title: "Follow up".to_string(),
                due_at: Some("2026-05-21T12:00:00Z".to_string()),
                reminder_at: "2026-05-21T11:45:00Z".to_string(),
                dismissed_at: None,
                completed_at: None,
                status: "pending".to_string(),
            },
        ]);

        assert_eq!(snapshot.reminder_events().len(), 1);
        assert_eq!(snapshot.reminder_tasks().len(), 1);
        assert_eq!(snapshot.reminder_messages().len(), 1);
        assert!(snapshot
            .reminder_message_for_id(crate::mapi::identity::mapi_store_id(101))
            .is_none());
        assert!(snapshot
            .event_for_id(
                crate::mapi::identity::REMINDERS_FOLDER_ID,
                crate::mapi::identity::mapi_store_id(98)
            )
            .is_some());
        assert!(snapshot
            .task_for_id(
                crate::mapi::identity::REMINDERS_FOLDER_ID,
                crate::mapi::identity::mapi_store_id(99)
            )
            .is_some());
    }

    #[test]
    fn snapshot_projects_computed_delegate_freebusy_messages() {
        let message_id = Uuid::parse_str("56565656-5656-4656-8656-565656565656").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            message_id,
            crate::mapi::identity::mapi_store_id(610),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_delegate_freebusy_messages(vec![
            DelegateFreeBusyMessageObject {
                id: message_id,
                account_id: Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap(),
                owner_account_id: Uuid::parse_str("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb").unwrap(),
                owner_email: "owner@example.test".to_string(),
                message_kind: "freebusy".to_string(),
                subject: "owner@example.test: busy".to_string(),
                body_text: "busy from 2026-05-26T08:00:00Z to 2026-05-26T09:00:00Z".to_string(),
                starts_at: Some("2026-05-26T08:00:00Z".to_string()),
                ends_at: Some("2026-05-26T09:00:00Z".to_string()),
                busy_status: Some("busy".to_string()),
                payload_json: "{}".to_string(),
                updated_at: "2026-05-26T08:00:00Z".to_string(),
            },
        ]);

        assert_eq!(snapshot.delegate_freebusy_messages().len(), 1);
        let projected_id = snapshot.delegate_freebusy_messages()[0].id;
        assert!(snapshot
            .delegate_freebusy_message_for_id(projected_id)
            .is_some());
    }
}
