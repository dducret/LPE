use lpe_storage::{ConversationAction, DelegateFreeBusyMessageObject};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    MapiAssociatedConfigMessage, MapiCommonViewNamedViewMessage, MapiConversationActionMessage,
    MapiDelegateFreeBusyMessage, MapiNavigationShortcutMessage,
};
use crate::store::{MapiAssociatedConfigRecord, UpsertMapiAssociatedConfigInput};

pub(super) const OUTLOOK_INBOX_ACCOUNT_PREFS_CONFIG_CLASS: &str = "IPM.Configuration.AccountPrefs";
pub(super) const OUTLOOK_INBOX_ACCOUNT_PREFS_CONFIG_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFFB);
pub(super) const OUTLOOK_INBOX_EAS_CONFIG_CLASS: &str = "IPM.Configuration.EAS";
pub(super) const OUTLOOK_INBOX_EAS_CONFIG_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFFD);
pub(super) const OUTLOOK_INBOX_ELC_CONFIG_CLASS: &str = "IPM.Configuration.ELC";
pub(super) const OUTLOOK_INBOX_ELC_CONFIG_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFFC);
pub(super) const OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_CLASS: &str =
    "IPM.Configuration.MessageListSettings";
pub(super) const OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF8);
pub(super) const OUTLOOK_INBOX_MRM_CONFIG_CLASS: &str = "IPM.Configuration.MRM";
pub(super) const OUTLOOK_INBOX_MRM_CONFIG_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE1);
pub(super) const OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS: &str =
    "IPM.Configuration.UMOLK.UserOptions";
pub(super) const OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFFA);
pub(crate) const OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS: &str = "IPM.RuleOrganizer";
pub(super) const OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFED);
pub(super) const OUTLOOK_RULE_ORGANIZER_BINARY_6802_JSON_KEY: &str = "0x68020102";
pub(crate) const OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS: &str =
    "IPM.Microsoft.FolderDesign.NamedView";
pub(crate) const OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF6);
pub(crate) const OUTLOOK_COMMON_VIEWS_COMPACT_NAMED_VIEW_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF7);
pub(crate) const OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE9);
pub(super) const OUTLOOK_COMMON_VIEWS_SENT_TO_NAMED_VIEW_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE8);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_MAIL_GROUP_HEADER_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE7);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF9);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_SENT_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE6);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_TRASH_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE5);
pub(super) const OUTLOOK_MAIL_FAVORITES_GROUP_NAME: &str = "Favorites";
pub(super) const OUTLOOK_INBOX_SHARING_CONFIGURATION_CLASS: &str = "IPM.Sharing.Configuration";
pub(super) const OUTLOOK_INBOX_SHARING_CONFIGURATION_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF5);
pub(super) const OUTLOOK_INBOX_SHARING_INDEX_CLASS: &str = "IPM.Sharing.Index";
pub(super) const OUTLOOK_INBOX_SHARING_INDEX_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF3);
pub(super) const OUTLOOK_INBOX_AGGREGATION_CLASS: &str = "IPM.Aggregation";
pub(super) const OUTLOOK_INBOX_AGGREGATION_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE3);
pub(crate) const OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS: &str = "IPM.Microsoft.CustomAction";
pub(super) const OUTLOOK_QUICK_STEP_CUSTOM_ACTION_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF4);
pub(super) const OUTLOOK_CONTACT_SYNC_CONFIG_CLASS: &str = "IPM.Microsoft.OSC.ContactSync";
pub(super) const OUTLOOK_CONTACTS_OSC_CONTACT_SYNC_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF1);
pub(super) const OUTLOOK_SUGGESTED_CONTACTS_OSC_CONTACT_SYNC_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF0);
pub(super) const OUTLOOK_QUICK_CONTACTS_OSC_CONTACT_SYNC_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFEF);
pub(super) const OUTLOOK_IM_CONTACT_LIST_OSC_CONTACT_SYNC_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFEE);
pub(super) const OUTLOOK_DYNAMIC_CONTACT_SYNC_CONFIG_COUNTER_BASE: u64 = 0x7FFF_FF00_0000;
pub(super) const OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS: &str =
    "IPM.Microsoft.ContactLink.TimeStamp";
pub(super) const OUTLOOK_CONTACTS_CONTACT_LINK_TIMESTAMP_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFEC);
pub(super) const OUTLOOK_SUGGESTED_CONTACTS_CONTACT_LINK_TIMESTAMP_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFEB);
pub(super) const OUTLOOK_QUICK_CONTACTS_CONTACT_LINK_TIMESTAMP_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFEA);
pub(super) const OUTLOOK_IM_CONTACT_LIST_CONTACT_LINK_TIMESTAMP_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE2);
pub(super) const OUTLOOK_DYNAMIC_CONTACT_LINK_TIMESTAMP_COUNTER_BASE: u64 = 0x7FFF_FE00_0000;
pub(super) const OUTLOOK_DEFAULT_CONVERSATION_ACTION_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2);
pub(super) const OUTLOOK_LOCAL_FREEBUSY_MESSAGE_ID: u64 =
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
        _ => "Messages",
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

pub(super) fn outlook_inbox_associated_config_defaults(
    folder_id: u64,
) -> Vec<MapiAssociatedConfigMessage> {
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

pub(super) fn outlook_inbox_associated_config_sync_defaults(
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
        OUTLOOK_INBOX_ELC_CONFIG_CLASS
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

pub(super) fn outlook_inbox_exact_virtual_associated_config_for_id(
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

pub(crate) fn outlook_inbox_broad_startup_associated_config_defaults(
) -> Vec<MapiAssociatedConfigMessage> {
    outlook_inbox_associated_config_defaults(crate::mapi::identity::INBOX_FOLDER_ID)
        .into_iter()
        .filter(|message| {
            matches!(
                message.message_class.as_str(),
                OUTLOOK_INBOX_ACCOUNT_PREFS_CONFIG_CLASS | OUTLOOK_INBOX_ELC_CONFIG_CLASS
            )
        })
        .collect()
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

pub(super) fn format_associated_config_classes(configs: &[MapiAssociatedConfigRecord]) -> String {
    let mut classes = configs
        .iter()
        .map(|config| config.message_class.clone())
        .collect::<Vec<_>>();
    classes.sort();
    classes.dedup();
    classes.join(",")
}

pub(super) fn format_associated_config_inputs(
    inputs: &[UpsertMapiAssociatedConfigInput],
) -> String {
    let mut classes = inputs
        .iter()
        .map(|input| input.message_class.clone())
        .collect::<Vec<_>>();
    classes.sort();
    classes.dedup();
    classes.join(",")
}

pub(super) fn log_outlook_inbox_associated_config_bootstrap(
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

pub(super) fn is_empty_synthetic_inbox_associated_config(
    config: &MapiAssociatedConfigRecord,
) -> bool {
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

pub(super) fn is_empty_outlook_inbox_named_view_placeholder(
    config: &MapiAssociatedConfigRecord,
) -> bool {
    config.folder_id == crate::mapi::identity::INBOX_FOLDER_ID
        && config.message_class == OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS
        && config.subject == "Compact"
        && config
            .properties_json
            .as_object()
            .is_some_and(|object| object.is_empty())
}

pub(super) fn is_empty_outlook_rule_organizer_placeholder(
    config: &MapiAssociatedConfigRecord,
) -> bool {
    config.folder_id == crate::mapi::identity::INBOX_FOLDER_ID
        && config.message_class == OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS
        && !config
            .properties_json
            .get(OUTLOOK_RULE_ORGANIZER_BINARY_6802_JSON_KEY)
            .and_then(|value| value.get("value"))
            .and_then(|value| value.as_str())
            .is_some_and(|value| !value.is_empty())
}

pub(super) fn outlook_quick_step_associated_config_defaults(
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

pub(super) fn outlook_common_views_default_named_views() -> Vec<MapiCommonViewNamedViewMessage> {
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

pub(super) fn outlook_common_views_default_navigation_shortcuts(
) -> Vec<MapiNavigationShortcutMessage> {
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

pub(super) fn outlook_contact_sync_associated_config_default(
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

pub(super) fn outlook_contact_link_timestamp_associated_config_default(
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

pub(super) fn outlook_contact_associated_config_defaults(
    folder_id: u64,
) -> Vec<MapiAssociatedConfigMessage> {
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

pub(super) fn outlook_dynamic_contact_sync_config_id(folder_id: u64) -> Option<u64> {
    let folder_counter = crate::mapi::identity::global_counter_from_store_id(folder_id)?;
    if folder_counter == 0 || folder_counter >= 0x00FF_FF00 {
        return None;
    }
    Some(crate::mapi::identity::mapi_store_id(
        OUTLOOK_DYNAMIC_CONTACT_SYNC_CONFIG_COUNTER_BASE | folder_counter,
    ))
}

pub(super) fn outlook_dynamic_contact_link_timestamp_config_id(folder_id: u64) -> Option<u64> {
    let folder_counter = crate::mapi::identity::global_counter_from_store_id(folder_id)?;
    if folder_counter == 0 || folder_counter >= 0x00FF_FF00 {
        return None;
    }
    Some(crate::mapi::identity::mapi_store_id(
        OUTLOOK_DYNAMIC_CONTACT_LINK_TIMESTAMP_COUNTER_BASE | folder_counter,
    ))
}

pub(super) fn outlook_dynamic_contact_sync_canonical_id(folder_id: u64) -> Uuid {
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

pub(super) fn outlook_dynamic_contact_link_timestamp_canonical_id(folder_id: u64) -> Uuid {
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

pub(super) fn outlook_default_conversation_action() -> MapiConversationActionMessage {
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

pub(super) fn virtual_local_freebusy_message() -> MapiDelegateFreeBusyMessage {
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
