use lpe_storage::DelegateFreeBusyMessageObject;
use uuid::Uuid;

use super::{MapiAssociatedConfigMessage, MapiDelegateFreeBusyMessage};
use crate::store::{MapiAssociatedConfigRecord, UpsertMapiAssociatedConfigInput};

pub(crate) fn is_associated_config_identity_property_tag(property_tag: u32) -> bool {
    matches!(
        property_tag >> 16,
        0x0FF6 // PidTagInstanceKey
            | 0x0FF9 // PidTagRecordKey
            | 0x3008 // PidTagLastModificationTime
            | 0x300B // PidTagSearchKey
            | 0x65E0 // PidTagSourceKey
            | 0x65E2 // PidTagChangeKey
            | 0x65E3 // PidTagPredecessorChangeList
            | 0x6709 // PidTagLocalCommitTime
            | 0x6748 // PidTagInstId
            | 0x674A // PidTagMid
            | 0x67A4 // PidTagChangeNumber
    )
}

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
pub(super) const OUTLOOK_MINIMAL_USER_OPTIONS_DICTIONARY_HEX: &str = "3c3f786d6c2076657273696f6e3d22312e302220656e636f64696e673d227574662d38223f3e3c55736572436f6e66696775726174696f6e20786d6c6e733d2264696374696f6e6172792e787364223e3c496e666f2076657273696f6e3d224f75746c6f6f6b2e3136222f3e3c446174613e3c65206b3d2231382d4f4c507265667356657273696f6e2220763d22392d31222f3e3c2f446174613e3c2f55736572436f6e66696775726174696f6e3e";
const OUTLOOK_STALE_USER_OPTIONS_XML_PLACEHOLDER_HEX: &str = "3c786d6c2f3e";
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
const OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_COUNTER_BASE: u64 = 0x7FFF_FFFE_0000;
#[cfg(test)]
pub(crate) const OUTLOOK_COMMON_VIEWS_SENT_TO_NAMED_VIEW_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE8);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_MAIL_GROUP_HEADER_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE7);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF9);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_SENT_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE6);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_TRASH_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE5);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_CALENDAR_GROUP_HEADER_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFDF);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_CALENDAR_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFDE);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_CONTACTS_GROUP_HEADER_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFDD);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_CONTACTS_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFDC);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_SUGGESTED_CONTACTS_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFDB);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_QUICK_CONTACTS_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFDA);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_IM_CONTACT_LIST_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFD9);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_TASKS_GROUP_HEADER_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFD8);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_TASKS_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFD7);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_NOTES_GROUP_HEADER_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFD6);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_NOTES_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFD5);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_JOURNAL_GROUP_HEADER_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFD4);
pub(super) const OUTLOOK_COMMON_VIEWS_DEFAULT_JOURNAL_NAVIGATION_SHORTCUT_ID: u64 =
    crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFD3);
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

pub(crate) fn outlook_default_folder_named_view_id(folder_id: u64) -> u64 {
    if folder_id == crate::mapi::identity::INBOX_FOLDER_ID {
        return OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID;
    }
    let folder_counter = crate::mapi::identity::global_counter_from_store_id(folder_id)
        .unwrap_or(folder_id & 0xffff);
    crate::mapi::identity::mapi_store_id(
        OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_COUNTER_BASE | (folder_counter & 0xffff),
    )
}

pub(crate) fn outlook_default_folder_named_view_name(folder_id: u64) -> &'static str {
    match folder_id {
        crate::mapi::identity::INBOX_FOLDER_ID => "Compact",
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
            | OUTLOOK_COMMON_VIEWS_DEFAULT_CALENDAR_GROUP_HEADER_ID
            | OUTLOOK_COMMON_VIEWS_DEFAULT_CALENDAR_NAVIGATION_SHORTCUT_ID
            | OUTLOOK_COMMON_VIEWS_DEFAULT_CONTACTS_GROUP_HEADER_ID
            | OUTLOOK_COMMON_VIEWS_DEFAULT_CONTACTS_NAVIGATION_SHORTCUT_ID
            | OUTLOOK_COMMON_VIEWS_DEFAULT_SUGGESTED_CONTACTS_NAVIGATION_SHORTCUT_ID
            | OUTLOOK_COMMON_VIEWS_DEFAULT_QUICK_CONTACTS_NAVIGATION_SHORTCUT_ID
            | OUTLOOK_COMMON_VIEWS_DEFAULT_IM_CONTACT_LIST_NAVIGATION_SHORTCUT_ID
            | OUTLOOK_COMMON_VIEWS_DEFAULT_TASKS_GROUP_HEADER_ID
            | OUTLOOK_COMMON_VIEWS_DEFAULT_TASKS_NAVIGATION_SHORTCUT_ID
            | OUTLOOK_COMMON_VIEWS_DEFAULT_NOTES_GROUP_HEADER_ID
            | OUTLOOK_COMMON_VIEWS_DEFAULT_NOTES_NAVIGATION_SHORTCUT_ID
            | OUTLOOK_COMMON_VIEWS_DEFAULT_JOURNAL_GROUP_HEADER_ID
            | OUTLOOK_COMMON_VIEWS_DEFAULT_JOURNAL_NAVIGATION_SHORTCUT_ID
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
            properties_json: serde_json::json!({
                "0x7c060003": {"type": "u32", "value": 4},
                "0x7c070102": {
                    "type": "binary",
                    "value": OUTLOOK_MINIMAL_USER_OPTIONS_DICTIONARY_HEX
                }
            }),
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

pub(crate) fn outlook_inbox_broad_startup_associated_config_defaults(
) -> Vec<MapiAssociatedConfigMessage> {
    outlook_inbox_associated_config_defaults(crate::mapi::identity::INBOX_FOLDER_ID)
        .into_iter()
        .filter(|message| message.message_class == OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_CLASS)
        .collect()
}

pub(crate) fn modeled_virtual_associated_config_message_for_canonical_id(
    canonical_id: Uuid,
) -> Option<MapiAssociatedConfigMessage> {
    outlook_inbox_associated_config_sync_defaults(crate::mapi::identity::INBOX_FOLDER_ID)
        .into_iter()
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

pub(super) fn is_stale_outlook_umolk_user_options_placeholder(
    config: &MapiAssociatedConfigRecord,
) -> bool {
    if config.folder_id != crate::mapi::identity::INBOX_FOLDER_ID
        || !is_outlook_umolk_user_options_message_class(&config.message_class)
    {
        return false;
    }
    config
        .properties_json
        .as_object()
        .and_then(|properties| {
            properties
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case("0x7c070102"))
                .and_then(|(_, value)| value.get("value"))
                .and_then(serde_json::Value::as_str)
        })
        .is_some_and(|value| {
            value.eq_ignore_ascii_case(OUTLOOK_STALE_USER_OPTIONS_XML_PLACEHOLDER_HEX)
        })
}

pub(crate) fn is_outlook_configuration_message_class(message_class: &str) -> bool {
    message_class
        .get(..18)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("IPM.Configuration."))
}

pub(crate) fn is_outlook_configuration_message_class_name(
    message_class: &str,
    expected: &str,
) -> bool {
    message_class.eq_ignore_ascii_case(expected)
}

pub(crate) fn is_outlook_umolk_user_options_message_class(message_class: &str) -> bool {
    message_class.eq_ignore_ascii_case(OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS)
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
