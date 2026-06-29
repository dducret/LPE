use super::rop::*;
use super::session::*;
use super::sync::*;
use super::tables::*;
use super::wire::MapiPropertyType;
use super::*;
use crate::mapi::identity::{
    RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID, RECOVERABLE_ITEMS_PURGES_FOLDER_ID,
    RECOVERABLE_ITEMS_ROOT_FOLDER_ID, RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID,
};
use crate::mapi_store::{
    MapiAssociatedConfigMessage, MapiAttachment, MapiCommonViewNamedViewMessage,
    MapiConversationActionMessage, MapiMessage, MapiNavigationShortcutMessage, MapiPublicFolder,
};
use crate::store::ExchangeAddressBookEntryDetails;
use anyhow::bail;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use lpe_domain::{civil_from_days, days_from_civil};
use lpe_storage::{
    calendar_attendee_labels, normalize_calendar_email, parse_calendar_participants_metadata,
    serialize_calendar_participants_metadata, CalendarOrganizerMetadata,
    CalendarParticipantMetadata, SearchFolderDefinition,
};

mod tags;
mod values;

pub(crate) use tags::*;
pub(super) use values::*;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MapiNamedProperty {
    pub(crate) guid: [u8; 16],
    pub(crate) kind: MapiNamedPropertyKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum MapiNamedPropertyKind {
    Lid(u32),
    Name(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct MapiSortOrder {
    pub(in crate::mapi) property_tag: u32,
    pub(in crate::mapi) order: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) enum MapiRestriction {
    InvalidTableRestriction,
    And(Vec<MapiRestriction>),
    Or(Vec<MapiRestriction>),
    Not(Box<MapiRestriction>),
    Content {
        property_tag: u32,
        value: String,
        fuzzy_level_low: u16,
        fuzzy_level_high: u16,
    },
    Property {
        relop: u8,
        property_tag: u32,
        value: MapiValue,
    },
    CompareProperties {
        relop: u8,
        left_property_tag: u32,
        right_property_tag: u32,
    },
    Bitmask {
        property_tag: u32,
        mask: u32,
        must_be_nonzero: bool,
    },
    Size {
        relop: u8,
        property_tag: u32,
        size: u32,
    },
    Exist {
        property_tag: u32,
    },
    Count {
        count: u32,
        child: Box<MapiRestriction>,
    },
    SubObject {
        subobject: u32,
        child: Box<MapiRestriction>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) enum MapiValue {
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F64(u64),
    String(String),
    Binary(Vec<u8>),
    Guid([u8; 16]),
    Error(u32),
    MultiI16(Vec<i16>),
    MultiI32(Vec<i32>),
    MultiI64(Vec<i64>),
    MultiString(Vec<String>),
    MultiBinary(Vec<Vec<u8>>),
    MultiGuid(Vec<[u8; 16]>),
}

pub(in crate::mapi) fn well_known_named_property_id(property: &MapiNamedProperty) -> Option<u16> {
    well_known_named_properties()
        .into_iter()
        .find_map(|(property_id, candidate)| (candidate == *property).then_some(property_id))
}

pub(in crate::mapi) fn well_known_named_property_for_id(
    property_id: u16,
) -> Option<MapiNamedProperty> {
    well_known_named_properties()
        .into_iter()
        .find_map(|(candidate_id, property)| (candidate_id == property_id).then_some(property))
}

pub(crate) fn is_reserved_named_property_id(property_id: u16) -> bool {
    well_known_named_property_for_id(property_id).is_some()
}

fn well_known_named_properties() -> Vec<(u16, MapiNamedProperty)> {
    [
        (
            PID_LID_GLOBAL_OBJECT_ID_NAMED_ID,
            PID_LID_GLOBAL_OBJECT_ID,
            PSETID_MEETING_GUID,
        ),
        (
            PID_LID_CLEAN_GLOBAL_OBJECT_ID_NAMED_ID,
            PID_LID_CLEAN_GLOBAL_OBJECT_ID,
            PSETID_MEETING_GUID,
        ),
    ]
    .into_iter()
    .chain(
        [
            (PID_LID_COMMON_START, PSETID_COMMON_GUID),
            (PID_LID_COMMON_END, PSETID_COMMON_GUID),
            (PID_LID_SIDE_EFFECTS, PSETID_COMMON_GUID),
            (PID_LID_OUTLOOK_COMMON_8514, PSETID_COMMON_GUID),
            (PID_LID_OUTLOOK_COMMON_8578, PSETID_COMMON_GUID),
            (PID_LID_OUTLOOK_COMMON_85B1, PSETID_COMMON_GUID),
            (PID_LID_REMINDER_TIME, PSETID_COMMON_GUID),
            (PID_LID_REMINDER_SET, PSETID_COMMON_GUID),
            (PID_LID_REMINDER_DELTA, PSETID_COMMON_GUID),
            (PID_LID_REMINDER_OVERRIDE, PSETID_COMMON_GUID),
            (PID_LID_REMINDER_PLAY_SOUND, PSETID_COMMON_GUID),
            (PID_LID_REMINDER_FILE_PARAMETER, PSETID_COMMON_GUID),
            (PID_LID_FLAG_REQUEST, PSETID_COMMON_GUID),
            (PID_LID_REMINDER_SIGNAL_TIME, PSETID_COMMON_GUID),
            (PID_LID_PERCENT_COMPLETE, PSETID_TASK_GUID),
            (PID_LID_TASK_START_DATE, PSETID_TASK_GUID),
            (PID_LID_TASK_DUE_DATE, PSETID_TASK_GUID),
            (PID_LID_BUSY_STATUS, PSETID_APPOINTMENT_GUID),
            (PID_LID_LOCATION, PSETID_APPOINTMENT_GUID),
            (PID_LID_APPOINTMENT_START_WHOLE, PSETID_APPOINTMENT_GUID),
            (PID_LID_APPOINTMENT_END_WHOLE, PSETID_APPOINTMENT_GUID),
            (PID_LID_APPOINTMENT_DURATION, PSETID_APPOINTMENT_GUID),
            (PID_LID_APPOINTMENT_COLOR, PSETID_APPOINTMENT_GUID),
            (PID_LID_APPOINTMENT_SUB_TYPE, PSETID_APPOINTMENT_GUID),
            (PID_LID_APPOINTMENT_RECUR, PSETID_APPOINTMENT_GUID),
            (PID_LID_APPOINTMENT_STATE_FLAGS, PSETID_APPOINTMENT_GUID),
            (PID_LID_RECURRING, PSETID_APPOINTMENT_GUID),
            (PID_LID_ALL_ATTENDEES_STRING, PSETID_APPOINTMENT_GUID),
            (PID_LID_TO_ATTENDEES_STRING, PSETID_APPOINTMENT_GUID),
            (PID_LID_CC_ATTENDEES_STRING, PSETID_APPOINTMENT_GUID),
            (PID_LID_TIME_ZONE_STRUCT, PSETID_APPOINTMENT_GUID),
            (PID_LID_TIME_ZONE_DESCRIPTION, PSETID_APPOINTMENT_GUID),
            (
                PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY,
                PSETID_APPOINTMENT_GUID,
            ),
            (
                PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY,
                PSETID_APPOINTMENT_GUID,
            ),
            (PID_LID_OUTLOOK_APPOINTMENT_8F07, PSETID_APPOINTMENT_GUID),
            (PID_LID_EMAIL1_DISPLAY_NAME, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL1_ADDRESS_TYPE, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL1_EMAIL_ADDRESS, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL1_ORIGINAL_DISPLAY_NAME, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL1_ORIGINAL_ENTRY_ID, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL2_DISPLAY_NAME, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL2_ADDRESS_TYPE, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL2_EMAIL_ADDRESS, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL2_ORIGINAL_DISPLAY_NAME, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL2_ORIGINAL_ENTRY_ID, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL3_DISPLAY_NAME, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL3_ADDRESS_TYPE, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL3_EMAIL_ADDRESS, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL3_ORIGINAL_DISPLAY_NAME, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL3_ORIGINAL_ENTRY_ID, PSETID_ADDRESS_GUID),
            (PID_LID_OUTLOOK_CONTACT_SOURCE_80E0, PSETID_ADDRESS_GUID),
            (PID_LID_OUTLOOK_CONTACT_SOURCE_80E2, PSETID_ADDRESS_GUID),
            (PID_LID_OUTLOOK_CONTACT_SOURCE_80E3, PSETID_ADDRESS_GUID),
            (PID_LID_OUTLOOK_CONTACT_SOURCE_80E5, PSETID_ADDRESS_GUID),
            (PID_LID_OUTLOOK_CONTACT_SOURCE_80E6, PSETID_ADDRESS_GUID),
            (PID_LID_OUTLOOK_CONTACT_SOURCE_80E8, PSETID_ADDRESS_GUID),
            (
                PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80E1,
                PS_PUBLIC_STRINGS_GUID,
            ),
            (
                PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EA,
                PS_PUBLIC_STRINGS_GUID,
            ),
            (
                PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EC,
                PS_PUBLIC_STRINGS_GUID,
            ),
            (
                PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80ED,
                PS_PUBLIC_STRINGS_GUID,
            ),
            (
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_ADDRESS_TYPE,
                PSETID_ADDRESS_GUID,
            ),
            (
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_DISPLAY_NAME,
                PSETID_ADDRESS_GUID,
            ),
            (
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_EMAIL_ADDRESS,
                PSETID_ADDRESS_GUID,
            ),
            (
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_ADDRESS_TYPE,
                PSETID_ADDRESS_GUID,
            ),
            (
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_DISPLAY_NAME,
                PSETID_ADDRESS_GUID,
            ),
            (
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_EMAIL_ADDRESS,
                PSETID_ADDRESS_GUID,
            ),
            (
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS3_ADDRESS_TYPE,
                PSETID_ADDRESS_GUID,
            ),
            (
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS3_EMAIL_ADDRESS,
                PSETID_ADDRESS_GUID,
            ),
            (PID_LID_COMPANIES, PSETID_COMMON_GUID),
            (PID_LID_CONTACTS, PSETID_COMMON_GUID),
            (PID_LID_CONTACT_LINK_SEARCH_KEY, PSETID_COMMON_GUID),
            (PID_LID_CONTACT_LINK_ENTRY, PSETID_COMMON_GUID),
            (PID_LID_CONTACT_LINK_NAME, PSETID_COMMON_GUID),
            (
                PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID,
                PSETID_COMMON_GUID,
            ),
            (
                PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID,
                PSETID_COMMON_GUID,
            ),
            (
                PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME,
                PSETID_COMMON_GUID,
            ),
            (PID_LID_CONVERSATION_PROCESSED, PSETID_COMMON_GUID),
            (
                PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME,
                PSETID_COMMON_GUID,
            ),
            (PID_LID_CONVERSATION_ACTION_VERSION, PSETID_COMMON_GUID),
            (PID_LID_LOG_TYPE, PSETID_LOG_GUID),
            (PID_LID_LOG_START, PSETID_LOG_GUID),
            (PID_LID_LOG_DURATION, PSETID_LOG_GUID),
            (PID_LID_LOG_END, PSETID_LOG_GUID),
            (PID_LID_LOG_FLAGS, PSETID_LOG_GUID),
            (PID_LID_LOG_TYPE_DESC, PSETID_LOG_GUID),
            (PID_LID_NOTE_COLOR, PSETID_NOTE_GUID),
            (PID_LID_NOTE_HEIGHT, PSETID_NOTE_GUID),
            (PID_LID_NOTE_WIDTH, PSETID_NOTE_GUID),
            (PID_LID_NOTE_X, PSETID_NOTE_GUID),
            (PID_LID_NOTE_Y, PSETID_NOTE_GUID),
            (PID_LID_POST_RSS_CHANNEL_LINK, PSETID_POST_RSS_GUID),
            (PID_LID_POST_RSS_ITEM_LINK, PSETID_POST_RSS_GUID),
            (PID_LID_POST_RSS_ITEM_HASH, PSETID_POST_RSS_GUID),
            (PID_LID_POST_RSS_ITEM_GUID, PSETID_POST_RSS_GUID),
            (PID_LID_POST_RSS_CHANNEL, PSETID_POST_RSS_GUID),
            (PID_LID_POST_RSS_ITEM_XML, PSETID_POST_RSS_GUID),
            (PID_LID_POST_RSS_SUBSCRIPTION, PSETID_POST_RSS_GUID),
            (PID_LID_OUTLOOK_SHARING_PROVIDER_GUID, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_REMOTE_NAME, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_REMOTE_UID, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_LOCAL_TYPE, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_CAPABILITIES, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8AA6, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A70, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A71, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A72, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A73, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A74, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A75, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A76, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A77, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A78, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A7E, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A80, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A88, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A8B, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A8D, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A8E, PSETID_SHARING_GUID),
        ]
        .into_iter()
        .map(|(lid, guid)| (lid as u16, lid, guid)),
    )
    .map(|(property_id, lid, guid)| {
        (
            property_id,
            MapiNamedProperty {
                guid,
                kind: MapiNamedPropertyKind::Lid(lid),
            },
        )
    })
    .collect::<Vec<_>>()
    .into_iter()
    .chain(std::iter::once((
        MapiPropertyTag::new(PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG)
            .property_id(),
        MapiNamedProperty {
            guid: PSETID_SHARING_GUID,
            kind: MapiNamedPropertyKind::Name(
                "SharingCalendarGroupEntryAssociatedLocalFolderId".to_string(),
            ),
        },
    )))
    .chain(std::iter::once((
        MapiPropertyTag::new(PID_NAME_SHARING_SEND_AS_STATE_TAG).property_id(),
        MapiNamedProperty {
            guid: PSETID_SHARING_GUID,
            kind: MapiNamedPropertyKind::Name("SharingSendAsState".to_string()),
        },
    )))
    .chain(std::iter::once((
        0x9000,
        MapiNamedProperty {
            guid: PS_PUBLIC_STRINGS_GUID,
            kind: MapiNamedPropertyKind::Name("Keywords".to_string()),
        },
    )))
    .chain(std::iter::once((
        MapiPropertyTag::new(PID_NAME_OSC_CONTACT_SOURCES_TAG).property_id(),
        MapiNamedProperty {
            guid: PS_PUBLIC_STRINGS_GUID,
            kind: MapiNamedPropertyKind::Name("OscContactSources".to_string()),
        },
    )))
    .chain([
        (
            MapiPropertyTag::new(PID_NAME_CONTENT_CLASS_W_TAG).property_id(),
            MapiNamedProperty {
                guid: PS_INTERNET_HEADERS_GUID,
                kind: MapiNamedPropertyKind::Name("content-class".to_string()),
            },
        ),
        (
            MapiPropertyTag::new(PID_NAME_CONTENT_TYPE_W_TAG).property_id(),
            MapiNamedProperty {
                guid: PS_INTERNET_HEADERS_GUID,
                kind: MapiNamedPropertyKind::Name("content-type".to_string()),
            },
        ),
    ])
    .collect()
}

pub(in crate::mapi) const NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID: [u8; 16] = [
    0xDC, 0xA7, 0x40, 0xC8, 0xC0, 0x42, 0x10, 0x1A, 0xB4, 0xB9, 0x08, 0x00, 0x2B, 0x2F, 0xE1, 0x82,
];

const FOLDER_IPM_SUBTREE_VALID: u32 = 0x0000_0001;
const FOLDER_IPM_INBOX_VALID: u32 = 0x0000_0002;
const FOLDER_IPM_OUTBOX_VALID: u32 = 0x0000_0004;
const FOLDER_IPM_WASTEBASKET_VALID: u32 = 0x0000_0008;
const FOLDER_IPM_SENTMAIL_VALID: u32 = 0x0000_0010;
const FOLDER_VIEWS_VALID: u32 = 0x0000_0020;
const FOLDER_COMMON_VIEWS_VALID: u32 = 0x0000_0040;
const FOLDER_FINDER_VALID: u32 = 0x0000_0080;

pub(in crate::mapi) fn logon_property_value(
    principal: &AccountPrincipal,
    property_tag: u32,
) -> Option<MapiValue> {
    match property_tag {
        PID_TAG_SERIALIZED_REPLID_GUID_MAP => Some(MapiValue::Binary(serialized_replid_guid_map())),
        PID_TAG_VALID_FOLDER_MASK => Some(MapiValue::U32(valid_folder_mask())),
        PID_TAG_RESOURCE_FLAGS => Some(MapiValue::U32(0)),
        PID_TAG_USER_ENTRY_ID => Some(MapiValue::Binary(mailbox_owner_entry_id(principal))),
        PID_TAG_MAILBOX_OWNER_ENTRY_ID => {
            Some(MapiValue::Binary(mailbox_owner_entry_id(principal)))
        }
        PID_TAG_MAILBOX_OWNER_NAME_W => Some(MapiValue::String(principal.display_name.clone())),
        PID_TAG_ASSOCIATED_SHARING_PROVIDER => Some(MapiValue::Guid(OUTLOOK_SHARING_PROVIDER_GUID)),
        PID_TAG_IPM_PUBLIC_FOLDERS_ENTRY_ID => Some(special_folder_entry_id_value(
            principal.account_id,
            PUBLIC_FOLDERS_ROOT_FOLDER_ID,
        )),
        PID_TAG_SERVER_TYPE_DISPLAY_NAME_W => Some(MapiValue::String("LPE".to_string())),
        PID_TAG_SERVER_CONNECTED_ICON | PID_TAG_SERVER_ACCOUNT_ICON => {
            Some(MapiValue::Binary(OUTLOOK_STORE_ICON_ICO.to_vec()))
        }
        PID_TAG_OUTLOOK_STORE_STATE => Some(MapiValue::U32(0)),
        PID_TAG_PRIVATE => Some(MapiValue::Bool(true)),
        PID_TAG_USER_GUID => Some(MapiValue::Binary(principal.account_id.as_bytes().to_vec())),
        PID_TAG_MESSAGE_SIZE_EXTENDED => principal
            .quota_used_octets
            .map(|value| MapiValue::I64(value.min(i64::MAX as u64) as i64)),
        PID_TAG_PROHIBIT_RECEIVE_QUOTA
        | PID_TAG_PROHIBIT_SEND_QUOTA
        | PID_TAG_STORAGE_QUOTA_LIMIT => principal
            .quota_mb
            .map(|value| MapiValue::U32(value.saturating_mul(1024))),
        PID_TAG_MAX_SUBMIT_MESSAGE_SIZE | PID_TAG_EXTENDED_RULE_SIZE_LIMIT => {
            Some(MapiValue::U32(35 * 1024))
        }
        PID_TAG_PST_PATH_W => Some(MapiValue::String(String::new())),
        _ => special_folder_identification_property_value(principal.account_id, property_tag),
    }
}

pub(in crate::mapi) fn special_folder_identification_property_value(
    mailbox_guid: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    match canonical_property_storage_tag(property_tag) {
        PID_TAG_VALID_FOLDER_MASK => Some(MapiValue::U32(valid_folder_mask())),
        PID_TAG_IPM_SUBTREE_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            IPM_SUBTREE_FOLDER_ID,
        )),
        PID_TAG_IPM_OUTBOX_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            OUTBOX_FOLDER_ID,
        )),
        PID_TAG_IPM_WASTEBASKET_ENTRY_ID => {
            Some(special_folder_entry_id_value(mailbox_guid, TRASH_FOLDER_ID))
        }
        PID_TAG_IPM_SENTMAIL_ENTRY_ID => {
            Some(special_folder_entry_id_value(mailbox_guid, SENT_FOLDER_ID))
        }
        PID_TAG_VIEWS_ENTRY_ID => {
            Some(special_folder_entry_id_value(mailbox_guid, VIEWS_FOLDER_ID))
        }
        PID_TAG_COMMON_VIEWS_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            COMMON_VIEWS_FOLDER_ID,
        )),
        PID_TAG_FINDER_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            SEARCH_FOLDER_ID,
        )),
        PID_TAG_IPM_ARCHIVE_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            ARCHIVE_FOLDER_ID,
        )),
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            CALENDAR_FOLDER_ID,
        )),
        PID_TAG_IPM_CONTACT_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            CONTACTS_FOLDER_ID,
        )),
        PID_TAG_IPM_JOURNAL_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            JOURNAL_FOLDER_ID,
        )),
        PID_TAG_IPM_NOTE_ENTRY_ID => {
            Some(special_folder_entry_id_value(mailbox_guid, NOTES_FOLDER_ID))
        }
        PID_TAG_IPM_TASK_ENTRY_ID => {
            Some(special_folder_entry_id_value(mailbox_guid, TASKS_FOLDER_ID))
        }
        PID_TAG_REM_ONLINE_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            REMINDERS_FOLDER_ID,
        )),
        PID_TAG_REM_OFFLINE_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            REMINDERS_FOLDER_ID,
        )),
        PID_TAG_IPM_DRAFTS_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            DRAFTS_FOLDER_ID,
        )),
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS => Some(MapiValue::MultiBinary(additional_ren_entry_ids(
            mailbox_guid,
        ))),
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX => {
            Some(MapiValue::Binary(additional_ren_entry_ids_ex(mailbox_guid)))
        }
        PID_TAG_FREE_BUSY_ENTRY_IDS => {
            Some(MapiValue::MultiBinary(free_busy_entry_ids(mailbox_guid)))
        }
        _ => None,
    }
}

pub(in crate::mapi) fn is_default_folder_identification_property_tag(property_tag: u32) -> bool {
    is_scalar_default_folder_entry_id_property_tag(property_tag)
        || matches!(
            canonical_property_storage_tag(property_tag),
            PID_TAG_ADDITIONAL_REN_ENTRY_IDS
                | PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX
                | PID_TAG_FREE_BUSY_ENTRY_IDS
        )
}

pub(in crate::mapi) fn is_scalar_default_folder_entry_id_property_tag(property_tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(property_tag),
        PID_TAG_IPM_SUBTREE_ENTRY_ID
            | PID_TAG_IPM_OUTBOX_ENTRY_ID
            | PID_TAG_IPM_WASTEBASKET_ENTRY_ID
            | PID_TAG_IPM_SENTMAIL_ENTRY_ID
            | PID_TAG_VIEWS_ENTRY_ID
            | PID_TAG_COMMON_VIEWS_ENTRY_ID
            | PID_TAG_FINDER_ENTRY_ID
            | PID_TAG_IPM_ARCHIVE_ENTRY_ID
            | PID_TAG_IPM_APPOINTMENT_ENTRY_ID
            | PID_TAG_IPM_CONTACT_ENTRY_ID
            | PID_TAG_IPM_JOURNAL_ENTRY_ID
            | PID_TAG_IPM_NOTE_ENTRY_ID
            | PID_TAG_IPM_TASK_ENTRY_ID
            | PID_TAG_REM_ONLINE_ENTRY_ID
            | PID_TAG_REM_OFFLINE_ENTRY_ID
            | PID_TAG_IPM_DRAFTS_ENTRY_ID
    )
}

pub(in crate::mapi) fn ipm_subtree_ost_ostid(principal: &AccountPrincipal) -> Vec<u8> {
    let mut value = Vec::with_capacity(20);
    value.extend_from_slice(principal.account_id.as_bytes());
    value.extend_from_slice(&1u32.to_le_bytes());
    value
}

fn valid_folder_mask() -> u32 {
    FOLDER_IPM_SUBTREE_VALID
        | FOLDER_IPM_INBOX_VALID
        | FOLDER_IPM_OUTBOX_VALID
        | FOLDER_IPM_WASTEBASKET_VALID
        | FOLDER_IPM_SENTMAIL_VALID
        | FOLDER_VIEWS_VALID
        | FOLDER_COMMON_VIEWS_VALID
        | FOLDER_FINDER_VALID
}

fn special_folder_entry_id_value(mailbox_guid: Uuid, folder_id: u64) -> MapiValue {
    MapiValue::Binary(special_folder_entry_id(mailbox_guid, folder_id))
}

fn additional_ren_entry_ids(mailbox_guid: Uuid) -> Vec<Vec<u8>> {
    [
        CONFLICTS_FOLDER_ID,
        SYNC_ISSUES_FOLDER_ID,
        LOCAL_FAILURES_FOLDER_ID,
        SERVER_FAILURES_FOLDER_ID,
        JUNK_FOLDER_ID,
    ]
    .into_iter()
    .map(|folder_id| special_folder_entry_id(mailbox_guid, folder_id))
    .collect()
}

fn additional_ren_entry_ids_ex(mailbox_guid: Uuid) -> Vec<u8> {
    let entries = [
        (0x8001u16, RSS_FEEDS_FOLDER_ID),
        (0x8002, TRACKED_MAIL_PROCESSING_FOLDER_ID),
        (0x8004, TODO_SEARCH_FOLDER_ID),
        (0x8006, CONVERSATION_ACTION_SETTINGS_FOLDER_ID),
        (0x8007, QUICK_STEP_SETTINGS_FOLDER_ID),
        (0x8008, SUGGESTED_CONTACTS_FOLDER_ID),
        (0x8009, CONTACTS_SEARCH_FOLDER_ID),
        (0x800A, IM_CONTACT_LIST_FOLDER_ID),
        (0x800B, QUICK_CONTACTS_FOLDER_ID),
        (0x800F, ARCHIVE_FOLDER_ID),
    ];
    let mut value = Vec::new();
    for (persist_id, folder_id) in entries {
        let entry_id = special_folder_entry_id(mailbox_guid, folder_id);
        let data_size = 4usize.saturating_add(entry_id.len());
        value.extend_from_slice(&persist_id.to_le_bytes());
        value.extend_from_slice(&(data_size.min(u16::MAX as usize) as u16).to_le_bytes());
        value.extend_from_slice(&0x0001u16.to_le_bytes());
        value.extend_from_slice(&(entry_id.len().min(u16::MAX as usize) as u16).to_le_bytes());
        value.extend_from_slice(&entry_id);
    }
    value.extend_from_slice(&0u16.to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
    value
}

fn free_busy_entry_ids(mailbox_guid: Uuid) -> Vec<Vec<u8>> {
    vec![
        Vec::new(),
        Vec::new(),
        Vec::new(),
        special_folder_entry_id(mailbox_guid, FREEBUSY_DATA_FOLDER_ID),
    ]
}

fn special_folder_entry_id(mailbox_guid: Uuid, folder_id: u64) -> Vec<u8> {
    crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, folder_id)
        .expect("special folders use valid MAPI folder IDs")
}

pub(in crate::mapi) fn mailbox_owner_entry_id(principal: &AccountPrincipal) -> Vec<u8> {
    let entry = super::nspi::principal_address_book_entry(principal);
    let legacy_dn = super::nspi::nspi_entry_unprefixed_legacy_dn(&entry);
    let mut value = Vec::with_capacity(28 + legacy_dn.len() + 1);
    value.extend_from_slice(&[0, 0, 0, 0]);
    value.extend_from_slice(&NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID);
    value.extend_from_slice(&1u32.to_le_bytes());
    value.extend_from_slice(&super::nspi::nspi_entry_display_type(&entry).to_le_bytes());
    value.extend_from_slice(legacy_dn.as_bytes());
    value.push(0);
    value
}

pub(in crate::mapi) fn sent_representing_entry_id(email: &JmapEmail) -> Vec<u8> {
    let entry = ExchangeAddressBookEntry {
        id: email.submitted_by_account_id,
        display_name: email_sent_representing_name(email).to_string(),
        email: email_sent_representing_address(email).to_string(),
        entry_kind: ExchangeAddressBookEntryKind::Account,
        directory_kind: ExchangeAddressBookDirectoryKind::Person,
        member_emails: Vec::new(),
        details: ExchangeAddressBookEntryDetails::default(),
    };
    super::nspi::nspi_entry_permanent_entry_id(&entry)
}

pub(in crate::mapi) fn rop_read_recipients_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let input_handle_index = request.input_handle_index().unwrap_or(0);
    let start = request.row_id().unwrap_or(0) as usize;

    let mut response = vec![0x0F, input_handle_index];
    write_u32(&mut response, 0);

    let available_rows = match object {
        Some(MapiObject::Message {
            folder_id,
            message_id,
            saved_email,
            ..
        }) => {
            let Some(email) = message_for_id(*folder_id, *message_id, mailboxes, emails)
                .or_else(|| {
                    search_folder_message_for_id(snapshot, *folder_id, *message_id)
                        .map(|message| &message.email)
                })
                .or(saved_email.as_ref().map(|saved| &saved.email))
            else {
                return rop_error_response(0x0F, input_handle_index, 0x8004_010F);
            };
            message_recipients(email)
                .into_iter()
                .enumerate()
                .map(|(offset, recipient)| {
                    (
                        offset as u32,
                        recipient.recipient_type,
                        serialize_recipient_row(recipient.address),
                    )
                })
                .collect::<Vec<_>>()
        }
        Some(MapiObject::PendingMessage { recipients, .. }) => recipients
            .iter()
            .map(|recipient| {
                (
                    recipient.row_id,
                    recipient.recipient_type,
                    serialize_pending_recipient_row(recipient),
                )
            })
            .collect::<Vec<_>>(),
        _ => return rop_error_response(0x0F, input_handle_index, 0x0000_04B9),
    };
    if available_rows.is_empty() {
        return rop_error_response(0x0F, input_handle_index, 0x8004_010F);
    }
    let start_index = if start == 0 {
        0
    } else if let Some(index) = available_rows
        .iter()
        .position(|(row_id, _, _)| *row_id == start as u32)
    {
        index
    } else {
        return rop_error_response(0x0F, input_handle_index, 0x8004_010F);
    };
    let mut row_count = 0usize;
    let mut rows = Vec::new();
    for (row_id, recipient_type, row) in available_rows
        .iter()
        .skip(start_index)
        .take(u8::MAX as usize)
    {
        write_u32(&mut rows, *row_id);
        rows.push(*recipient_type);
        rows.extend_from_slice(&0x0FFFu16.to_le_bytes());
        rows.extend_from_slice(&0u16.to_le_bytes());
        rows.extend_from_slice(&(row.len() as u16).to_le_bytes());
        rows.extend_from_slice(row);
        row_count += 1;
    }
    response.push(row_count.min(u8::MAX as usize) as u8);
    response.extend_from_slice(&rows);
    response
}

pub(in crate::mapi) fn rop_set_message_read_flag_response(
    request: &RopRequest,
    read_status_changed: bool,
) -> Vec<u8> {
    let mut response = vec![0x11, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(read_status_changed as u8);
    response
}

pub(in crate::mapi) fn search_folder_message_for_id(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
    message_id: u64,
) -> Option<&MapiMessage> {
    match folder_id {
        TODO_SEARCH_FOLDER_ID => snapshot.todo_search_message_for_id(message_id),
        TRACKED_MAIL_PROCESSING_FOLDER_ID => {
            snapshot.tracked_mail_processing_message_for_id(message_id)
        }
        REMINDERS_FOLDER_ID => snapshot.reminder_message_for_id(message_id),
        _ => None,
    }
}

pub(in crate::mapi) fn restriction_matches_mailbox_with_context_for_account(
    restriction: Option<&MapiRestriction>,
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
    mailbox_guid: Uuid,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        mailbox_property_value_with_context_for_account(
            mailbox,
            mailboxes,
            property_tag,
            mailbox_guid,
        )
    })
}

pub(in crate::mapi) fn restriction_matches_collaboration_folder(
    restriction: Option<&MapiRestriction>,
    folder: &MapiCollaborationFolder,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        collaboration_folder_property_value(folder, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_public_folder(
    restriction: Option<&MapiRestriction>,
    folder: &MapiPublicFolder,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        public_folder_property_value(folder, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_email(
    restriction: Option<&MapiRestriction>,
    email: &JmapEmail,
) -> bool {
    restriction_matches_email_with_attachments(restriction, email, &[])
}

pub(in crate::mapi) fn restriction_matches_email_with_attachments(
    restriction: Option<&MapiRestriction>,
    email: &JmapEmail,
    attachments: &[MapiAttachment],
) -> bool {
    let Some(restriction) = restriction else {
        return true;
    };
    match restriction {
        MapiRestriction::InvalidTableRestriction => false,
        MapiRestriction::And(children) => children.iter().all(|child| {
            restriction_matches_email_with_attachments(Some(child), email, attachments)
        }),
        MapiRestriction::Or(children) => children.iter().any(|child| {
            restriction_matches_email_with_attachments(Some(child), email, attachments)
        }),
        MapiRestriction::Not(child) => {
            !restriction_matches_email_with_attachments(Some(child), email, attachments)
        }
        MapiRestriction::SubObject { subobject, child } => {
            match canonical_property_storage_tag(*subobject) {
                PID_TAG_MESSAGE_RECIPIENTS => message_recipients(email).iter().any(|recipient| {
                    restriction_matches(Some(child), |property_tag| {
                        recipient_property_value(recipient, property_tag)
                    })
                }),
                PID_TAG_MESSAGE_ATTACHMENTS => attachments
                    .iter()
                    .any(|attachment| restriction_matches_attachment(Some(child), attachment)),
                _ => false,
            }
        }
        MapiRestriction::Count { count, child } => {
            *count > 0
                && restriction_matches_email_with_attachments(Some(child), email, attachments)
        }
        MapiRestriction::Content {
            property_tag,
            value,
            fuzzy_level_low,
            fuzzy_level_high,
        } => email_property_value(email, *property_tag)
            .and_then(|property| property.into_text())
            .is_some_and(|property| {
                content_restriction_matches(&property, value, *fuzzy_level_low, *fuzzy_level_high)
            }),
        MapiRestriction::Property {
            relop,
            property_tag,
            value,
        } => email_property_value(email, *property_tag)
            .is_some_and(|property| compare_mapi_values(&property, value, *relop)),
        MapiRestriction::CompareProperties {
            relop,
            left_property_tag,
            right_property_tag,
        } => email_property_value(email, *left_property_tag).is_some_and(|left| {
            email_property_value(email, *right_property_tag)
                .is_some_and(|right| compare_mapi_values(&left, &right, *relop))
        }),
        MapiRestriction::Bitmask {
            property_tag,
            mask,
            must_be_nonzero,
        } => email_property_value(email, *property_tag)
            .and_then(|value| value.into_u32())
            .is_some_and(|value| ((value & mask) != 0) == *must_be_nonzero),
        MapiRestriction::Size {
            relop,
            property_tag,
            size,
        } => email_property_value(email, *property_tag)
            .map(|value| value.size() as i64)
            .is_some_and(|actual| compare_i64(actual, *size as i64, *relop)),
        MapiRestriction::Exist { property_tag } => {
            email_property_value(email, *property_tag).is_some()
        }
    }
}

fn recipient_property_value(recipient: &MapiRecipient<'_>, property_tag: u32) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    let display_name = recipient
        .address
        .display_name
        .as_deref()
        .unwrap_or(&recipient.address.address);
    match property_tag {
        PID_TAG_RECIPIENT_TYPE => Some(MapiValue::U32(u32::from(recipient.recipient_type))),
        PID_TAG_RECIPIENT_ORDER => Some(MapiValue::U32(recipient.order)),
        PID_TAG_RECIPIENT_FLAGS => Some(MapiValue::U32(1)),
        PID_TAG_RECIPIENT_TRACK_STATUS => Some(MapiValue::U32(0)),
        PID_TAG_DISPLAY_NAME_W | PID_TAG_RECIPIENT_DISPLAY_NAME_W => {
            Some(MapiValue::String(display_name.to_string()))
        }
        PID_TAG_EMAIL_ADDRESS_W | PID_TAG_SMTP_ADDRESS_W => {
            Some(MapiValue::String(recipient.address.address.clone()))
        }
        PID_TAG_ADDRESS_BOOK_DISPLAY_NAME_PRINTABLE_W => {
            Some(MapiValue::String(display_name.to_string()))
        }
        0x3002_001F => Some(MapiValue::String("SMTP".to_string())),
        _ => None,
    }
}

pub(in crate::mapi) fn restriction_matches_contact_in_folder(
    restriction: Option<&MapiRestriction>,
    contact: &AccessibleContact,
    folder_id: u64,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        contact_property_value(contact, mapi_item_id(&contact.id), folder_id, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_task(
    restriction: Option<&MapiRestriction>,
    task: &ClientTask,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        task_property_value(task, mapi_item_id(&task.id), TASKS_FOLDER_ID, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_note(
    restriction: Option<&MapiRestriction>,
    note: &ClientNote,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        note_property_value(note, mapi_item_id(&note.id), NOTES_FOLDER_ID, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_journal_entry(
    restriction: Option<&MapiRestriction>,
    entry: &JournalEntry,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        journal_entry_property_value(
            entry,
            mapi_item_id(&entry.id),
            JOURNAL_FOLDER_ID,
            property_tag,
        )
    })
}

pub(in crate::mapi) fn restriction_matches_attachment(
    restriction: Option<&MapiRestriction>,
    attachment: &MapiAttachment,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        attachment_property_value(attachment, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_navigation_shortcut(
    restriction: Option<&MapiRestriction>,
    message: &MapiNavigationShortcutMessage,
    account_id: Uuid,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        navigation_shortcut_property_value(message, account_id, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_common_view_named_view(
    restriction: Option<&MapiRestriction>,
    message: &MapiCommonViewNamedViewMessage,
    account_id: Uuid,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        common_view_named_view_property_value(message, account_id, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_associated_config(
    restriction: Option<&MapiRestriction>,
    message: &MapiAssociatedConfigMessage,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        associated_config_property_value(message, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches(
    restriction: Option<&MapiRestriction>,
    value_for: impl Copy + Fn(u32) -> Option<MapiValue>,
) -> bool {
    let Some(restriction) = restriction else {
        return true;
    };
    match restriction {
        MapiRestriction::InvalidTableRestriction => false,
        MapiRestriction::And(children) => children
            .iter()
            .all(|child| restriction_matches(Some(child), value_for)),
        MapiRestriction::Or(children) => children
            .iter()
            .any(|child| restriction_matches(Some(child), value_for)),
        MapiRestriction::Not(child) => !restriction_matches(Some(child), value_for),
        MapiRestriction::Content {
            property_tag,
            value,
            fuzzy_level_low,
            fuzzy_level_high,
        } => value_for(*property_tag)
            .and_then(|property| property.into_text())
            .is_some_and(|property| {
                content_restriction_matches(&property, value, *fuzzy_level_low, *fuzzy_level_high)
            }),
        MapiRestriction::Property {
            relop,
            property_tag,
            value,
        } => value_for(*property_tag)
            .is_some_and(|property| compare_mapi_values(&property, value, *relop)),
        MapiRestriction::CompareProperties {
            relop,
            left_property_tag,
            right_property_tag,
        } => value_for(*left_property_tag).is_some_and(|left| {
            value_for(*right_property_tag)
                .is_some_and(|right| compare_mapi_values(&left, &right, *relop))
        }),
        MapiRestriction::Bitmask {
            property_tag,
            mask,
            must_be_nonzero,
        } => value_for(*property_tag)
            .and_then(|value| value.into_u32())
            .is_some_and(|value| ((value & mask) != 0) == *must_be_nonzero),
        MapiRestriction::Size {
            relop,
            property_tag,
            size,
        } => value_for(*property_tag)
            .map(|value| value.size() as i64)
            .is_some_and(|actual| compare_i64(actual, *size as i64, *relop)),
        MapiRestriction::Exist { property_tag } => value_for(*property_tag).is_some(),
        MapiRestriction::Count { count, child } => {
            *count > 0 && restriction_matches(Some(child), value_for)
        }
        MapiRestriction::SubObject { .. } => false,
    }
}

fn content_restriction_matches(
    property: &str,
    value: &str,
    fuzzy_level_low: u16,
    fuzzy_level_high: u16,
) -> bool {
    let ignore_case = fuzzy_level_high & 0x0001 != 0 || fuzzy_level_high & 0x0004 != 0;
    let (property, value) = if ignore_case {
        (property.to_ascii_lowercase(), value.to_ascii_lowercase())
    } else {
        (property.to_string(), value.to_string())
    };

    match fuzzy_level_low {
        0x0000 => property == value,
        0x0002 => property.starts_with(&value),
        _ => property.contains(&value),
    }
}

#[cfg(test)]
pub(in crate::mapi) fn mailbox_property_value_with_context(
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
    property_tag: u32,
) -> Option<MapiValue> {
    mailbox_property_value_with_context_for_account(mailbox, mailboxes, property_tag, Uuid::nil())
}

pub(in crate::mapi) fn mailbox_property_value_with_context_for_account(
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
    property_tag: u32,
    mailbox_guid: Uuid,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    if matches!(mapi_folder_id(mailbox), ROOT_FOLDER_ID | INBOX_FOLDER_ID) {
        if let Some(value) =
            special_folder_identification_property_value(mailbox_guid, property_tag)
        {
            return Some(value);
        }
    }
    match property_tag {
        PID_TAG_DISPLAY_NAME_W => Some(MapiValue::String(mapi_mailbox_display_name(mailbox))),
        PID_TAG_CONTENT_COUNT => Some(MapiValue::U32(mailbox.total_emails)),
        PID_TAG_CONTENT_UNREAD_COUNT => Some(MapiValue::U32(mailbox.unread_emails)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(mailbox.size_octets as i64)),
        PID_TAG_MESSAGE_SIZE_EXTENDED => {
            Some(mapi_message_size_extended_value(mailbox.size_octets as i64))
        }
        PID_TAG_SUBFOLDERS => Some(MapiValue::Bool(mailbox_has_subfolders(mailbox, mailboxes))),
        PID_TAG_FOLDER_TYPE => Some(MapiValue::U32(
            if mailbox.role == "__mapi_search" || mailbox.role.starts_with("__mapi_search_folder_")
            {
                FOLDER_SEARCH
            } else {
                FOLDER_GENERIC
            },
        )),
        PID_TAG_ACCESS | PID_TAG_RIGHTS => Some(MapiValue::U32(MAPI_FOLDER_ACCESS)),
        PID_TAG_EXTENDED_FOLDER_FLAGS => Some(MapiValue::Binary(extended_folder_flags_for_folder(
            mapi_folder_id(mailbox),
        ))),
        PID_TAG_ARCHIVE_TAG | PID_TAG_POLICY_TAG => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_RETENTION_PERIOD | PID_TAG_RETENTION_FLAGS | PID_TAG_ARCHIVE_PERIOD => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_FOLDER_WEBVIEWINFO | PID_TAG_FOLDER_XVIEWINFO_E => {
            Some(MapiValue::Binary(Vec::new()))
        }
        OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_FOLDER_FORM_FLAGS | PID_TAG_FOLDER_VIEWS_ONLY | PID_TAG_FOLDER_VIEWLIST_FLAGS => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_DEFAULT_FORM_NAME_W => Some(MapiValue::String(String::new())),
        PID_TAG_DEFAULT_VIEW_ENTRY_ID
            if default_view_supported_folder(
                mapi_folder_id(mailbox),
                folder_message_class(mailbox),
            ) =>
        {
            default_folder_view_entry_id(
                mailbox_guid,
                mapi_folder_id(mailbox),
                folder_message_class(mailbox),
            )
        }
        tag if is_acl_member_name_property_tag(tag) => Some(MapiValue::String(String::new())),
        PID_TAG_FOLDER_FORM_STORAGE => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_ATTRIBUTE_HIDDEN => {
            Some(MapiValue::Bool(mailbox_projects_hidden_attribute(mailbox)))
        }
        PID_TAG_CONTAINER_CLASS_W => Some(MapiValue::String(folder_message_class(mailbox).into())),
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => {
            default_post_message_class_for_container_class(folder_message_class(mailbox))
                .map(|message_class| MapiValue::String(message_class.to_string()))
        }
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(mapi_folder_id(mailbox))),
        PID_TAG_ENTRY_ID => crate::mapi::identity::folder_entry_id_from_object_id(
            mailbox_guid,
            mapi_folder_id(mailbox),
        )
        .map(MapiValue::Binary),
        PID_TAG_RECORD_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_mailbox_folder(mailbox),
        )),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(mapi_folder_id(mailbox)),
        )),
        PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME
        | PID_TAG_LOCAL_COMMIT_TIME_MAX
        | PID_TAG_HIER_REV => Some(MapiValue::U64(mapi_mailstore::filetime_from_change_number(
            mapi_mailstore::canonical_folder_change_number(mailbox),
        ))),
        PID_TAG_DELETED_COUNT_TOTAL => Some(MapiValue::U32(0)),
        PID_TAG_SERIALIZED_REPLID_GUID_MAP => Some(MapiValue::Binary(serialized_replid_guid_map())),
        PID_TAG_HIERARCHY_CHANGE_NUMBER => Some(MapiValue::U32(
            mapi_mailstore::canonical_folder_change_number(mailbox).min(u64::from(u32::MAX)) as u32,
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_mailbox_folder(mailbox),
        )),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(mailbox_parent_folder_id(mailbox, mailboxes)),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(
                mapi_mailstore::canonical_folder_change_number(mailbox),
            ),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => {
            Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
                mapi_mailstore::canonical_folder_change_number(mailbox),
            )))
        }
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(
            mapi_mailstore::canonical_folder_change_number(mailbox),
        )),
        _ => None,
    }
}

pub(in crate::mapi) fn search_folder_definition_property_value(
    definition: &SearchFolderDefinition,
    folder_id: u64,
    property_tag: u32,
    mailbox_guid: Uuid,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    let parent_folder_id = SEARCH_FOLDER_ID;
    let message_class =
        search_folder_container_class_for_result_kind(&definition.result_object_kind);
    let change_number = mapi_mailstore::change_number_for_store_id(folder_id);
    match property_tag {
        PID_TAG_DISPLAY_NAME_W => Some(MapiValue::String(definition.display_name.clone())),
        PID_TAG_ENTRY_ID => {
            crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, folder_id)
                .map(MapiValue::Binary)
        }
        PID_TAG_RECORD_KEY | PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(folder_id),
        )),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(folder_id),
        )),
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(folder_id)),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(parent_folder_id)),
        PID_TAG_FOLDER_TYPE => Some(MapiValue::U32(FOLDER_SEARCH)),
        PID_TAG_CONTENT_COUNT
        | PID_TAG_CONTENT_UNREAD_COUNT
        | PID_TAG_DELETED_COUNT_TOTAL
        | PID_TAG_ASSOCIATED_CONTENT_COUNT => Some(MapiValue::U32(0)),
        PID_TAG_ACCESS | PID_TAG_RIGHTS => Some(MapiValue::U32(MAPI_FOLDER_ACCESS)),
        PID_TAG_EXTENDED_FOLDER_FLAGS => Some(MapiValue::Binary(
            extended_folder_flags_for_search_folder(definition, folder_id),
        )),
        PID_TAG_SEARCH_FOLDER_ID => Some(MapiValue::Binary(search_folder_id(definition))),
        PID_TAG_ARCHIVE_TAG | PID_TAG_POLICY_TAG => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_RETENTION_PERIOD | PID_TAG_RETENTION_FLAGS | PID_TAG_ARCHIVE_PERIOD => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_FOLDER_WEBVIEWINFO | PID_TAG_FOLDER_XVIEWINFO_E => {
            Some(MapiValue::Binary(Vec::new()))
        }
        OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_FOLDER_FORM_FLAGS | PID_TAG_FOLDER_VIEWS_ONLY | PID_TAG_FOLDER_VIEWLIST_FLAGS => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_DEFAULT_FORM_NAME_W => Some(MapiValue::String(String::new())),
        PID_TAG_DEFAULT_VIEW_ENTRY_ID
            if default_view_supported_folder(folder_id, message_class) =>
        {
            default_folder_view_entry_id(mailbox_guid, folder_id, message_class)
        }
        tag if is_acl_member_name_property_tag(tag) => Some(MapiValue::String(String::new())),
        PID_TAG_FOLDER_FORM_STORAGE => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_SUBFOLDERS => Some(MapiValue::Bool(false)),
        PID_TAG_ATTRIBUTE_HIDDEN => Some(MapiValue::Bool(false)),
        PID_TAG_CONTAINER_CLASS_W | PID_TAG_MESSAGE_CLASS_W => {
            Some(MapiValue::String(message_class.to_string()))
        }
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => {
            default_post_message_class_for_container_class(message_class)
                .map(|default_class| MapiValue::String(default_class.to_string()))
        }
        PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME
        | PID_TAG_LOCAL_COMMIT_TIME_MAX
        | PID_TAG_HIER_REV => Some(MapiValue::I64(mapi_mailstore::filetime_from_change_number(
            change_number,
        ) as i64)),
        PID_TAG_HIERARCHY_CHANGE_NUMBER => {
            Some(MapiValue::U32(change_number.min(u64::from(u32::MAX)) as u32))
        }
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(parent_folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ => None,
    }
}

fn search_folder_container_class_for_result_kind(result_object_kind: &str) -> &'static str {
    match result_object_kind {
        "contact" => "IPF.Contact",
        "task" => "IPF.Task",
        "mixed" | "message" => "IPF.Note",
        _ => "IPF.Note",
    }
}

pub(in crate::mapi) fn search_folder_definition_message_property_value(
    definition: &SearchFolderDefinition,
    account_id: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    let message_id = search_folder_definition_message_id(definition)?;
    let change_number = mapi_mailstore::change_number_for_store_id(message_id);
    match property_tag {
        PID_TAG_FOLDER_ID | PID_TAG_PARENT_FOLDER_ID => {
            Some(MapiValue::U64(COMMON_VIEWS_FOLDER_ID))
        }
        PID_TAG_MID | PID_TAG_INST_ID => Some(MapiValue::U64(message_id)),
        PID_TAG_INSTANCE_NUM => Some(MapiValue::U32(0)),
        PID_TAG_ENTRY_ID => crate::mapi::identity::message_entry_id_from_object_ids(
            account_id,
            COMMON_VIEWS_FOLDER_ID,
            message_id,
        )
        .map(MapiValue::Binary),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(message_id),
        )),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(definition.display_name.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(
            "IPM.Microsoft.WunderBar.SFInfo".to_string(),
        )),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_FAI)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I32(128)),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(MapiValue::I64(128)),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_ASSOCIATED => Some(MapiValue::Bool(true)),
        PID_TAG_SOURCE_KEY | PID_TAG_RECORD_KEY | PID_TAG_SEARCH_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_uuid(&definition.id),
        )),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(COMMON_VIEWS_FOLDER_ID),
        )),
        PID_TAG_PARENT_ENTRY_ID => crate::mapi::identity::folder_entry_id_from_object_id(
            account_id,
            COMMON_VIEWS_FOLDER_ID,
        )
        .map(MapiValue::Binary),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        PID_TAG_SEARCH_FOLDER_ID => Some(MapiValue::Binary(search_folder_id(definition))),
        PID_TAG_SEARCH_FOLDER_TEMPLATE_ID => {
            Some(MapiValue::U32(search_folder_template_id(definition)))
        }
        PID_TAG_SEARCH_FOLDER_TAG => Some(MapiValue::U32(search_folder_tag(definition))),
        PID_TAG_SEARCH_FOLDER_LAST_USED => Some(MapiValue::U32(search_folder_last_used())),
        PID_TAG_SEARCH_FOLDER_EXPIRATION => Some(MapiValue::U32(search_folder_expiration())),
        PID_TAG_SEARCH_FOLDER_STORAGE_TYPE => {
            Some(MapiValue::U32(search_folder_storage_type(definition)))
        }
        PID_TAG_SEARCH_FOLDER_EFP_FLAGS => Some(MapiValue::U32(0)),
        PID_TAG_SEARCH_FOLDER_DEFINITION => {
            Some(MapiValue::Binary(search_folder_definition_blob(definition)))
        }
        PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_change_number(change_number),
        )),
        _ => None,
    }
}

fn search_folder_definition_message_id(definition: &SearchFolderDefinition) -> Option<u64> {
    crate::mapi::identity::mapped_mapi_object_id(&definition.id)
}

fn search_folder_template_id(definition: &SearchFolderDefinition) -> u32 {
    if let Some(value) = definition
        .restriction_json
        .get("pidTagSearchFolderTemplateId")
        .and_then(serde_json::Value::as_u64)
    {
        return value.min(u64::from(u32::MAX)) as u32;
    }
    match definition.role.as_str() {
        "todo_search" => 10,
        _ if definition.is_builtin => 2,
        _ => 0,
    }
}

fn search_folder_storage_type(definition: &SearchFolderDefinition) -> u32 {
    if let Some(value) = definition
        .restriction_json
        .get("pidTagSearchFolderStorageType")
        .and_then(serde_json::Value::as_u64)
    {
        return value.min(u64::from(u32::MAX)) as u32;
    }
    if let Some(value) = definition
        .restriction_json
        .get("pidTagSearchFolderDefinition")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| BASE64_STANDARD.decode(value).ok())
        .and_then(|blob| {
            blob.get(4..8)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u32::from_le_bytes)
        })
    {
        return value;
    }
    let mut storage_type = 0x48;
    if search_folder_text_search(definition).is_some() {
        storage_type |= 0x02;
    }
    if search_folder_numerical_search(definition).is_some() {
        storage_type |= 0x01;
    }
    storage_type
}

fn search_folder_definition_blob(definition: &SearchFolderDefinition) -> Vec<u8> {
    if let Some(value) = definition
        .restriction_json
        .get("pidTagSearchFolderDefinition")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| BASE64_STANDARD.decode(value).ok())
    {
        return value;
    }
    let mut blob = Vec::new();
    blob.extend_from_slice(&0x0000_1004u32.to_le_bytes());
    blob.extend_from_slice(&search_folder_storage_type(definition).to_le_bytes());
    blob.extend_from_slice(&search_folder_numerical_search(definition).unwrap_or([0; 4]));
    write_search_folder_text_search(&mut blob, search_folder_text_search(definition).as_deref());
    blob.extend_from_slice(&0u32.to_le_bytes());
    blob.extend_from_slice(
        &(definition
            .scope_json
            .get("recursive")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(definition.is_builtin) as u32)
            .to_le_bytes(),
    );
    blob.push(0);
    blob.extend_from_slice(&0u32.to_le_bytes());
    blob.extend_from_slice(&0u32.to_le_bytes());
    blob
}

fn search_folder_text_search(definition: &SearchFolderDefinition) -> Option<String> {
    definition
        .restriction_json
        .get("pidTagSearchFolderTextSearch")
        .or_else(|| definition.restriction_json.get("textSearch"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.chars().take(u16::MAX as usize).collect())
}

fn search_folder_numerical_search(definition: &SearchFolderDefinition) -> Option<[u8; 4]> {
    if let Some(age) = definition
        .restriction_json
        .get("pidTagSearchFolderNumericalSearchAge")
    {
        let unit = age
            .get("unit")
            .and_then(serde_json::Value::as_str)
            .and_then(|unit| match unit {
                "days" => Some(0u32),
                "weeks" => Some(1u32),
                "months" => Some(2u32),
                _ => None,
            })?;
        let amount = age
            .get("amount")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default()
            .min(u64::from(u16::MAX)) as u32;
        return Some(((unit << 16) | amount).to_be_bytes());
    }

    definition
        .restriction_json
        .get("pidTagSearchFolderNumericalSearch")
        .or_else(|| definition.restriction_json.get("numericalSearch"))
        .and_then(serde_json::Value::as_u64)
        .map(|value| (value.min(u64::from(u32::MAX)) as u32).to_le_bytes())
}

fn write_search_folder_text_search(blob: &mut Vec<u8>, text: Option<&str>) {
    let Some(text) = text else {
        blob.push(0);
        return;
    };
    let char_count = text.chars().count().min(u16::MAX as usize);
    if char_count > 254 {
        blob.push(255);
        blob.extend_from_slice(&(char_count as u16).to_le_bytes());
    } else {
        blob.push(char_count as u8);
    }
    blob.extend_from_slice(text.as_bytes());
}

fn search_folder_last_used() -> u32 {
    214_089_600
}

fn search_folder_expiration() -> u32 {
    214_089_641
}

fn search_folder_tag(definition: &SearchFolderDefinition) -> u32 {
    if let Some(value) = definition
        .restriction_json
        .get("pidTagSearchFolderTag")
        .and_then(serde_json::Value::as_u64)
    {
        return value.min(u64::from(u32::MAX)) as u32;
    }
    let bytes = definition.id.as_bytes();
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

pub(in crate::mapi) fn mapi_mailbox_display_name(mailbox: &JmapMailbox) -> String {
    if mailbox.role.eq_ignore_ascii_case("inbox") {
        "Inbox".to_string()
    } else if mailbox.role == "conversation_history" {
        "Conversation History".to_string()
    } else {
        mailbox.name.clone()
    }
}

pub(in crate::mapi) fn default_post_message_class_for_container_class(
    container_class: &str,
) -> Option<&'static str> {
    match container_class {
        "IPF.Note" => Some("IPM.Note"),
        class if class.starts_with("IPF.Note.") => Some("IPM.Note"),
        "IPF.Appointment" => Some("IPM.Appointment"),
        "IPF.Contact" | "IPF.Contact.MOC.QuickContacts" | "IPF.Contact.MOC.ImContactList" => {
            Some("IPM.Contact")
        }
        "IPF.Task" => Some("IPM.Task"),
        "IPF.StickyNote" => Some("IPM.StickyNote"),
        "IPF.Journal" => Some("IPM.Activity"),
        "IPF.Configuration" => Some("IPM.Configuration"),
        "Outlook.Reminder" => Some("IPM.Note"),
        _ => None,
    }
}

pub(in crate::mapi) fn extended_folder_flags() -> Vec<u8> {
    vec![0x01, 0x04, 0x00, 0x00, 0x10, 0x00]
}

pub(in crate::mapi) fn extended_folder_flags_for_folder(folder_id: u64) -> Vec<u8> {
    let mut flags = extended_folder_flags();
    if folder_id == TODO_SEARCH_FOLDER_ID {
        flags.extend_from_slice(&[0x05, 0x04]);
        flags.extend_from_slice(&0x000C_0000u32.to_le_bytes());
    }
    flags
}

fn extended_folder_flags_for_search_folder(
    definition: &SearchFolderDefinition,
    folder_id: u64,
) -> Vec<u8> {
    let mut flags = extended_folder_flags_for_folder(folder_id);
    flags.extend_from_slice(&[0x03, 0x04]);
    flags.extend_from_slice(&search_folder_tag(definition).to_le_bytes());
    flags.extend_from_slice(&[0x02, 0x10]);
    flags.extend_from_slice(&search_folder_id(definition));
    flags
}

fn search_folder_id(definition: &SearchFolderDefinition) -> Vec<u8> {
    definition.id.as_bytes().to_vec()
}

pub(in crate::mapi) fn default_view_supported_container_class(container_class: &str) -> bool {
    container_class == "IPF.Note"
        || container_class.starts_with("IPF.Note.")
        || container_class == "IPF.Contact"
        || container_class.starts_with("IPF.Contact.")
        || container_class == "IPF.Appointment"
        || container_class.starts_with("IPF.Appointment.")
        || container_class == "IPF.Task"
        || container_class.starts_with("IPF.Task.")
        || container_class == "IPF.StickyNote"
        || container_class.starts_with("IPF.StickyNote.")
        || container_class == "IPF.Journal"
        || container_class.starts_with("IPF.Journal.")
}

pub(in crate::mapi) fn default_view_supported_folder(
    folder_id: u64,
    container_class: &str,
) -> bool {
    if !default_view_supported_container_class(container_class) {
        return false;
    }
    if container_class == "IPF.Contact" || container_class.starts_with("IPF.Contact.") {
        return matches!(folder_id, CONTACTS_FOLDER_ID | CONTACTS_SEARCH_FOLDER_ID);
    }
    if container_class == "IPF.Appointment" || container_class.starts_with("IPF.Appointment.") {
        return folder_id == CALENDAR_FOLDER_ID;
    }
    if container_class == "IPF.Task" || container_class.starts_with("IPF.Task.") {
        return matches!(folder_id, TASKS_FOLDER_ID | TODO_SEARCH_FOLDER_ID);
    }
    if container_class == "IPF.StickyNote" || container_class.starts_with("IPF.StickyNote.") {
        return folder_id == NOTES_FOLDER_ID;
    }
    if container_class == "IPF.Journal" || container_class.starts_with("IPF.Journal.") {
        return folder_id == JOURNAL_FOLDER_ID;
    }
    if matches!(
        folder_id,
        INBOX_FOLDER_ID
            | OUTBOX_FOLDER_ID
            | SENT_FOLDER_ID
            | TRASH_FOLDER_ID
            | DRAFTS_FOLDER_ID
            | JUNK_FOLDER_ID
            | ARCHIVE_FOLDER_ID
            | CONVERSATION_HISTORY_FOLDER_ID
    ) {
        return true;
    }
    !matches!(
        folder_id,
        ROOT_FOLDER_ID
            | DEFERRED_ACTION_FOLDER_ID
            | SPOOLER_QUEUE_FOLDER_ID
            | IPM_SUBTREE_FOLDER_ID
            | COMMON_VIEWS_FOLDER_ID
            | SCHEDULE_FOLDER_ID
            | SEARCH_FOLDER_ID
            | VIEWS_FOLDER_ID
            | SHORTCUTS_FOLDER_ID
            | FREEBUSY_DATA_FOLDER_ID
            | SYNC_ISSUES_FOLDER_ID
            | CONFLICTS_FOLDER_ID
            | LOCAL_FAILURES_FOLDER_ID
            | SERVER_FAILURES_FOLDER_ID
            | RSS_FEEDS_FOLDER_ID
            | TRACKED_MAIL_PROCESSING_FOLDER_ID
            | TODO_SEARCH_FOLDER_ID
            | CONVERSATION_ACTION_SETTINGS_FOLDER_ID
            | RECOVERABLE_ITEMS_ROOT_FOLDER_ID
            | RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID
            | RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID
            | RECOVERABLE_ITEMS_PURGES_FOLDER_ID
            | QUICK_STEP_SETTINGS_FOLDER_ID
            | PUBLIC_FOLDERS_ROOT_FOLDER_ID
    )
}

pub(in crate::mapi) fn default_folder_view_entry_id(
    mailbox_guid: Uuid,
    folder_id: u64,
    container_class: &str,
) -> Option<MapiValue> {
    let (view_folder_id, view_id) = if default_view_uses_common_views(container_class, folder_id) {
        (
            COMMON_VIEWS_FOLDER_ID,
            crate::mapi_store::OUTLOOK_COMMON_VIEWS_COMPACT_NAMED_VIEW_ID,
        )
    } else {
        (
            folder_id,
            crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
        )
    };
    crate::mapi::identity::message_entry_id_from_object_ids(mailbox_guid, view_folder_id, view_id)
        .map(MapiValue::Binary)
}

pub(in crate::mapi) fn default_view_uses_common_views(
    container_class: &str,
    folder_id: u64,
) -> bool {
    let _ = (container_class, folder_id);
    false
}

fn mailbox_has_subfolders(mailbox: &JmapMailbox, mailboxes: &[JmapMailbox]) -> bool {
    if mapi_folder_id(mailbox) == SYNC_ISSUES_FOLDER_ID {
        return false;
    }
    !mailboxes.is_empty()
        && mailboxes
            .iter()
            .any(|candidate| candidate.parent_id == Some(mailbox.id))
}

fn mailbox_parent_folder_id(mailbox: &JmapMailbox, mailboxes: &[JmapMailbox]) -> u64 {
    match mapi_folder_id(mailbox) {
        IPM_SUBTREE_FOLDER_ID
        | DEFERRED_ACTION_FOLDER_ID
        | SPOOLER_QUEUE_FOLDER_ID
        | COMMON_VIEWS_FOLDER_ID
        | SCHEDULE_FOLDER_ID
        | SEARCH_FOLDER_ID
        | VIEWS_FOLDER_ID
        | SHORTCUTS_FOLDER_ID
        | REMINDERS_FOLDER_ID
        | DOCUMENT_LIBRARIES_FOLDER_ID
        | TRACKED_MAIL_PROCESSING_FOLDER_ID
        | TODO_SEARCH_FOLDER_ID
        | FREEBUSY_DATA_FOLDER_ID => return ROOT_FOLDER_ID,
        _ => {}
    }
    match mailbox.role.as_str() {
        "conflicts" | "local_failures" | "server_failures" => SYNC_ISSUES_FOLDER_ID,
        _ => mailbox
            .parent_id
            .and_then(|parent_id| mailboxes.iter().find(|candidate| candidate.id == parent_id))
            .map(|parent| mapi_folder_id(parent))
            .unwrap_or(IPM_SUBTREE_FOLDER_ID),
    }
}

pub(in crate::mapi) fn collaboration_folder_property_value(
    folder: &MapiCollaborationFolder,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    let change_number = mapi_mailstore::change_number_for_store_id(folder.id);
    match property_tag {
        PID_TAG_DISPLAY_NAME_W => Some(MapiValue::String(folder.collection.display_name.clone())),
        PID_TAG_CONTENT_COUNT => Some(MapiValue::U32(folder.item_count)),
        PID_TAG_CONTENT_UNREAD_COUNT => Some(MapiValue::U32(0)),
        PID_TAG_SUBFOLDERS => Some(MapiValue::Bool(false)),
        PID_TAG_FOLDER_TYPE => Some(MapiValue::U32(FOLDER_GENERIC)),
        PID_TAG_ACCESS | PID_TAG_RIGHTS => Some(MapiValue::U32(MAPI_FOLDER_ACCESS)),
        PID_TAG_EXTENDED_FOLDER_FLAGS => Some(MapiValue::Binary(extended_folder_flags())),
        PID_TAG_ARCHIVE_TAG | PID_TAG_POLICY_TAG => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_RETENTION_PERIOD | PID_TAG_RETENTION_FLAGS | PID_TAG_ARCHIVE_PERIOD => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_FOLDER_WEBVIEWINFO | PID_TAG_FOLDER_XVIEWINFO_E => {
            Some(MapiValue::Binary(Vec::new()))
        }
        OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_FOLDER_FORM_FLAGS | PID_TAG_FOLDER_VIEWS_ONLY | PID_TAG_FOLDER_VIEWLIST_FLAGS => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_DEFAULT_FORM_NAME_W => Some(MapiValue::String(String::new())),
        tag if is_acl_member_name_property_tag(tag) => Some(MapiValue::String(String::new())),
        PID_TAG_FOLDER_FORM_STORAGE => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_CONTAINER_CLASS_W => Some(MapiValue::String(
            collaboration_folder_message_class(folder.kind).to_string(),
        )),
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => {
            default_post_message_class_for_container_class(collaboration_folder_message_class(
                folder.kind,
            ))
            .map(|message_class| MapiValue::String(message_class.to_string()))
        }
        PID_TAG_DEFAULT_VIEW_ENTRY_ID
            if default_view_supported_folder(
                folder.id,
                collaboration_folder_message_class(folder.kind),
            ) =>
        {
            default_folder_view_entry_id(
                folder.collection.owner_account_id,
                folder.id,
                collaboration_folder_message_class(folder.kind),
            )
        }
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(folder.id)),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(IPM_SUBTREE_FOLDER_ID)),
        PID_TAG_ENTRY_ID => crate::mapi::identity::folder_entry_id_from_object_id(
            folder.collection.owner_account_id,
            folder.id,
        )
        .map(MapiValue::Binary),
        PID_TAG_RECORD_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            folder.id,
        ))),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(folder.id),
        )),
        PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME
        | PID_TAG_LOCAL_COMMIT_TIME_MAX
        | PID_TAG_HIER_REV => Some(MapiValue::U64(mapi_mailstore::filetime_from_change_number(
            change_number,
        ))),
        PID_TAG_DELETED_COUNT_TOTAL => Some(MapiValue::U32(0)),
        PID_TAG_SERIALIZED_REPLID_GUID_MAP => Some(MapiValue::Binary(serialized_replid_guid_map())),
        PID_TAG_HIERARCHY_CHANGE_NUMBER => {
            Some(MapiValue::U32(change_number.min(u64::from(u32::MAX)) as u32))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(
            collaboration_folder_message_class(folder.kind).to_string(),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            folder.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(IPM_SUBTREE_FOLDER_ID),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ => None,
    }
}

pub(in crate::mapi) fn public_folder_property_value(
    folder: &MapiPublicFolder,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    let change_number = mapi_mailstore::change_number_for_store_id(folder.id);
    let parent_folder_id = folder
        .folder
        .parent_folder_id
        .and_then(|parent_id| crate::mapi::identity::mapped_mapi_object_id(&parent_id))
        .unwrap_or(PUBLIC_FOLDERS_ROOT_FOLDER_ID);
    match property_tag {
        PID_TAG_DISPLAY_NAME_W => Some(MapiValue::String(folder.folder.display_name.clone())),
        PID_TAG_CONTENT_COUNT => Some(MapiValue::U32(folder.item_count)),
        PID_TAG_CONTENT_UNREAD_COUNT => Some(MapiValue::U32(0)),
        PID_TAG_SUBFOLDERS => Some(MapiValue::Bool(folder.child_count > 0)),
        PID_TAG_FOLDER_TYPE => Some(MapiValue::U32(FOLDER_GENERIC)),
        PID_TAG_ACCESS | PID_TAG_RIGHTS => Some(MapiValue::U32(MAPI_FOLDER_ACCESS)),
        PID_TAG_EXTENDED_FOLDER_FLAGS => Some(MapiValue::Binary(extended_folder_flags())),
        PID_TAG_ARCHIVE_TAG | PID_TAG_POLICY_TAG => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_RETENTION_PERIOD | PID_TAG_RETENTION_FLAGS | PID_TAG_ARCHIVE_PERIOD => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_FOLDER_WEBVIEWINFO | PID_TAG_FOLDER_XVIEWINFO_E => {
            Some(MapiValue::Binary(Vec::new()))
        }
        OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_FOLDER_FORM_FLAGS | PID_TAG_FOLDER_VIEWS_ONLY | PID_TAG_FOLDER_VIEWLIST_FLAGS => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_DEFAULT_FORM_NAME_W => Some(MapiValue::String(String::new())),
        tag if is_acl_member_name_property_tag(tag) => Some(MapiValue::String(String::new())),
        PID_TAG_FOLDER_FORM_STORAGE => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_CONTAINER_CLASS_W | PID_TAG_MESSAGE_CLASS_W => {
            Some(MapiValue::String(folder.folder.folder_class.clone()))
        }
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => {
            default_post_message_class_for_container_class(&folder.folder.folder_class)
                .map(|message_class| MapiValue::String(message_class.to_string()))
        }
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(folder.id)),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(parent_folder_id)),
        PID_TAG_ENTRY_ID => {
            crate::mapi::identity::folder_entry_id_from_object_id(Uuid::nil(), folder.id)
                .map(MapiValue::Binary)
        }
        PID_TAG_RECORD_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            folder.id,
        ))),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(folder.id),
        )),
        PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME
        | PID_TAG_LOCAL_COMMIT_TIME_MAX
        | PID_TAG_HIER_REV => Some(MapiValue::U64(mapi_mailstore::filetime_from_change_number(
            change_number,
        ))),
        PID_TAG_DELETED_COUNT_TOTAL => Some(MapiValue::U32(0)),
        PID_TAG_SERIALIZED_REPLID_GUID_MAP => Some(MapiValue::Binary(serialized_replid_guid_map())),
        PID_TAG_HIERARCHY_CHANGE_NUMBER => {
            Some(MapiValue::U32(change_number.min(u64::from(u32::MAX)) as u32))
        }
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            folder.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(parent_folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ => None,
    }
}

pub(in crate::mapi) fn email_property_value(
    email: &JmapEmail,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    if let Some(value) = rss_email_named_property_value(email, property_tag) {
        return Some(value);
    }
    if named_property_id_matches(property_tag, PID_NAME_KEYWORDS_TAG) {
        return Some(MapiValue::MultiString(email.categories.clone()));
    }
    match property_tag {
        PID_TAG_FOLDER_ID | PID_TAG_PARENT_FOLDER_ID => {
            Some(MapiValue::U64(mapi_folder_id_for_email(email)))
        }
        PID_TAG_MID => Some(MapiValue::U64(mapi_message_id(email))),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_CONVERSATION_TOPIC_W => {
            Some(MapiValue::String(email.subject.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W | PID_TAG_ORIGINAL_MESSAGE_CLASS_W => Some(MapiValue::String(
            message_class_for_email(email).to_string(),
        )),
        PID_TAG_CREATION_TIME
        | PID_TAG_MESSAGE_DELIVERY_TIME
        | PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_rfc3339_utc(&email.received_at),
        )),
        PID_TAG_CLIENT_SUBMIT_TIME => email
            .sent_at
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_ACCESS_LEVEL => Some(MapiValue::U32(1)),
        PID_TAG_IMPORTANCE => Some(MapiValue::U32(1)),
        PID_TAG_PRIORITY | PID_TAG_SENSITIVITY => Some(MapiValue::U32(0)),
        PID_TAG_SUBJECT_PREFIX_W => Some(MapiValue::String(String::new())),
        PID_TAG_MESSAGE_STATUS => Some(MapiValue::U32(0)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(message_flags(email))),
        PID_TAG_READ => Some(MapiValue::Bool(!email.unread)),
        PID_TAG_FLAG_STATUS => Some(MapiValue::U32(mapi_mailstore::canonical_flag_status(email))),
        PID_LID_OUTLOOK_APPOINTMENT_8F07_TAG | 0x8017_000B => Some(MapiValue::Bool(false)),
        PID_LID_OUTLOOK_COMMON_8514_TAG => Some(MapiValue::Bool(false)),
        PID_LID_PERCENT_COMPLETE_TAG => {
            Some(MapiValue::F64(email_percent_complete(email).to_bits()))
        }
        PID_TAG_FLAG_COMPLETE_TIME => email
            .followup_completed_at
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_TAG_FOLLOWUP_ICON => Some(MapiValue::I32(email.followup_icon)),
        PID_TAG_TODO_ITEM_FLAGS => Some(MapiValue::I32(email.todo_item_flags)),
        PID_TAG_SWAPPED_TODO_STORE => email
            .swapped_todo_store_id
            .map(|id| MapiValue::Binary(id.as_bytes().to_vec())),
        PID_TAG_SWAPPED_TODO_DATA => email
            .swapped_todo_data
            .as_ref()
            .map(|data| MapiValue::Binary(data.clone())),
        PID_LID_FLAG_REQUEST_W_TAG => Some(MapiValue::String(email.followup_request.clone())),
        PID_LID_TASK_START_DATE_TAG => email
            .followup_start_at
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_LID_TASK_DUE_DATE_TAG => email
            .followup_due_at
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_LID_REMINDER_SET_TAG => Some(MapiValue::Bool(email.reminder_set)),
        PID_LID_REMINDER_TIME_TAG | PID_LID_REMINDER_SIGNAL_TIME_TAG => email
            .reminder_at
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(email.size_octets)),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(email.size_octets)),
        OUTLOOK_COMPACT_VIEW_AUXILIARY_FLAGS_TAG => Some(MapiValue::U32(0)),
        PID_TAG_SENDER_NAME_W => Some(MapiValue::String(email_sender_name(email).to_string())),
        PID_TAG_SENDER_ADDRESS_TYPE_W => Some(MapiValue::String("SMTP".to_string())),
        PID_TAG_SENDER_EMAIL_ADDRESS_W | PID_TAG_SENDER_SMTP_ADDRESS_W => {
            Some(MapiValue::String(email_sender_address(email).to_string()))
        }
        PID_TAG_SENT_REPRESENTING_NAME_W => Some(MapiValue::String(
            email_sent_representing_name(email).to_string(),
        )),
        PID_TAG_SENT_REPRESENTING_ENTRY_ID => {
            Some(MapiValue::Binary(sent_representing_entry_id(email)))
        }
        PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W => Some(MapiValue::String("SMTP".to_string())),
        PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W | PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W => {
            Some(MapiValue::String(
                email_sent_representing_address(email).to_string(),
            ))
        }
        PID_TAG_DISPLAY_TO_W => Some(MapiValue::String(display_to(email))),
        PID_TAG_DISPLAY_CC_W => Some(MapiValue::String(display_cc(email))),
        PID_TAG_DISPLAY_BCC_W => Some(MapiValue::String(display_bcc(email))),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(email.has_attachments)),
        PID_TAG_RTF_IN_SYNC => Some(MapiValue::Bool(false)),
        PID_TAG_BODY_W => Some(MapiValue::String(email.body_text.clone())),
        PID_TAG_RTF_COMPRESSED => Some(MapiValue::Binary(uncompressed_rtf_body(&email.body_text))),
        PID_TAG_BODY_HTML_W => email
            .body_html_sanitized
            .clone()
            .or_else(|| html_body_from_plain_text(&email.body_text))
            .map(MapiValue::String),
        PID_TAG_HTML_BINARY => email
            .body_html_sanitized
            .clone()
            .or_else(|| html_body_from_plain_text(&email.body_text))
            .map(|value| MapiValue::Binary(value.into_bytes())),
        PID_TAG_NATIVE_BODY => Some(MapiValue::U32(native_body_format(email))),
        PID_TAG_INTERNET_CODEPAGE => Some(MapiValue::U32(65001)),
        PID_TAG_MESSAGE_LOCALE_ID => Some(MapiValue::U32(0x0409)),
        PID_TAG_CONVERSATION_INDEX => Some(MapiValue::Binary(conversation_index_for_uuid(
            email.thread_id,
        ))),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => {
            let object_id = mapi_message_id(email);
            Some(MapiValue::Binary(
                crate::mapi::identity::instance_key_for_object_id(object_id),
            ))
        }
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &email.id,
        ))),
        PID_TAG_SEARCH_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &email.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_mailbox_role(&email.mailbox_id, &email.mailbox_role),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(
                mapi_mailstore::canonical_message_change_number(email),
            ),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => {
            Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
                mapi_mailstore::canonical_message_change_number(email),
            )))
        }
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(
            mapi_mailstore::canonical_message_change_number(email),
        )),
        PID_TAG_INTERNET_MESSAGE_ID_W => email.internet_message_id.clone().map(MapiValue::String),
        PID_NAME_CONTENT_CLASS_W_TAG => {
            Some(MapiValue::String("urn:content-classes:message".to_string()))
        }
        PID_TAG_TRANSPORT_MESSAGE_HEADERS_W => Some(MapiValue::String(transport_headers(email))),
        _ => None,
    }
}

pub(in crate::mapi) fn email_sender_name(email: &JmapEmail) -> &str {
    email
        .sender_display
        .as_deref()
        .or(email.sender_address.as_deref())
        .or(email.from_display.as_deref())
        .unwrap_or(&email.from_address)
}

pub(in crate::mapi) fn email_sender_address(email: &JmapEmail) -> &str {
    email
        .sender_address
        .as_deref()
        .unwrap_or(&email.from_address)
}

pub(in crate::mapi) fn email_sent_representing_name(email: &JmapEmail) -> &str {
    email.from_display.as_deref().unwrap_or(&email.from_address)
}

pub(in crate::mapi) fn email_sent_representing_address(email: &JmapEmail) -> &str {
    &email.from_address
}

pub(in crate::mapi) fn navigation_shortcut_property_value(
    message: &MapiNavigationShortcutMessage,
    account_id: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    let requested_property_tag = property_tag;
    let property_tag = canonical_property_storage_tag(property_tag);
    match property_tag {
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
        PID_TAG_MID => Some(MapiValue::U64(message.id)),
        PID_TAG_INST_ID => Some(MapiValue::U64(message.id)),
        PID_TAG_INSTANCE_NUM => Some(MapiValue::U32(0)),
        PID_TAG_ENTRY_ID => crate::mapi::identity::message_entry_id_from_object_ids(
            account_id,
            message.folder_id,
            message.id,
        )
        .map(MapiValue::Binary),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(message.id),
        )),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(message.subject.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(
            "IPM.Microsoft.WunderBar.Link".to_string(),
        )),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_FAI)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I32(128)),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(MapiValue::I64(128)),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_ASSOCIATED => Some(MapiValue::Bool(true)),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            message.id,
        ))),
        PID_TAG_RECORD_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            message.id,
        ))),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(
                mapi_mailstore::change_number_for_store_id(message.id),
            ),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => {
            Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
                mapi_mailstore::change_number_for_store_id(message.id),
            )))
        }
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(message.folder_id),
        )),
        PID_TAG_PARENT_ENTRY_ID => {
            crate::mapi::identity::folder_entry_id_from_object_id(account_id, message.folder_id)
                .map(MapiValue::Binary)
        }
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(message.id & 0x00FF_FFFF_FFFF_FFFF)),
        PID_TAG_WLINK_SAVE_STAMP => Some(MapiValue::U32(wlink_save_stamp(message))),
        PID_TAG_WLINK_TYPE => Some(MapiValue::U32(message.shortcut_type)),
        PID_TAG_WLINK_FLAGS => Some(MapiValue::U32(message.flags)),
        PID_TAG_WLINK_SECTION => Some(MapiValue::U32(message.section)),
        PID_TAG_WLINK_ORDINAL => Some(MapiValue::Binary(wlink_ordinal_bytes(message.ordinal))),
        property_tag
            if property_tag_id(property_tag) == property_tag_id(PID_TAG_WLINK_GROUP_HEADER_ID) =>
        {
            let group_id = message
                .group_header_id
                .map(|group_id| *group_id.as_bytes())
                .unwrap_or_else(default_wlink_group_guid);
            Some(wlink_guid_property_value(requested_property_tag, group_id))
        }
        property_tag
            if property_tag_id(property_tag) == property_tag_id(PID_TAG_WLINK_GROUP_CLSID) =>
        {
            let group_id = message
                .group_header_id
                .map(|group_id| *group_id.as_bytes())
                .unwrap_or_else(default_wlink_group_guid);
            Some(wlink_guid_property_value(requested_property_tag, group_id))
        }
        PID_TAG_WLINK_GROUP_NAME_W => Some(MapiValue::String(wlink_group_name(message))),
        PID_TAG_WLINK_ENTRY_ID if message.shortcut_type != 4 => message
            .target_folder_id
            .and_then(|folder_id| {
                crate::mapi::identity::folder_entry_id_from_object_id(account_id, folder_id)
            })
            .map(MapiValue::Binary),
        property_tag
            if is_sharing_local_folder_id_property_tag(property_tag)
                && message.shortcut_type != 4 =>
        {
            message
                .target_folder_id
                .and_then(|folder_id| {
                    crate::mapi::identity::folder_entry_id_from_object_id(account_id, folder_id)
                })
                .map(MapiValue::Binary)
        }
        PID_TAG_WLINK_RECORD_KEY if message.shortcut_type != 4 => message
            .target_folder_id
            .map(mapi_mailstore::source_key_for_store_id)
            .map(MapiValue::Binary),
        PID_TAG_WLINK_STORE_ENTRY_ID if message.shortcut_type != 4 => Some(MapiValue::Binary(
            mapi_mailstore::private_store_entry_id(account_id),
        )),
        PID_TAG_WLINK_CALENDAR_COLOR if navigation_shortcut_targets_calendar(message) => {
            Some(MapiValue::I32(-1))
        }
        PID_TAG_WLINK_ADDRESS_BOOK_EID if navigation_shortcut_targets_calendar(message) => Some(
            MapiValue::Binary(navigation_shortcut_owner_entry_id(account_id)),
        ),
        PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID if message.shortcut_type != 4 => Some(
            MapiValue::Binary(mapi_mailstore::private_store_entry_id(account_id)),
        ),
        PID_TAG_WLINK_CLIENT_ID if navigation_shortcut_targets_calendar(message) => Some(
            MapiValue::Binary(wlink_save_stamp(message).to_le_bytes().to_vec()),
        ),
        PID_TAG_WLINK_RO_GROUP_TYPE if navigation_shortcut_targets_calendar(message) => {
            Some(MapiValue::I32(-1))
        }
        property_tag
            if property_tag_id(property_tag) == property_tag_id(PID_TAG_WLINK_FOLDER_TYPE) =>
        {
            Some(wlink_guid_property_value(
                requested_property_tag,
                wlink_folder_type_guid(message),
            ))
        }
        _ => None,
    }
}

fn navigation_shortcut_targets_calendar(message: &MapiNavigationShortcutMessage) -> bool {
    message.shortcut_type != 4
        && (message.section == 3 || message.target_folder_id == Some(CALENDAR_FOLDER_ID))
}

fn navigation_shortcut_owner_entry_id(account_id: Uuid) -> Vec<u8> {
    let entry = ExchangeAddressBookEntry {
        id: account_id,
        display_name: String::new(),
        email: String::new(),
        entry_kind: ExchangeAddressBookEntryKind::Account,
        directory_kind: ExchangeAddressBookDirectoryKind::Person,
        member_emails: Vec::new(),
        details: ExchangeAddressBookEntryDetails::default(),
    };
    super::nspi::nspi_entry_permanent_entry_id(&entry)
}

fn is_sharing_local_folder_id_property_tag(property_tag: u32) -> bool {
    matches!(
        property_tag,
        PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG
            | OUTLOOK_STALE_SHARING_LOCAL_FOLDER_ID_TAG
    )
}

pub(in crate::mapi) fn common_view_named_view_property_value(
    message: &MapiCommonViewNamedViewMessage,
    account_id: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    let requested_property_tag = property_tag;
    match canonical_property_storage_tag(property_tag) {
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
        PID_TAG_MID | PID_TAG_INST_ID => Some(MapiValue::U64(message.id)),
        PID_TAG_INSTANCE_NUM => Some(MapiValue::U32(0)),
        PID_TAG_ENTRY_ID => crate::mapi::identity::message_entry_id_from_object_ids(
            account_id,
            message.folder_id,
            message.id,
        )
        .map(MapiValue::Binary),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(message.id),
        )),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(message.name.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(
            "IPM.Microsoft.FolderDesign.NamedView".to_string(),
        )),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_FAI)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I32(128)),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(MapiValue::I64(128)),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_ASSOCIATED => Some(MapiValue::Bool(true)),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
        PID_TAG_SOURCE_KEY | PID_TAG_RECORD_KEY | PID_TAG_SEARCH_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(message.id),
        )),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(message.folder_id),
        )),
        PID_TAG_PARENT_ENTRY_ID => {
            crate::mapi::identity::folder_entry_id_from_object_id(account_id, message.folder_id)
                .map(MapiValue::Binary)
        }
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(
                mapi_mailstore::change_number_for_store_id(message.id),
            ),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => {
            Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
                mapi_mailstore::change_number_for_store_id(message.id),
            )))
        }
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(mapi_mailstore::change_number_for_store_id(
            message.id,
        ))),
        PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => {
            Some(MapiValue::U64(mapi_mailstore::filetime_from_change_number(
                mapi_mailstore::change_number_for_store_id(message.id),
            )))
        }
        PID_TAG_VIEW_DESCRIPTOR_FLAGS => Some(MapiValue::U32(message.view_flags)),
        PID_TAG_VIEW_DESCRIPTOR_BINARY | OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835 => {
            let definition = outlook_folder_view_definition(message.folder_id, &message.name);
            log_view_definition_diagnostics(
                message.folder_id,
                message.id,
                &message.name,
                &definition,
            );
            Some(MapiValue::Binary(view_descriptor_binary(&definition)))
        }
        OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C => {
            let definition = outlook_folder_view_definition(message.folder_id, &message.name);
            log_view_definition_diagnostics(
                message.folder_id,
                message.id,
                &message.name,
                &definition,
            );
            Some(MapiValue::Binary(view_descriptor_strings_binary(
                &definition,
            )))
        }
        PID_TAG_VIEW_DESCRIPTOR_VERSION | PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL => {
            Some(MapiValue::U32(message.view_type))
        }
        PID_TAG_VIEW_DESCRIPTOR_NAME_W => Some(MapiValue::String(message.name.clone())),
        PID_TAG_VIEW_DESCRIPTOR_STRINGS_W => {
            let definition = outlook_folder_view_definition(message.folder_id, &message.name);
            log_view_definition_diagnostics(
                message.folder_id,
                message.id,
                &message.name,
                &definition,
            );
            Some(MapiValue::String(view_descriptor_strings(&definition)))
        }
        PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE => Some(MapiValue::U32(0)),
        OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B => {
            let definition = outlook_folder_view_definition(message.folder_id, &message.name);
            log_view_definition_diagnostics(
                message.folder_id,
                message.id,
                &message.name,
                &definition,
            );
            Some(MapiValue::Binary(view_descriptor_binary(&definition)))
        }
        tag if property_tag_id(tag) == property_tag_id(PID_TAG_VIEW_DESCRIPTOR_CLSID) => Some(
            wlink_guid_property_value(requested_property_tag, *message.canonical_id.as_bytes()),
        ),
        tag if property_tag_id(tag) == property_tag_id(PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE) => {
            Some(wlink_guid_property_value(
                requested_property_tag,
                common_view_named_view_folder_type_guid(),
            ))
        }
        tag if property_tag_id(tag) == property_tag_id(PID_TAG_WLINK_GROUP_HEADER_ID) => Some(
            wlink_guid_property_value(requested_property_tag, default_wlink_group_guid()),
        ),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::mapi) enum ViewDefinitionKind {
    CalendarCompact,
    ContactList,
    JournalList,
    MailCompact,
    MailSentTo,
    NoteList,
    TaskList,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::mapi) struct ViewColumn {
    pub(in crate::mapi) property_tag: u32,
    pub(in crate::mapi) width: u32,
    pub(in crate::mapi) flags: u32,
    pub(in crate::mapi) kind: ViewColumnKind,
    pub(in crate::mapi) header: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::mapi) enum ViewColumnKind {
    Id,
    NamedId { guid: [u8; 16], id: u32 },
    NamedString { guid: [u8; 16], name: &'static str },
}

#[derive(Debug, Clone)]
pub(in crate::mapi) struct ViewDefinition {
    pub(in crate::mapi) kind: ViewDefinitionKind,
    pub(in crate::mapi) columns: Vec<ViewColumn>,
    pub(in crate::mapi) sort_column: usize,
    pub(in crate::mapi) sort_descending: bool,
}

pub(in crate::mapi) fn outlook_mail_view_definition(view_name: &str) -> ViewDefinition {
    if view_name.eq_ignore_ascii_case("Sent To") {
        return ViewDefinition {
            kind: ViewDefinitionKind::MailSentTo,
            columns: vec![
                view_column(PID_TAG_IMPORTANCE, 0x12, 0x0000_2F4A, "Importance"),
                view_named_id_column(
                    PID_LID_REMINDER_SET_TAG,
                    0x12,
                    0x0000_3F40,
                    PSETID_COMMON_GUID,
                    PID_LID_REMINDER_SET,
                    "Reminder",
                ),
                view_column(
                    string8_property_tag(PID_TAG_MESSAGE_CLASS_W),
                    0x12,
                    0x0000_270A,
                    "Icon",
                ),
                view_column(PID_TAG_FLAG_STATUS, 0x12, 0x0000_2F4A, "Flag Status"),
                view_column(PID_TAG_HAS_ATTACHMENTS, 0x12, 0x0000_2F4A, "Attachment"),
                view_column(
                    string8_property_tag(PID_TAG_DISPLAY_TO_W),
                    0x0C,
                    0x0000_2F00,
                    "To",
                ),
                view_column(
                    string8_property_tag(PID_TAG_SUBJECT_W),
                    0x11,
                    0x0000_2F00,
                    "Subject",
                ),
                view_column(PID_TAG_CLIENT_SUBMIT_TIME, 0x10, 0x0000_2F40, "Sent"),
                view_column(PID_TAG_MESSAGE_SIZE, 0x0C, 0x0000_2740, "Size"),
                view_named_string_column(
                    multiple_string8_property_tag(PID_NAME_KEYWORDS_TAG),
                    0x12,
                    0x0000_7B20,
                    PS_PUBLIC_STRINGS_GUID,
                    "Keywords",
                    "Categories",
                ),
            ],
            sort_column: 7,
            sort_descending: true,
        };
    }
    if view_name.eq_ignore_ascii_case("Messages") {
        return ViewDefinition {
            kind: ViewDefinitionKind::MailCompact,
            columns: vec![
                view_column(PID_TAG_IMPORTANCE, 0x12, 0x0000_2F4A, "Importance"),
                view_named_id_column(
                    PID_LID_OUTLOOK_COMMON_8514_TAG,
                    0x12,
                    0x0000_3F40,
                    PSETID_COMMON_GUID,
                    PID_LID_OUTLOOK_COMMON_8514,
                    "Reminder",
                ),
                view_column(
                    string8_property_tag(PID_TAG_MESSAGE_CLASS_W),
                    0x12,
                    0x0000_270A,
                    "Icon",
                ),
                view_column(PID_TAG_MESSAGE_STATUS, 0x12, 0x0000_2F4A, "Flag Status"),
                view_column(PID_TAG_HAS_ATTACHMENTS, 0x12, 0x0000_2F4A, "Attachment"),
                view_column(
                    string8_property_tag(PID_TAG_SENT_REPRESENTING_NAME_W),
                    0x0C,
                    0x0000_2F00,
                    "From",
                ),
                view_column(
                    string8_property_tag(PID_TAG_SUBJECT_W),
                    0x11,
                    0x0000_2F00,
                    "Subject",
                ),
                view_column(PID_TAG_MESSAGE_DELIVERY_TIME, 0x10, 0x0000_2F40, "Received"),
                view_column(
                    OUTLOOK_COMPACT_VIEW_AUXILIARY_FLAGS_TAG,
                    0x0C,
                    0x0000_2740,
                    "Size",
                ),
                view_named_string_column(
                    multiple_string8_property_tag(PID_NAME_KEYWORDS_TAG),
                    0x12,
                    0x0000_7B20,
                    PS_PUBLIC_STRINGS_GUID,
                    "Keywords",
                    "Categories",
                ),
            ],
            sort_column: 7,
            sort_descending: true,
        };
    }

    ViewDefinition {
        kind: ViewDefinitionKind::MailCompact,
        columns: vec![
            view_column(PID_TAG_IMPORTANCE, 0x12, 0x0000_2F4A, "Importance"),
            view_named_id_column(
                PID_LID_OUTLOOK_COMMON_8514_TAG,
                0x12,
                0x0000_3F40,
                PSETID_COMMON_GUID,
                PID_LID_OUTLOOK_COMMON_8514,
                "Reminder",
            ),
            view_column(
                string8_property_tag(PID_TAG_MESSAGE_CLASS_W),
                0x12,
                0x0000_270A,
                "Icon",
            ),
            view_column(PID_TAG_MESSAGE_STATUS, 0x12, 0x0000_2F4A, "Flag Status"),
            view_column(PID_TAG_HAS_ATTACHMENTS, 0x12, 0x0000_2F4A, "Attachment"),
            view_column(
                string8_property_tag(PID_TAG_SENT_REPRESENTING_NAME_W),
                0x0C,
                0x0000_2F00,
                "From",
            ),
            view_column(
                string8_property_tag(PID_TAG_SUBJECT_W),
                0x11,
                0x0000_2F00,
                "Subject",
            ),
            view_column(PID_TAG_MESSAGE_DELIVERY_TIME, 0x10, 0x0000_2F40, "Received"),
            view_column(
                OUTLOOK_COMPACT_VIEW_AUXILIARY_FLAGS_TAG,
                0x0C,
                0x0000_2740,
                "Size",
            ),
            view_named_string_column(
                multiple_string8_property_tag(PID_NAME_KEYWORDS_TAG),
                0x12,
                0x0000_7B20,
                PS_PUBLIC_STRINGS_GUID,
                "Keywords",
                "Categories",
            ),
        ],
        sort_column: 7,
        sort_descending: true,
    }
}

pub(in crate::mapi) fn outlook_folder_view_definition(
    folder_id: u64,
    view_name: &str,
) -> ViewDefinition {
    match folder_id {
        CALENDAR_FOLDER_ID => outlook_calendar_view_definition(view_name),
        CONTACTS_FOLDER_ID
        | SUGGESTED_CONTACTS_FOLDER_ID
        | QUICK_CONTACTS_FOLDER_ID
        | IM_CONTACT_LIST_FOLDER_ID
        | CONTACTS_SEARCH_FOLDER_ID => outlook_contact_view_definition(view_name),
        JOURNAL_FOLDER_ID => outlook_journal_view_definition(view_name),
        NOTES_FOLDER_ID => outlook_note_view_definition(view_name),
        TASKS_FOLDER_ID | TODO_SEARCH_FOLDER_ID => outlook_task_view_definition(view_name),
        _ => outlook_mail_view_definition(view_name),
    }
}

fn outlook_calendar_view_definition(_view_name: &str) -> ViewDefinition {
    ViewDefinition {
        kind: ViewDefinitionKind::CalendarCompact,
        columns: vec![
            view_column(
                string8_property_tag(PID_TAG_MESSAGE_CLASS_W),
                0x12,
                0x0000_270A,
                "Icon",
            ),
            view_column(
                string8_property_tag(PID_TAG_SUBJECT_W),
                0x18,
                0x0000_2F00,
                "Subject",
            ),
            view_named_id_column(
                PID_LID_COMMON_START_TAG,
                0x10,
                0x0000_3F40,
                PSETID_COMMON_GUID,
                PID_LID_COMMON_START,
                "Start",
            ),
            view_named_id_column(
                PID_LID_COMMON_END_TAG,
                0x10,
                0x0000_3F40,
                PSETID_COMMON_GUID,
                PID_LID_COMMON_END,
                "End",
            ),
            view_named_id_column(
                string8_property_tag(PID_LID_LOCATION_W_TAG),
                0x14,
                0x0000_3F00,
                PSETID_APPOINTMENT_GUID,
                PID_LID_LOCATION,
                "Location",
            ),
            view_named_id_column(
                PID_LID_BUSY_STATUS_TAG,
                0x0C,
                0x0000_3F40,
                PSETID_APPOINTMENT_GUID,
                PID_LID_BUSY_STATUS,
                "Busy",
            ),
        ],
        sort_column: 2,
        sort_descending: false,
    }
}

fn outlook_contact_view_definition(_view_name: &str) -> ViewDefinition {
    ViewDefinition {
        kind: ViewDefinitionKind::ContactList,
        columns: vec![
            view_column(
                string8_property_tag(PID_TAG_MESSAGE_CLASS_W),
                0x12,
                0x0000_270A,
                "Icon",
            ),
            view_column(
                string8_property_tag(PID_TAG_DISPLAY_NAME_W),
                0x18,
                0x0000_2F00,
                "Full Name",
            ),
            view_named_id_column(
                string8_property_tag(PID_LID_EMAIL1_EMAIL_ADDRESS_W_TAG),
                0x18,
                0x0000_3F00,
                PSETID_ADDRESS_GUID,
                PID_LID_EMAIL1_EMAIL_ADDRESS,
                "Email",
            ),
            view_column(
                string8_property_tag(PID_TAG_MOBILE_TELEPHONE_NUMBER_W),
                0x12,
                0x0000_2F00,
                "Mobile",
            ),
            view_column(
                string8_property_tag(PID_TAG_COMPANY_NAME_W),
                0x14,
                0x0000_2F00,
                "Company",
            ),
            view_column(
                string8_property_tag(PID_TAG_TITLE_W),
                0x14,
                0x0000_2F00,
                "Job Title",
            ),
        ],
        sort_column: 1,
        sort_descending: false,
    }
}

fn outlook_task_view_definition(_view_name: &str) -> ViewDefinition {
    ViewDefinition {
        kind: ViewDefinitionKind::TaskList,
        columns: vec![
            view_column(
                string8_property_tag(PID_TAG_MESSAGE_CLASS_W),
                0x12,
                0x0000_270A,
                "Icon",
            ),
            view_column(
                string8_property_tag(PID_TAG_SUBJECT_W),
                0x18,
                0x0000_2F00,
                "Subject",
            ),
            view_column(PID_TAG_FLAG_STATUS, 0x0C, 0x0000_2F4A, "Status"),
            view_named_id_column(
                PID_LID_TASK_DUE_DATE_TAG,
                0x10,
                0x0000_3F40,
                PSETID_TASK_GUID,
                PID_LID_TASK_DUE_DATE,
                "Due Date",
            ),
            view_named_id_column(
                PID_LID_TASK_START_DATE_TAG,
                0x10,
                0x0000_3F40,
                PSETID_TASK_GUID,
                PID_LID_TASK_START_DATE,
                "Start Date",
            ),
            view_named_id_column(
                PID_LID_PERCENT_COMPLETE_TAG,
                0x0C,
                0x0000_3F40,
                PSETID_TASK_GUID,
                PID_LID_PERCENT_COMPLETE,
                "% Complete",
            ),
        ],
        sort_column: 3,
        sort_descending: false,
    }
}

fn outlook_note_view_definition(_view_name: &str) -> ViewDefinition {
    ViewDefinition {
        kind: ViewDefinitionKind::NoteList,
        columns: vec![
            view_column(
                string8_property_tag(PID_TAG_MESSAGE_CLASS_W),
                0x12,
                0x0000_270A,
                "Icon",
            ),
            view_column(
                string8_property_tag(PID_TAG_SUBJECT_W),
                0x18,
                0x0000_2F00,
                "Subject",
            ),
            view_column(
                PID_TAG_LAST_MODIFICATION_TIME,
                0x10,
                0x0000_2F40,
                "Modified",
            ),
            view_named_id_column(
                PID_LID_NOTE_COLOR_TAG,
                0x0C,
                0x0000_3F40,
                PSETID_NOTE_GUID,
                PID_LID_NOTE_COLOR,
                "Color",
            ),
        ],
        sort_column: 2,
        sort_descending: true,
    }
}

fn outlook_journal_view_definition(_view_name: &str) -> ViewDefinition {
    ViewDefinition {
        kind: ViewDefinitionKind::JournalList,
        columns: vec![
            view_column(
                string8_property_tag(PID_TAG_MESSAGE_CLASS_W),
                0x12,
                0x0000_270A,
                "Icon",
            ),
            view_column(
                string8_property_tag(PID_TAG_SUBJECT_W),
                0x18,
                0x0000_2F00,
                "Subject",
            ),
            view_named_id_column(
                PID_LID_LOG_START_TAG,
                0x10,
                0x0000_3F40,
                PSETID_LOG_GUID,
                PID_LID_LOG_START,
                "Start",
            ),
            view_named_id_column(
                PID_LID_LOG_DURATION_TAG,
                0x0C,
                0x0000_3F40,
                PSETID_LOG_GUID,
                PID_LID_LOG_DURATION,
                "Duration",
            ),
            view_named_id_column(
                string8_property_tag(PID_LID_LOG_TYPE_W_TAG),
                0x12,
                0x0000_3F00,
                PSETID_LOG_GUID,
                PID_LID_LOG_TYPE,
                "Type",
            ),
        ],
        sort_column: 2,
        sort_descending: true,
    }
}

fn string8_property_tag(property_tag: u32) -> u32 {
    (property_tag & 0xFFFF_0000) | 0x001E
}

fn multiple_string8_property_tag(property_tag: u32) -> u32 {
    (property_tag & 0xFFFF_0000) | 0x101E
}

fn view_column(property_tag: u32, width: u32, flags: u32, header: &'static str) -> ViewColumn {
    ViewColumn {
        property_tag,
        width,
        flags,
        kind: ViewColumnKind::Id,
        header,
    }
}

fn view_named_id_column(
    property_tag: u32,
    width: u32,
    flags: u32,
    guid: [u8; 16],
    id: u32,
    header: &'static str,
) -> ViewColumn {
    ViewColumn {
        property_tag,
        width,
        flags,
        kind: ViewColumnKind::NamedId { guid, id },
        header,
    }
}

fn view_named_string_column(
    property_tag: u32,
    width: u32,
    flags: u32,
    guid: [u8; 16],
    name: &'static str,
    header: &'static str,
) -> ViewColumn {
    ViewColumn {
        property_tag,
        width,
        flags,
        kind: ViewColumnKind::NamedString { guid, name },
        header,
    }
}

pub(in crate::mapi) fn view_descriptor_binary(definition: &ViewDefinition) -> Vec<u8> {
    let column_count = definition.columns.len() + 1;
    let mut value = Vec::with_capacity(60 + column_count * 36);
    value.extend_from_slice(&[0; 8]);
    value.extend_from_slice(&8u32.to_le_bytes());
    value.extend_from_slice(
        &(if definition.sort_descending {
            0x0000_0002u32
        } else {
            0
        })
        .to_le_bytes(),
    );
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&(column_count as u32).to_le_bytes());
    value.extend_from_slice(&((definition.sort_column + 1) as u32).to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&[0; 24]);

    write_view_column_packet(&mut value, 0x0004_0001, 7, 0x0000_0028, ViewColumnKind::Id);
    for column in &definition.columns {
        write_view_column_packet(
            &mut value,
            column.property_tag,
            column.width,
            column.flags,
            column.kind,
        );
    }

    value
}

fn write_view_column_packet(
    value: &mut Vec<u8>,
    property_tag: u32,
    width: u32,
    flags: u32,
    kind: ViewColumnKind,
) {
    let property_id = match kind {
        ViewColumnKind::NamedString { .. } => 0,
        ViewColumnKind::NamedId { id, .. } => id,
        ViewColumnKind::Id => property_tag >> 16,
    };
    value.extend_from_slice(&(property_tag_type(property_tag) as u16).to_le_bytes());
    value.extend_from_slice(&(property_id as u16).to_le_bytes());
    value.extend_from_slice(&width.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&flags.to_le_bytes());
    match kind {
        ViewColumnKind::Id => value.extend_from_slice(&[0; 12]),
        ViewColumnKind::NamedId { .. } | ViewColumnKind::NamedString { .. } => {
            value.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0x34, 0x01, 0x9A, 0x11]);
        }
    }
    match kind {
        ViewColumnKind::Id => {
            value.extend_from_slice(&0u32.to_le_bytes());
            value.extend_from_slice(&property_id.to_le_bytes());
        }
        ViewColumnKind::NamedId { guid, id } => {
            value.extend_from_slice(&0u32.to_le_bytes());
            value.extend_from_slice(&id.to_le_bytes());
            value.extend_from_slice(&guid);
        }
        ViewColumnKind::NamedString { guid, name } => {
            value.extend_from_slice(&1u32.to_le_bytes());
            value.extend_from_slice(&0x0022_A764u32.to_le_bytes());
            value.extend_from_slice(&guid);
            let mut buffer = Vec::new();
            for unit in name.encode_utf16() {
                buffer.extend_from_slice(&unit.to_le_bytes());
            }
            buffer.extend_from_slice(&0u16.to_le_bytes());
            value.extend_from_slice(&(buffer.len() as u32).to_le_bytes());
            value.extend_from_slice(&buffer);
        }
    }
}

pub(in crate::mapi) fn view_descriptor_strings(definition: &ViewDefinition) -> String {
    let mut strings = String::new();
    strings.push('\n');
    for column in &definition.columns {
        strings.push_str(column.header);
        strings.push('\n');
    }
    strings
}

pub(in crate::mapi) fn view_descriptor_strings_binary(definition: &ViewDefinition) -> Vec<u8> {
    let strings = view_descriptor_strings(definition);
    let mut bytes = Vec::with_capacity(strings.encode_utf16().count() * 2);
    for unit in strings.encode_utf16() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    bytes
}

pub(in crate::mapi) fn log_view_definition_diagnostics(
    folder_id: u64,
    view_id: u64,
    view_name: &str,
    definition: &ViewDefinition,
) {
    let descriptor_len = view_descriptor_binary(definition).len();
    let descriptor_strings_len = view_descriptor_strings(definition).encode_utf16().count() * 2;
    tracing::debug!(
        folder_id = format_args!("0x{folder_id:016x}"),
        view_message_id = format_args!("0x{view_id:016x}"),
        view_name,
        canonical_version = 8u32,
        descriptor_binary_len = descriptor_len,
        descriptor_strings_len,
        column_count = definition.columns.len(),
        sort_count = 1usize,
        static_default = true,
        persisted = false,
        view_kind = ?definition.kind,
        "mapi named view descriptor"
    );
}

pub(in crate::mapi) fn view_descriptor_property_tags(descriptor: &[u8]) -> Vec<u32> {
    view_descriptor_all_property_tags(descriptor)
        .into_iter()
        .skip(1)
        .collect()
}

pub(in crate::mapi) fn view_descriptor_all_property_tags(descriptor: &[u8]) -> Vec<u32> {
    let Some(column_count) = descriptor
        .get(20..24)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .and_then(|count| usize::try_from(count).ok())
    else {
        return Vec::new();
    };

    let mut offset = 60usize;
    let mut tags = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let Some(packet) = descriptor.get(offset..offset + 36) else {
            break;
        };
        let property_type = u16::from_le_bytes([packet[0], packet[1]]) as u32;
        let property_id = u16::from_le_bytes([packet[2], packet[3]]) as u32;
        let flags = u32::from_le_bytes([packet[12], packet[13], packet[14], packet[15]]);
        let kind = u32::from_le_bytes([packet[28], packet[29], packet[30], packet[31]]);
        tags.push((property_id << 16) | property_type);
        offset += 36;

        if flags & 0x0000_1000 == 0 {
            continue;
        }
        offset = offset.saturating_add(16);
        if kind == 1 {
            let Some(length_bytes) = descriptor.get(offset..offset + 4) else {
                break;
            };
            let buffer_length = u32::from_le_bytes(
                length_bytes
                    .try_into()
                    .expect("slice length checked for view descriptor buffer length"),
            ) as usize;
            offset = offset.saturating_add(4).saturating_add(buffer_length);
        }
    }

    tags
}

fn property_tag_id(property_tag: u32) -> u32 {
    property_tag & 0xFFFF_0000
}

fn named_property_id_matches(left: u32, right: u32) -> bool {
    property_tag_id(left) == property_tag_id(right)
}

fn wlink_guid_property_value(property_tag: u32, guid: [u8; 16]) -> MapiValue {
    match MapiPropertyTag::new(property_tag).property_type() {
        Some(MapiPropertyType::Binary) => MapiValue::Binary(guid.to_vec()),
        _ => MapiValue::Guid(guid),
    }
}

pub(in crate::mapi) fn default_wlink_group_guid() -> [u8; 16] {
    [
        0x5B, 0xA9, 0x43, 0xD8, 0xDA, 0xAA, 0x46, 0x2C, 0xA6, 0x3E, 0x91, 0x36, 0xF6, 0x5C, 0x86,
        0x81,
    ]
}

pub(crate) fn default_wlink_group_uuid() -> Uuid {
    Uuid::from_bytes(default_wlink_group_guid())
}

fn wlink_group_name(message: &MapiNavigationShortcutMessage) -> String {
    if message.group_name.trim().is_empty() {
        if message.section == 1 {
            "Favorites".to_string()
        } else {
            "Mail".to_string()
        }
    } else {
        message.group_name.clone()
    }
}

fn wlink_save_stamp(message: &MapiNavigationShortcutMessage) -> u32 {
    if message.save_stamp != 0 {
        return message.save_stamp;
    }
    let bytes = message
        .group_header_id
        .as_ref()
        .map(Uuid::as_bytes)
        .unwrap_or_else(|| message.canonical_id.as_bytes());
    let stamp = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    if stamp == 0 {
        1
    } else {
        stamp
    }
}

fn wlink_mail_folder_type_guid() -> [u8; 16] {
    [
        0x0C, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ]
}

pub(in crate::mapi) fn common_view_named_view_folder_type_guid() -> [u8; 16] {
    [
        0x00, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ]
}

pub(in crate::mapi) fn wlink_folder_type_guid(message: &MapiNavigationShortcutMessage) -> [u8; 16] {
    if message.target_folder_id.is_some_and(|folder_id| {
        matches!(
            folder_id,
            INBOX_FOLDER_ID
                | OUTBOX_FOLDER_ID
                | SENT_FOLDER_ID
                | DRAFTS_FOLDER_ID
                | TRASH_FOLDER_ID
                | JUNK_FOLDER_ID
                | ARCHIVE_FOLDER_ID
        )
    }) {
        return wlink_mail_folder_type_guid();
    }
    [
        0x02, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ]
}

pub(in crate::mapi) fn wlink_ordinal_bytes(value: u32) -> Vec<u8> {
    let mut bytes = if value <= u8::MAX as u32 {
        vec![value as u8]
    } else {
        value
            .to_be_bytes()
            .into_iter()
            .skip_while(|byte| *byte == 0)
            .collect()
    };
    match bytes.last_mut() {
        Some(last) if *last == 0 => *last = 1,
        Some(last) if *last == u8::MAX => *last = u8::MAX - 1,
        None => bytes.push(1),
        _ => {}
    }
    bytes
}

pub(in crate::mapi) fn conversation_action_property_value(
    message: &MapiConversationActionMessage,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    let action = &message.action;
    match property_tag {
        PID_TAG_MID => Some(MapiValue::U64(message.id)),
        PID_TAG_ENTRY_ID => crate::mapi::identity::message_entry_id_from_object_ids(
            Uuid::nil(),
            message.folder_id,
            message.id,
        )
        .map(MapiValue::Binary),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(message.id),
        )),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_CONVERSATION_TOPIC_W => {
            Some(MapiValue::String(conversation_action_subject(action)))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.ConversationAction".to_string())),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I32(
            conversation_action_size(action).min(i32::MAX as usize) as i32,
        )),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(
            conversation_action_size(action).min(i64::MAX as usize) as i64,
        )),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_ASSOCIATED => Some(MapiValue::Bool(true)),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
        PID_TAG_CONVERSATION_INDEX => Some(MapiValue::Binary(conversation_index_for_uuid(
            action.conversation_id,
        ))),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            message.id,
        ))),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(
                mapi_mailstore::change_number_for_store_id(message.id),
            ),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => {
            Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
                mapi_mailstore::change_number_for_store_id(message.id),
            )))
        }
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(message.folder_id),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(message.id & 0x00FF_FFFF_FFFF_FFFF)),
        PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG => {
            action.move_folder_entry_id.clone().map(MapiValue::Binary)
        }
        PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG => {
            action.move_store_entry_id.clone().map(MapiValue::Binary)
        }
        PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG => action
            .max_delivery_time
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG => action
            .last_applied_time
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_LID_CONVERSATION_ACTION_VERSION_TAG => Some(MapiValue::I32(action.version)),
        PID_LID_CONVERSATION_PROCESSED_TAG => Some(MapiValue::I32(action.processed)),
        PID_NAME_KEYWORDS_TAG => Some(MapiValue::MultiString(json_string_array(
            &action.categories_json,
        ))),
        _ => None,
    }
}

pub(in crate::mapi) fn conversation_index_for_uuid(conversation_id: Uuid) -> Vec<u8> {
    let mut value = Vec::with_capacity(22);
    value.extend_from_slice(&[0x01, 0, 0, 0, 0, 0]);
    value.extend_from_slice(conversation_id.as_bytes());
    value
}

pub(in crate::mapi) fn message_class_for_email(email: &JmapEmail) -> &'static str {
    if email.mailbox_role == "rss_feeds" {
        "IPM.Post.RSS"
    } else {
        "IPM.Note"
    }
}

pub(in crate::mapi) fn native_body_format(email: &JmapEmail) -> u32 {
    if email
        .body_html_sanitized
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty())
    {
        3
    } else if email.body_text.trim().is_empty() {
        0
    } else {
        1
    }
}

fn html_body_from_plain_text(body_text: &str) -> Option<String> {
    if body_text.trim().is_empty() {
        return None;
    }
    let mut html = String::from("<html><body>");
    for ch in body_text.chars() {
        match ch {
            '&' => html.push_str("&amp;"),
            '<' => html.push_str("&lt;"),
            '>' => html.push_str("&gt;"),
            '"' => html.push_str("&quot;"),
            '\'' => html.push_str("&#39;"),
            '\r' => {}
            '\n' => html.push_str("<br>"),
            _ => html.push(ch),
        }
    }
    html.push_str("</body></html>");
    Some(html)
}

pub(in crate::mapi) fn uncompressed_rtf_body(body_text: &str) -> Vec<u8> {
    let mut rtf = String::from("{\\rtf1\\ansi\\deff0{\\fonttbl{\\f0\\fnil Segoe UI;}}\\f0\\fs20 ");
    append_rtf_escaped_text(&mut rtf, body_text);
    rtf.push('}');
    rtf_uncompressed_container(rtf.as_bytes())
}

fn append_rtf_escaped_text(output: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '\\' => output.push_str("\\\\"),
            '{' => output.push_str("\\{"),
            '}' => output.push_str("\\}"),
            '\r' => {}
            '\n' => output.push_str("\\par "),
            '\t' => output.push_str("\\tab "),
            ' '..='~' => output.push(ch),
            _ => {
                let mut units = [0; 2];
                for unit in ch.encode_utf16(&mut units) {
                    let signed = *unit as i16;
                    output.push_str(&format!("\\u{signed}?"));
                }
            }
        }
    }
}

fn rtf_uncompressed_container(raw: &[u8]) -> Vec<u8> {
    let raw_size = u32::try_from(raw.len()).expect("RTF body too large for MAPI");
    let compressed_size = raw_size
        .checked_add(12)
        .expect("RTF body too large for MAPI");
    let mut value = Vec::with_capacity(raw.len() + 16);
    value.extend_from_slice(&compressed_size.to_le_bytes());
    value.extend_from_slice(&raw_size.to_le_bytes());
    value.extend_from_slice(&0x414C_454D_u32.to_le_bytes());
    value.extend_from_slice(&0_u32.to_le_bytes());
    value.extend_from_slice(raw);
    value
}

fn transport_headers(email: &JmapEmail) -> String {
    let mut headers = Vec::new();
    if let Some(message_id) = email.internet_message_id.as_deref() {
        headers.push(format!("Message-ID: {message_id}"));
    }
    headers.push(format!(
        "From: {}",
        email.from_display.as_deref().unwrap_or(&email.from_address)
    ));
    let to = display_to(email);
    if !to.is_empty() {
        headers.push(format!("To: {to}"));
    }
    let cc = display_cc(email);
    if !cc.is_empty() {
        headers.push(format!("Cc: {cc}"));
    }
    headers.push(format!("Subject: {}", email.subject));
    headers.join("\r\n")
}

pub(in crate::mapi) fn conversation_id_from_index(value: &[u8]) -> Option<Uuid> {
    let bytes: [u8; 16] = value.get(6..22)?.try_into().ok()?;
    Some(Uuid::from_bytes(bytes))
}

pub(in crate::mapi) fn conversation_action_subject(
    action: &lpe_storage::ConversationAction,
) -> String {
    let subject = action.subject.trim();
    if subject.is_empty() {
        "Conv.Action".to_string()
    } else if subject.starts_with("Conv.Action") {
        subject.to_string()
    } else {
        format!("Conv.Action: {subject}")
    }
}

fn conversation_action_size(action: &lpe_storage::ConversationAction) -> usize {
    conversation_action_subject(action)
        .len()
        .saturating_add(action.categories_json.len())
        .saturating_add(
            action
                .move_folder_entry_id
                .as_ref()
                .map(Vec::len)
                .unwrap_or_default(),
        )
        .saturating_add(
            action
                .move_store_entry_id
                .as_ref()
                .map(Vec::len)
                .unwrap_or_default(),
        )
}

fn rss_email_named_property_value(email: &JmapEmail, property_tag: u32) -> Option<MapiValue> {
    if email.mailbox_role != "rss_feeds" {
        return None;
    }
    match property_tag {
        PID_LID_POST_RSS_CHANNEL_LINK_W_TAG => Some(MapiValue::String(String::new())),
        PID_LID_POST_RSS_ITEM_LINK_W_TAG => Some(MapiValue::String(String::new())),
        PID_LID_POST_RSS_ITEM_HASH_TAG => Some(MapiValue::I32(
            (mapi_mailstore::canonical_message_change_number(email) & 0x7FFF_FFFF) as i32,
        )),
        PID_LID_POST_RSS_ITEM_GUID_W_TAG => Some(MapiValue::String(
            email
                .internet_message_id
                .clone()
                .unwrap_or_else(|| email.id.to_string()),
        )),
        PID_LID_POST_RSS_CHANNEL_W_TAG => Some(MapiValue::String(email.mailbox_name.clone())),
        PID_LID_POST_RSS_ITEM_XML_W_TAG => Some(MapiValue::String(email.body_text.clone())),
        PID_LID_POST_RSS_SUBSCRIPTION_W_TAG => Some(MapiValue::String(email.mailbox_name.clone())),
        _ => None,
    }
}

pub(in crate::mapi) fn contact_property_value(
    contact: &AccessibleContact,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    let change_number = mapi_mailstore::change_number_for_store_id(item_id);
    match property_tag {
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(folder_id)),
        PID_TAG_MID => Some(MapiValue::U64(item_id)),
        PID_TAG_INST_ID => Some(MapiValue::U64(item_id)),
        PID_TAG_INSTANCE_NUM => Some(MapiValue::U32(0)),
        PID_TAG_DISPLAY_NAME_W | PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
            Some(MapiValue::String(contact.name.clone()))
        }
        PID_TAG_DISPLAY_NAME_PREFIX_W => {
            Some(MapiValue::String(contact.structured_name.prefix.clone()))
        }
        PID_TAG_GIVEN_NAME_W => Some(MapiValue::String(contact_given_name(contact))),
        PID_TAG_MIDDLE_NAME_W => Some(MapiValue::String(contact.structured_name.middle.clone())),
        PID_TAG_SURNAME_W => Some(MapiValue::String(contact_family_name(contact))),
        PID_TAG_GENERATION_W => Some(MapiValue::String(contact.structured_name.suffix.clone())),
        PID_TAG_NICKNAME_W => Some(MapiValue::String(contact.structured_name.nickname.clone())),
        PID_TAG_EMAIL_ADDRESS_W | PID_TAG_SMTP_ADDRESS_W => {
            Some(MapiValue::String(contact.email.clone()))
        }
        PID_LID_EMAIL1_ADDRESS_TYPE_W_TAG => {
            contact_email_value(contact, 0).map(|_| MapiValue::String("SMTP".to_string()))
        }
        PID_LID_EMAIL1_DISPLAY_NAME_W_TAG
        | PID_LID_EMAIL1_EMAIL_ADDRESS_W_TAG
        | PID_LID_EMAIL1_ORIGINAL_DISPLAY_NAME_W_TAG
        | PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_DISPLAY_NAME_W_TAG
        | PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_EMAIL_ADDRESS_W_TAG => {
            contact_email_value(contact, 0).map(MapiValue::String)
        }
        PID_LID_EMAIL2_ADDRESS_TYPE_W_TAG => Some(MapiValue::String(
            contact_email_value(contact, 1)
                .map(|_| "SMTP".to_string())
                .unwrap_or_default(),
        )),
        PID_LID_EMAIL2_DISPLAY_NAME_W_TAG
        | PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG
        | PID_LID_EMAIL2_ORIGINAL_DISPLAY_NAME_W_TAG
        | PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_DISPLAY_NAME_W_TAG
        | PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_EMAIL_ADDRESS_W_TAG => Some(MapiValue::String(
            contact_email_value(contact, 1).unwrap_or_default(),
        )),
        PID_LID_EMAIL3_ADDRESS_TYPE_W_TAG => Some(MapiValue::String(
            contact_email_value(contact, 2)
                .map(|_| "SMTP".to_string())
                .unwrap_or_default(),
        )),
        PID_LID_EMAIL3_DISPLAY_NAME_W_TAG
        | PID_LID_EMAIL3_EMAIL_ADDRESS_W_TAG
        | PID_LID_EMAIL3_ORIGINAL_DISPLAY_NAME_W_TAG
        | PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS3_EMAIL_ADDRESS_W_TAG => Some(MapiValue::String(
            contact_email_value(contact, 2).unwrap_or_default(),
        )),
        PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_ADDRESS_TYPE_W_TAG => {
            contact_email_value(contact, 0).map(|_| MapiValue::String("SMTP".to_string()))
        }
        PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_ADDRESS_TYPE_W_TAG => Some(MapiValue::String(
            contact_email_value(contact, 1)
                .map(|_| "SMTP".to_string())
                .unwrap_or_default(),
        )),
        PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS3_ADDRESS_TYPE_W_TAG => Some(MapiValue::String(
            contact_email_value(contact, 2)
                .map(|_| "SMTP".to_string())
                .unwrap_or_default(),
        )),
        PID_LID_OUTLOOK_CONTACT_SOURCE_80E0_TAG
        | PID_LID_OUTLOOK_CONTACT_SOURCE_80E2_TAG
        | PID_LID_OUTLOOK_CONTACT_SOURCE_80E3_TAG
        | PID_LID_OUTLOOK_CONTACT_SOURCE_80E5_TAG
        | PID_LID_OUTLOOK_CONTACT_SOURCE_80E6_TAG
        | PID_LID_OUTLOOK_CONTACT_SOURCE_80E8_TAG => {
            outlook_contact_source_empty_value(property_tag)
        }
        tag if MapiPropertyTag::new(tag).property_id()
            == MapiPropertyTag::new(PID_NAME_OSC_CONTACT_SOURCES_TAG).property_id() =>
        {
            outlook_contact_source_empty_value(tag)
        }
        PID_TAG_MOBILE_TELEPHONE_NUMBER_W => Some(MapiValue::String(contact_phone_by_label(
            contact,
            &["mobile", "cell"],
        ))),
        PID_TAG_BUSINESS_TELEPHONE_NUMBER_W => Some(MapiValue::String(contact_phone_by_label(
            contact,
            &["work", "business"],
        ))),
        PID_TAG_HOME_TELEPHONE_NUMBER_W => Some(MapiValue::String(contact_phone_by_label(
            contact,
            &["home"],
        ))),
        PID_TAG_PRIMARY_TELEPHONE_NUMBER_W => Some(MapiValue::String(contact.phone.clone())),
        PID_TAG_BUSINESS2_TELEPHONE_NUMBERS_W => Some(MapiValue::MultiString(
            contact_phone_values_by_label(contact, &["work2", "business2"]),
        )),
        PID_TAG_COMPANY_NAME_W => Some(MapiValue::String(contact_organization_name(contact))),
        PID_TAG_DEPARTMENT_NAME_W => Some(MapiValue::String(contact.team.clone())),
        PID_TAG_TITLE_W => Some(MapiValue::String(contact_job_title(contact))),
        PID_TAG_PERSONAL_HOME_PAGE_W => Some(MapiValue::String(contact_url_by_label(
            contact,
            &["home", "personal"],
        ))),
        PID_TAG_BUSINESS_HOME_PAGE_W => Some(MapiValue::String(contact_url_by_label(
            contact,
            &["work", "business"],
        ))),
        PID_TAG_BODY_W => Some(MapiValue::String(contact.notes.clone())),
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Contact".to_string())),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(contact_size(contact))),
        PID_TAG_MESSAGE_SIZE_EXTENDED => {
            Some(mapi_message_size_extended_value(contact_size(contact)))
        }
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(item_id),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &contact.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ => None,
    }
}

fn outlook_contact_source_empty_value(property_tag: u32) -> Option<MapiValue> {
    match MapiPropertyTag::new(property_tag).property_type_code() {
        0x0003 => Some(MapiValue::U32(0)),
        0x000B => Some(MapiValue::Bool(false)),
        0x001E | 0x001F => Some(MapiValue::String(String::new())),
        0x0048 => Some(MapiValue::Guid(Uuid::nil().into_bytes())),
        0x0102 => Some(MapiValue::Binary(Vec::new())),
        0x1003 => Some(MapiValue::MultiI32(Vec::new())),
        0x101E | 0x101F => Some(MapiValue::MultiString(Vec::new())),
        0x1102 => Some(MapiValue::MultiBinary(Vec::new())),
        _ => None,
    }
}

pub(in crate::mapi) fn contact_given_name(contact: &AccessibleContact) -> String {
    if !contact.structured_name.given.trim().is_empty() {
        return contact.structured_name.given.clone();
    }
    contact
        .name
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string()
}

pub(in crate::mapi) fn contact_family_name(contact: &AccessibleContact) -> String {
    if !contact.structured_name.family.trim().is_empty() {
        return contact.structured_name.family.clone();
    }
    contact
        .name
        .split_whitespace()
        .last()
        .filter(|value| *value != contact.name)
        .unwrap_or_default()
        .to_string()
}

pub(in crate::mapi) fn contact_organization_name(contact: &AccessibleContact) -> String {
    if contact.organization_name.trim().is_empty() {
        contact.team.clone()
    } else {
        contact.organization_name.clone()
    }
}

pub(in crate::mapi) fn contact_job_title(contact: &AccessibleContact) -> String {
    if contact.job_title.trim().is_empty() {
        contact.role.clone()
    } else {
        contact.job_title.clone()
    }
}

fn contact_phone_by_label(contact: &AccessibleContact, labels: &[&str]) -> String {
    contact_phone_values_by_label(contact, labels)
        .into_iter()
        .next()
        .unwrap_or_else(|| contact.phone.clone())
}

fn contact_phone_values_by_label(contact: &AccessibleContact, labels: &[&str]) -> Vec<String> {
    contact_labeled_json_values(&contact.phones_json, "phone", labels)
}

fn contact_email_value(contact: &AccessibleContact, index: usize) -> Option<String> {
    let mut values = Vec::new();
    let primary = contact.email.trim();
    if !primary.is_empty() {
        values.push(primary.to_string());
    }
    for value in contact_json_values(&contact.emails_json, "email") {
        if !values
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(&value))
        {
            values.push(value);
        }
    }
    values.into_iter().nth(index)
}

fn contact_url_by_label(contact: &AccessibleContact, labels: &[&str]) -> String {
    contact_labeled_json_values(&contact.urls_json, "url", labels)
        .into_iter()
        .next()
        .or_else(|| {
            contact_labeled_json_values(&contact.urls_json, "href", labels)
                .into_iter()
                .next()
        })
        .unwrap_or_default()
}

fn contact_labeled_json_values(
    value: &serde_json::Value,
    key: &str,
    labels: &[&str],
) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| {
            let label = item
                .get("label")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            labels
                .iter()
                .any(|expected| label.eq_ignore_ascii_case(expected))
        })
        .filter_map(|item| item.get(key).and_then(serde_json::Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn contact_json_values(value: &serde_json::Value, key: &str) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| item.get(key).and_then(serde_json::Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

pub(in crate::mapi) fn event_property_value(
    event: &AccessibleEvent,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
) -> Option<MapiValue> {
    event_property_value_with_reminder(event, item_id, folder_id, property_tag, None)
}

pub(in crate::mapi) fn event_property_value_with_reminder(
    event: &AccessibleEvent,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
    reminder: Option<&lpe_storage::ClientReminder>,
) -> Option<MapiValue> {
    if let Some(value) = event_reminder_property_value(event, reminder, property_tag) {
        return Some(value);
    }
    let property_tag = canonical_property_storage_tag(property_tag);
    let change_number = mapi_mailstore::change_number_for_store_id(item_id);
    match property_tag {
        PID_TAG_FOLDER_ID | PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(folder_id)),
        PID_TAG_MID | PID_TAG_INST_ID => Some(MapiValue::U64(item_id)),
        PID_TAG_INSTANCE_NUM => Some(MapiValue::U32(0)),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(event.title.clone()))
        }
        PID_TAG_BODY_W => Some(MapiValue::String(event.notes.clone())),
        PID_TAG_START_DATE
        | PID_LID_COMMON_START_TAG
        | PID_LID_APPOINTMENT_START_WHOLE_TAG
        | PID_TAG_MESSAGE_DELIVERY_TIME
        | PID_TAG_LAST_MODIFICATION_TIME => {
            Some(MapiValue::I64(event_start_filetime(event) as i64))
        }
        PID_TAG_END_DATE | PID_LID_COMMON_END_TAG | PID_LID_APPOINTMENT_END_WHOLE_TAG => {
            Some(MapiValue::I64(event_end_filetime(event) as i64))
        }
        PID_TAG_LOCATION_W | PID_LID_LOCATION_W_TAG => {
            Some(MapiValue::String(event.location.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Appointment".to_string())),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_MESSAGE_STATUS => Some(MapiValue::U32(0)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(event_size(event))),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(event_size(event))),
        PID_TAG_SENDER_NAME_W => Some(MapiValue::String(calendar_organizer_name(event))),
        PID_TAG_SENDER_EMAIL_ADDRESS_W => Some(MapiValue::String(calendar_organizer_email(event))),
        PID_TAG_DISPLAY_TO_W => Some(MapiValue::String(calendar_display_to(event))),
        PID_TAG_DISPLAY_CC_W => Some(MapiValue::String(calendar_optional_attendees(event))),
        PID_TAG_BODY_HTML_W => Some(MapiValue::String(event.body_html.clone())),
        PID_TAG_HTML_BINARY => Some(MapiValue::Binary(event.body_html.clone().into_bytes())),
        PID_LID_ALL_ATTENDEES_STRING_W_TAG => {
            Some(MapiValue::String(calendar_all_attendees(event)))
        }
        PID_LID_TO_ATTENDEES_STRING_W_TAG => {
            Some(MapiValue::String(calendar_required_attendees(event)))
        }
        PID_LID_CC_ATTENDEES_STRING_W_TAG => {
            Some(MapiValue::String(calendar_optional_attendees(event)))
        }
        PID_LID_BUSY_STATUS_TAG => Some(MapiValue::I32(appointment_busy_status(event))),
        PID_LID_APPOINTMENT_DURATION_TAG => Some(MapiValue::I32(appointment_duration(event))),
        PID_LID_APPOINTMENT_COLOR_TAG => Some(MapiValue::I32(0)),
        PID_LID_SIDE_EFFECTS_TAG => Some(MapiValue::I32(CALENDAR_EVENT_SIDE_EFFECTS)),
        PID_LID_OUTLOOK_COMMON_8578_TAG => Some(MapiValue::I32(0)),
        PID_LID_APPOINTMENT_SUB_TYPE_TAG => Some(MapiValue::Bool(event.all_day)),
        PID_LID_APPOINTMENT_STATE_FLAGS_TAG => Some(MapiValue::I32(appointment_state_flags(event))),
        PID_LID_RECURRING_TAG => Some(MapiValue::Bool(!event.recurrence_rule.trim().is_empty())),
        PID_LID_TIME_ZONE_STRUCT_TAG => Some(MapiValue::Binary(calendar_time_zone_struct(event))),
        PID_LID_TIME_ZONE_DESCRIPTION_W_TAG => Some(MapiValue::String(
            calendar_time_zone_key(&event.time_zone).to_string(),
        )),
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG
        | PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG => {
            Some(MapiValue::Binary(calendar_time_zone_definition(event)))
        }
        PID_LID_APPOINTMENT_RECUR_TAG => calendar_recurrence_blob(event).map(MapiValue::Binary),
        PID_LID_GLOBAL_OBJECT_ID_TAG | PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG => {
            Some(MapiValue::Binary(calendar_global_object_id(event)))
        }
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(item_id),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &event.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ => None,
    }
}

fn calendar_organizer(event: &AccessibleEvent) -> CalendarOrganizerMetadata {
    parse_calendar_participants_metadata(&event.attendees_json)
        .organizer
        .or_else(|| serde_json::from_str::<CalendarOrganizerMetadata>(&event.organizer_json).ok())
        .unwrap_or_else(|| CalendarOrganizerMetadata {
            email: event.owner_email.clone(),
            common_name: event.owner_display_name.clone(),
        })
}

fn calendar_organizer_name(event: &AccessibleEvent) -> String {
    let organizer = calendar_organizer(event);
    if organizer.common_name.trim().is_empty() {
        organizer.email
    } else {
        organizer.common_name
    }
}

fn calendar_organizer_email(event: &AccessibleEvent) -> String {
    calendar_organizer(event).email
}

fn calendar_display_to(event: &AccessibleEvent) -> String {
    let participants = parse_calendar_participants_metadata(&event.attendees_json);
    let labels = calendar_attendee_labels(&participants);
    if labels.trim().is_empty() {
        event.attendees.clone()
    } else {
        labels
    }
}

fn calendar_all_attendees(event: &AccessibleEvent) -> String {
    let participants = parse_calendar_participants_metadata(&event.attendees_json);
    calendar_participant_labels(participants.attendees.iter())
}

fn calendar_required_attendees(event: &AccessibleEvent) -> String {
    let participants = parse_calendar_participants_metadata(&event.attendees_json);
    calendar_participant_labels(
        participants
            .attendees
            .iter()
            .filter(|attendee| !attendee.role.eq_ignore_ascii_case("OPT-PARTICIPANT")),
    )
}

fn calendar_optional_attendees(event: &AccessibleEvent) -> String {
    let participants = parse_calendar_participants_metadata(&event.attendees_json);
    calendar_participant_labels(
        participants
            .attendees
            .iter()
            .filter(|attendee| attendee.role.eq_ignore_ascii_case("OPT-PARTICIPANT")),
    )
}

fn calendar_participant_labels<'a>(
    participants: impl Iterator<Item = &'a CalendarParticipantMetadata>,
) -> String {
    participants
        .map(|attendee| {
            if attendee.common_name.trim().is_empty() {
                attendee.email.trim()
            } else {
                attendee.common_name.trim()
            }
        })
        .filter(|label| !label.is_empty())
        .collect::<Vec<_>>()
        .join("; ")
}

fn appointment_busy_status(event: &AccessibleEvent) -> i32 {
    if event.status.eq_ignore_ascii_case("cancelled") {
        0
    } else if event.status.eq_ignore_ascii_case("tentative") {
        1
    } else {
        2
    }
}

fn appointment_state_flags(event: &AccessibleEvent) -> i32 {
    let participants = parse_calendar_participants_metadata(&event.attendees_json);
    let mut flags = 0;
    if participants.organizer.is_some() || !participants.attendees.is_empty() {
        flags |= 0x0000_0001;
    }
    if event.status.eq_ignore_ascii_case("cancelled") {
        flags |= 0x0000_0004;
    }
    flags
}

fn appointment_duration(event: &AccessibleEvent) -> i32 {
    let start = event_start_filetime(event);
    let end = event_end_filetime(event);
    if end <= start {
        return 0;
    }
    ((end - start) / 600_000_000).min(i32::MAX as u64) as i32
}

fn calendar_time_zone_key(time_zone: &str) -> &'static str {
    if time_zone.eq_ignore_ascii_case("W. Europe Standard Time")
        || time_zone.eq_ignore_ascii_case("Europe/Zurich")
        || time_zone.eq_ignore_ascii_case("Europe/Berlin")
        || time_zone.eq_ignore_ascii_case("Europe/Rome")
        || time_zone.eq_ignore_ascii_case("Europe/Vienna")
    {
        "W. Europe Standard Time"
    } else {
        "UTC"
    }
}

fn calendar_time_zone_struct(event: &AccessibleEvent) -> Vec<u8> {
    let tz = calendar_time_zone(event);
    let mut value = Vec::with_capacity(48);
    value.extend_from_slice(&tz.bias.to_le_bytes());
    value.extend_from_slice(&tz.standard_bias.to_le_bytes());
    value.extend_from_slice(&tz.daylight_bias.to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
    push_system_time(&mut value, tz.standard_date);
    value.extend_from_slice(&0u16.to_le_bytes());
    push_system_time(&mut value, tz.daylight_date);
    value
}

fn calendar_time_zone_definition(event: &AccessibleEvent) -> Vec<u8> {
    let tz = calendar_time_zone(event);
    let key_name = calendar_time_zone_key(&event.time_zone);
    let key_name_units = key_name.encode_utf16().collect::<Vec<_>>();
    let cb_header = 2usize
        .saturating_add(2)
        .saturating_add(key_name_units.len().saturating_mul(2))
        .saturating_add(2)
        .min(u16::MAX as usize) as u16;
    let mut value = Vec::with_capacity(8 + key_name_units.len() * 2 + 66);
    value.push(0x02);
    value.push(0x01);
    value.extend_from_slice(&cb_header.to_le_bytes());
    value.extend_from_slice(&0x0002u16.to_le_bytes());
    value.extend_from_slice(&(key_name_units.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for unit in key_name_units {
        value.extend_from_slice(&unit.to_le_bytes());
    }
    value.extend_from_slice(&1u16.to_le_bytes());
    push_time_zone_rule(&mut value, tz);
    value
}

#[derive(Clone, Copy)]
struct CalendarTimeZone {
    bias: i32,
    standard_bias: i32,
    daylight_bias: i32,
    standard_date: CalendarSystemTime,
    daylight_date: CalendarSystemTime,
}

#[derive(Clone, Copy)]
struct CalendarSystemTime {
    year: u16,
    month: u16,
    day_of_week: u16,
    day: u16,
    hour: u16,
    minute: u16,
}

fn calendar_time_zone(event: &AccessibleEvent) -> CalendarTimeZone {
    if calendar_time_zone_key(&event.time_zone) == "W. Europe Standard Time" {
        CalendarTimeZone {
            bias: -60,
            standard_bias: 0,
            daylight_bias: -60,
            standard_date: CalendarSystemTime {
                year: 0,
                month: 10,
                day_of_week: 0,
                day: 5,
                hour: 3,
                minute: 0,
            },
            daylight_date: CalendarSystemTime {
                year: 0,
                month: 3,
                day_of_week: 0,
                day: 5,
                hour: 2,
                minute: 0,
            },
        }
    } else {
        CalendarTimeZone {
            bias: 0,
            standard_bias: 0,
            daylight_bias: 0,
            standard_date: CalendarSystemTime::zero(),
            daylight_date: CalendarSystemTime::zero(),
        }
    }
}

impl CalendarSystemTime {
    fn zero() -> Self {
        Self {
            year: 0,
            month: 0,
            day_of_week: 0,
            day: 0,
            hour: 0,
            minute: 0,
        }
    }
}

fn push_time_zone_rule(value: &mut Vec<u8>, tz: CalendarTimeZone) {
    value.push(0x02);
    value.push(0x01);
    value.extend_from_slice(&0x003Eu16.to_le_bytes());
    value.extend_from_slice(&0x0002u16.to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
    value.extend_from_slice(&[0; 14]);
    value.extend_from_slice(&tz.bias.to_le_bytes());
    value.extend_from_slice(&tz.standard_bias.to_le_bytes());
    value.extend_from_slice(&tz.daylight_bias.to_le_bytes());
    push_system_time(value, tz.standard_date);
    push_system_time(value, tz.daylight_date);
}

fn push_system_time(value: &mut Vec<u8>, system_time: CalendarSystemTime) {
    value.extend_from_slice(&system_time.year.to_le_bytes());
    value.extend_from_slice(&system_time.month.to_le_bytes());
    value.extend_from_slice(&system_time.day_of_week.to_le_bytes());
    value.extend_from_slice(&system_time.day.to_le_bytes());
    value.extend_from_slice(&system_time.hour.to_le_bytes());
    value.extend_from_slice(&system_time.minute.to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
}

fn calendar_global_object_id(event: &AccessibleEvent) -> Vec<u8> {
    let uid = if event.uid.is_empty() {
        event.id.to_string()
    } else {
        event.uid.clone()
    };
    let mut data = b"vCal-Uid".to_vec();
    data.extend_from_slice(&1u32.to_le_bytes());
    data.extend_from_slice(uid.as_bytes());

    let mut value = vec![
        0x04, 0x00, 0x00, 0x00, 0x82, 0x00, 0xE0, 0x00, 0x74, 0xC5, 0xB7, 0x10, 0x1A, 0x82, 0xE0,
        0x08,
    ];
    value.extend_from_slice(&[0, 0, 0, 0]);
    value.extend_from_slice(&0u64.to_le_bytes());
    value.extend_from_slice(&0u64.to_le_bytes());
    value.extend_from_slice(&(data.len().min(u32::MAX as usize) as u32).to_le_bytes());
    value.extend_from_slice(&data);
    value
}

pub(in crate::mapi) fn task_property_value(
    task: &ClientTask,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
) -> Option<MapiValue> {
    task_property_value_with_reminder(task, item_id, folder_id, property_tag, None)
}

pub(in crate::mapi) fn task_property_value_with_reminder(
    task: &ClientTask,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
    reminder: Option<&lpe_storage::ClientReminder>,
) -> Option<MapiValue> {
    if let Some(value) = task_reminder_property_value(reminder, property_tag) {
        return Some(value);
    }
    let property_tag = canonical_property_storage_tag(property_tag);
    let change_number = mapi_mailstore::change_number_for_store_id(item_id);
    match property_tag {
        PID_TAG_MID => Some(MapiValue::U64(item_id)),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(task.title.clone()))
        }
        PID_TAG_BODY_W => Some(MapiValue::String(task.description.clone())),
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Task".to_string())),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_FLAG_STATUS => Some(MapiValue::U32(task_flag_status(task))),
        PID_LID_PERCENT_COMPLETE_TAG => Some(MapiValue::F64(task_percent_complete(task).to_bits())),
        PID_LID_RECURRING_TAG => Some(MapiValue::Bool(!task.recurrence_rule.trim().is_empty())),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(task_size(task))),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(task_size(task))),
        PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_rfc3339_utc(&task.updated_at),
        )),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(item_id),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &task.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ => None,
    }
}

fn event_reminder_property_value(
    event: &AccessibleEvent,
    reminder: Option<&lpe_storage::ClientReminder>,
    property_tag: u32,
) -> Option<MapiValue> {
    let reminder = reminder?;
    match property_tag {
        PID_LID_REMINDER_SET_TAG => Some(MapiValue::Bool(true)),
        PID_LID_REMINDER_DELTA_TAG => Some(MapiValue::I32(reminder_delta_minutes(
            event_start_filetime(event),
            &reminder.reminder_at,
        ))),
        PID_LID_REMINDER_OVERRIDE_TAG | PID_LID_REMINDER_PLAY_SOUND_TAG => {
            Some(MapiValue::Bool(false))
        }
        PID_LID_REMINDER_FILE_PARAMETER_W_TAG => Some(MapiValue::String(String::new())),
        PID_LID_REMINDER_SIGNAL_TIME_TAG => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_rfc3339_utc(&reminder.reminder_at),
        )),
        PID_LID_REMINDER_TIME_TAG => Some(MapiValue::U64(event_start_filetime(event))),
        _ => None,
    }
}

fn task_reminder_property_value(
    reminder: Option<&lpe_storage::ClientReminder>,
    property_tag: u32,
) -> Option<MapiValue> {
    let reminder = reminder?;
    match property_tag {
        PID_LID_REMINDER_SET_TAG => Some(MapiValue::Bool(true)),
        PID_LID_REMINDER_DELTA_TAG => Some(MapiValue::I32(
            reminder
                .due_at
                .as_deref()
                .map(|due_at| {
                    reminder_delta_minutes(
                        mapi_mailstore::filetime_from_rfc3339_utc(due_at),
                        &reminder.reminder_at,
                    )
                })
                .unwrap_or_default(),
        )),
        PID_LID_REMINDER_OVERRIDE_TAG | PID_LID_REMINDER_PLAY_SOUND_TAG => {
            Some(MapiValue::Bool(false))
        }
        PID_LID_REMINDER_FILE_PARAMETER_W_TAG => Some(MapiValue::String(String::new())),
        PID_LID_REMINDER_TIME_TAG | PID_LID_REMINDER_SIGNAL_TIME_TAG => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_rfc3339_utc(&reminder.reminder_at),
        )),
        _ => None,
    }
}

fn reminder_delta_minutes(anchor_filetime: u64, reminder_at: &str) -> i32 {
    let reminder_filetime = mapi_mailstore::filetime_from_rfc3339_utc(reminder_at);
    if anchor_filetime <= reminder_filetime {
        return 0;
    }
    ((anchor_filetime - reminder_filetime) / 600_000_000).min(i32::MAX as u64) as i32
}

pub(in crate::mapi) fn note_property_value(
    note: &ClientNote,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
) -> Option<MapiValue> {
    if let Some(value) = note_named_property_value(note, property_tag) {
        return Some(value);
    }
    let property_tag = canonical_property_storage_tag(property_tag);
    let change_number = mapi_mailstore::change_number_for_store_id(item_id);
    match property_tag {
        PID_TAG_MID => Some(MapiValue::U64(item_id)),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(note.title.clone()))
        }
        PID_TAG_BODY_W => Some(MapiValue::String(note.body_text.clone())),
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.StickyNote".to_string())),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(note_size(note))),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(note_size(note))),
        PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_rfc3339_utc(&note.updated_at),
        )),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(item_id),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &note.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ => None,
    }
}

pub(in crate::mapi) fn journal_entry_property_value(
    entry: &JournalEntry,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
) -> Option<MapiValue> {
    if let Some(value) = journal_entry_named_property_value(entry, property_tag) {
        return Some(value);
    }
    let property_tag = canonical_property_storage_tag(property_tag);
    let change_number = mapi_mailstore::change_number_for_store_id(item_id);
    match property_tag {
        PID_TAG_MID => Some(MapiValue::U64(item_id)),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(entry.subject.clone()))
        }
        PID_TAG_BODY_W => Some(MapiValue::String(entry.body_text.clone())),
        PID_TAG_START_DATE | PID_TAG_MESSAGE_DELIVERY_TIME => entry
            .starts_at
            .as_deref()
            .or(entry.occurred_at.as_deref())
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_TAG_END_DATE => entry
            .ends_at
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_rfc3339_utc(&entry.updated_at),
        )),
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(entry.message_class.clone())),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(journal_entry_size(entry))),
        PID_TAG_MESSAGE_SIZE_EXTENDED => {
            Some(mapi_message_size_extended_value(journal_entry_size(entry)))
        }
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(item_id),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &entry.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ => None,
    }
}

pub(in crate::mapi) fn note_named_property_value(
    note: &ClientNote,
    property_tag: u32,
) -> Option<MapiValue> {
    match property_tag {
        PID_LID_NOTE_COLOR_TAG => Some(MapiValue::I32(note_color_value(&note.color))),
        _ => None,
    }
}

pub(in crate::mapi) fn journal_entry_named_property_value(
    entry: &JournalEntry,
    property_tag: u32,
) -> Option<MapiValue> {
    match property_tag {
        PID_LID_COMMON_START_TAG | PID_LID_LOG_START_TAG => entry
            .starts_at
            .as_deref()
            .or(entry.occurred_at.as_deref())
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_LID_COMMON_END_TAG | PID_LID_LOG_END_TAG => entry
            .ends_at
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_LID_COMPANIES_TAG => Some(MapiValue::MultiString(json_string_array(
            &entry.companies_json,
        ))),
        PID_LID_CONTACTS_TAG => Some(MapiValue::MultiString(json_string_array(
            &entry.contacts_json,
        ))),
        PID_LID_CONTACT_LINK_NAME_W_TAG | PID_LID_CONTACT_LINK_NAME_STRING8_TAG => {
            let names = json_string_array(&entry.contacts_json);
            (!names.is_empty()).then(|| MapiValue::String(names.join("; ")))
        }
        PID_LID_CONTACT_LINK_ENTRY_TAG => Some(MapiValue::Binary(empty_contact_link_entry_blob())),
        PID_LID_CONTACT_LINK_SEARCH_KEY_TAG => {
            Some(MapiValue::Binary(empty_contact_link_search_key_blob()))
        }
        PID_LID_LOG_TYPE_W_TAG | PID_LID_LOG_TYPE_STRING8_TAG => {
            Some(MapiValue::String(entry.entry_type.clone()))
        }
        PID_LID_LOG_TYPE_DESC_W_TAG | PID_LID_LOG_TYPE_DESC_STRING8_TAG => {
            Some(MapiValue::String(entry.entry_type.clone()))
        }
        PID_LID_LOG_DURATION_TAG => Some(MapiValue::I32(0)),
        PID_LID_LOG_FLAGS_TAG => Some(MapiValue::I32(0)),
        _ => None,
    }
}

fn note_color_value(color: &str) -> i32 {
    match color.trim().to_ascii_lowercase().as_str() {
        "blue" => 0,
        "green" => 1,
        "pink" => 2,
        "white" => 4,
        _ => 3,
    }
}

fn note_color_name(value: i64) -> &'static str {
    match value {
        0 => "blue",
        1 => "green",
        2 => "pink",
        4 => "white",
        _ => "yellow",
    }
}

fn json_string_array(value: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(value).unwrap_or_default()
}

fn contact_names_from_link_name(value: &str) -> Vec<String> {
    value
        .split(';')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn empty_contact_link_entry_blob() -> Vec<u8> {
    0u32.to_le_bytes().to_vec()
}

fn empty_contact_link_search_key_blob() -> Vec<u8> {
    0u16.to_le_bytes().to_vec()
}

fn json_from_mapi_multi_string_value(
    properties: &HashMap<u32, MapiValue>,
    tag: u32,
) -> Option<String> {
    match properties.get(&tag) {
        Some(MapiValue::MultiString(values)) => serde_json::to_string(values).ok(),
        Some(MapiValue::String(value)) if !value.trim().is_empty() => {
            serde_json::to_string(&vec![value.clone()]).ok()
        }
        _ => None,
    }
}

fn json_from_mapi_multi_string(
    properties: &HashMap<u32, MapiValue>,
    tag: u32,
    existing: &str,
) -> String {
    json_from_mapi_multi_string_value(properties, tag).unwrap_or_else(|| existing.to_string())
}

pub(in crate::mapi) fn note_size(note: &ClientNote) -> i64 {
    note.title
        .len()
        .saturating_add(note.body_text.len())
        .min(i64::MAX as usize) as i64
}

pub(in crate::mapi) fn journal_entry_size(entry: &JournalEntry) -> i64 {
    entry
        .subject
        .len()
        .saturating_add(entry.body_text.len())
        .saturating_add(entry.entry_type.len())
        .saturating_add(entry.companies_json.len())
        .saturating_add(entry.contacts_json.len())
        .min(i64::MAX as usize) as i64
}

fn task_flag_status(task: &ClientTask) -> u32 {
    if task.status == "completed" {
        FOLLOWUP_COMPLETE
    } else {
        FOLLOWUP_FLAGGED
    }
}

fn task_percent_complete(task: &ClientTask) -> f64 {
    if task.status == "completed" {
        1.0
    } else {
        0.0
    }
}

fn email_percent_complete(email: &JmapEmail) -> f64 {
    if email.followup_flag_status == "complete" {
        1.0
    } else {
        0.0
    }
}

pub(in crate::mapi) fn mapi_message_size_value(size_octets: i64) -> MapiValue {
    MapiValue::U32(size_octets.clamp(0, i64::from(u32::MAX)) as u32)
}

pub(in crate::mapi) fn mapi_message_size_extended_value(size_octets: i64) -> MapiValue {
    MapiValue::I64(size_octets.max(0))
}

pub(in crate::mapi) fn attachment_property_value(
    attachment: &MapiAttachment,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    match property_tag {
        PID_TAG_ATTACH_NUM => Some(MapiValue::U32(attachment.attach_num)),
        PID_TAG_DISPLAY_NAME_W | PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
            Some(MapiValue::String(attachment.file_name.clone()))
        }
        PID_TAG_ATTACH_EXTENSION_W => Some(MapiValue::String(attachment_file_extension(
            &attachment.file_name,
        ))),
        PID_TAG_ATTACH_MIME_TAG_W => Some(MapiValue::String(attachment.media_type.clone())),
        PID_TAG_ATTACH_SIZE => Some(MapiValue::U32(
            attachment.size_octets.min(u64::from(u32::MAX)) as u32,
        )),
        PID_TAG_ATTACH_METHOD => Some(MapiValue::U32(attachment_method_value(attachment))),
        PID_TAG_RENDERING_POSITION => Some(MapiValue::U32(u32::MAX)),
        PID_TAG_ATTACHMENT_FLAGS | PID_TAG_ATTACHMENT_LINK_ID => Some(MapiValue::U32(0)),
        PID_TAG_ATTACH_FLAGS => Some(MapiValue::U32(if attachment.content_id.is_some() {
            4
        } else {
            0
        })),
        PID_TAG_ATTACHMENT_HIDDEN => Some(MapiValue::Bool(attachment_is_inline(attachment))),
        PID_TAG_ATTACH_CONTENT_ID_W => Some(MapiValue::String(
            attachment.content_id.clone().unwrap_or_default(),
        )),
        PID_TAG_ATTACH_RENDERING => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_CREATION_TIME | PID_TAG_LAST_MODIFICATION_TIME => Some(MapiValue::U64(0)),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            attachment.file_reference.as_bytes().to_vec(),
        )),
        _ => None,
    }
}

pub(in crate::mapi) fn attachment_is_inline(attachment: &MapiAttachment) -> bool {
    attachment
        .disposition
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("inline"))
        || attachment.content_id.is_some()
}

pub(in crate::mapi) fn attachment_is_embedded_message(attachment: &MapiAttachment) -> bool {
    attachment_metadata_is_embedded_message(&attachment.media_type, &attachment.file_name)
}

pub(in crate::mapi) fn attachment_metadata_is_embedded_message(
    media_type: &str,
    file_name: &str,
) -> bool {
    media_type
        .trim()
        .eq_ignore_ascii_case("application/vnd.ms-outlook")
        || file_name.trim().to_ascii_lowercase().ends_with(".msg")
}

pub(in crate::mapi) fn attachment_method_value(attachment: &MapiAttachment) -> u32 {
    if attachment_is_embedded_message(attachment) {
        ATTACH_EMBEDDED_MESSAGE
    } else {
        ATTACH_BY_VALUE
    }
}

pub(in crate::mapi) fn attachment_method_value_from_metadata(
    media_type: &str,
    file_name: &str,
) -> u32 {
    if attachment_metadata_is_embedded_message(media_type, file_name) {
        ATTACH_EMBEDDED_MESSAGE
    } else {
        ATTACH_BY_VALUE
    }
}

pub(in crate::mapi) fn attachment_file_extension(file_name: &str) -> String {
    let file_name = file_name.trim();
    file_name
        .rsplit_once('.')
        .filter(|(base, ext)| !base.is_empty() && !ext.is_empty())
        .map(|(_, ext)| format!(".{ext}"))
        .unwrap_or_default()
}

pub(in crate::mapi) fn compare_mapi_values(left: &MapiValue, right: &MapiValue, relop: u8) -> bool {
    if let (Some(left), Some(right)) = (left.as_i64(), right.as_i64()) {
        return compare_i64(left, right, relop);
    }
    if let (Some(left), Some(right)) = (left.as_text(), right.as_text()) {
        return compare_ordering(compare_case_insensitive(left, right), relop);
    }
    if let (Some(left), Some(right)) = (left.as_bool(), right.as_bool()) {
        return compare_ordering(left.cmp(&right), relop);
    }
    if let Some(ordering) = compare_folder_entry_id_values(left, right) {
        return compare_ordering(ordering, relop);
    }
    compare_ordering(left.cmp_value(right), relop)
}

fn compare_folder_entry_id_values(left: &MapiValue, right: &MapiValue) -> Option<Ordering> {
    let (MapiValue::Binary(left), MapiValue::Binary(right)) = (left, right) else {
        return None;
    };
    let left = crate::mapi::identity::object_id_from_folder_entry_id(left)?;
    let right = crate::mapi::identity::object_id_from_folder_entry_id(right)?;
    Some(left.cmp(&right))
}

pub(in crate::mapi) fn compare_i64(left: i64, right: i64, relop: u8) -> bool {
    compare_ordering(left.cmp(&right), relop)
}

pub(in crate::mapi) fn compare_ordering(ordering: Ordering, relop: u8) -> bool {
    match relop {
        0x00 => ordering == Ordering::Less,
        0x01 => matches!(ordering, Ordering::Less | Ordering::Equal),
        0x02 => ordering == Ordering::Greater,
        0x03 => matches!(ordering, Ordering::Greater | Ordering::Equal),
        0x04 => ordering == Ordering::Equal,
        0x05 => ordering != Ordering::Equal,
        _ => false,
    }
}

impl MapiValue {
    pub(in crate::mapi) fn as_i64(&self) -> Option<i64> {
        match self {
            MapiValue::Bool(value) => Some(i64::from(*value)),
            MapiValue::I16(value) => Some(i64::from(*value)),
            MapiValue::I32(value) => Some(i64::from(*value)),
            MapiValue::I64(value) => Some(*value),
            MapiValue::U32(value) => Some(i64::from(*value)),
            MapiValue::U64(value) => i64::try_from(*value).ok(),
            MapiValue::F64(_)
            | MapiValue::String(_)
            | MapiValue::Binary(_)
            | MapiValue::Guid(_)
            | MapiValue::Error(_)
            | MapiValue::MultiI16(_)
            | MapiValue::MultiI32(_)
            | MapiValue::MultiI64(_)
            | MapiValue::MultiString(_)
            | MapiValue::MultiBinary(_)
            | MapiValue::MultiGuid(_) => None,
        }
    }

    pub(in crate::mapi) fn as_bool(&self) -> Option<bool> {
        match self {
            MapiValue::Bool(value) => Some(*value),
            MapiValue::I16(value) => Some(*value != 0),
            MapiValue::I32(value) => Some(*value != 0),
            MapiValue::I64(value) => Some(*value != 0),
            MapiValue::U32(value) => Some(*value != 0),
            MapiValue::U64(value) => Some(*value != 0),
            MapiValue::F64(value) => Some(f64::from_bits(*value) != 0.0),
            MapiValue::String(_)
            | MapiValue::Binary(_)
            | MapiValue::Guid(_)
            | MapiValue::Error(_)
            | MapiValue::MultiI16(_)
            | MapiValue::MultiI32(_)
            | MapiValue::MultiI64(_)
            | MapiValue::MultiString(_)
            | MapiValue::MultiBinary(_)
            | MapiValue::MultiGuid(_) => None,
        }
    }

    pub(in crate::mapi) fn as_text(&self) -> Option<&str> {
        match self {
            MapiValue::String(value) => Some(value),
            _ => None,
        }
    }

    pub(in crate::mapi) fn into_text(self) -> Option<String> {
        match self {
            MapiValue::Bool(value) => Some(value.to_string()),
            MapiValue::I16(value) => Some(value.to_string()),
            MapiValue::I32(value) => Some(value.to_string()),
            MapiValue::I64(value) => Some(value.to_string()),
            MapiValue::U32(value) => Some(value.to_string()),
            MapiValue::U64(value) => Some(value.to_string()),
            MapiValue::F64(value) => Some(f64::from_bits(value).to_string()),
            MapiValue::String(value) => Some(value),
            MapiValue::Binary(_)
            | MapiValue::Guid(_)
            | MapiValue::Error(_)
            | MapiValue::MultiI16(_)
            | MapiValue::MultiI32(_)
            | MapiValue::MultiI64(_)
            | MapiValue::MultiString(_)
            | MapiValue::MultiBinary(_)
            | MapiValue::MultiGuid(_) => None,
        }
    }

    pub(in crate::mapi) fn into_u32(self) -> Option<u32> {
        match self {
            MapiValue::Bool(value) => Some(u32::from(value)),
            MapiValue::I16(value) => u32::try_from(value).ok(),
            MapiValue::I32(value) => u32::try_from(value).ok(),
            MapiValue::I64(value) => u32::try_from(value).ok(),
            MapiValue::U32(value) => Some(value),
            MapiValue::U64(value) => u32::try_from(value).ok(),
            MapiValue::Error(value) => Some(value),
            MapiValue::F64(value) => {
                let value = f64::from_bits(value);
                if value.is_finite() && value >= 0.0 && value <= f64::from(u32::MAX) {
                    Some(value as u32)
                } else {
                    None
                }
            }
            MapiValue::String(_)
            | MapiValue::Binary(_)
            | MapiValue::Guid(_)
            | MapiValue::MultiI16(_)
            | MapiValue::MultiI32(_)
            | MapiValue::MultiI64(_)
            | MapiValue::MultiString(_)
            | MapiValue::MultiBinary(_)
            | MapiValue::MultiGuid(_) => None,
        }
    }

    pub(in crate::mapi) fn size(&self) -> usize {
        match self {
            MapiValue::Bool(_) => 1,
            MapiValue::I16(_) => 2,
            MapiValue::I32(_) | MapiValue::U32(_) => 4,
            MapiValue::I64(_) | MapiValue::U64(_) | MapiValue::F64(_) => 8,
            MapiValue::String(value) => value.encode_utf16().count() * 2,
            MapiValue::Binary(value) => value.len(),
            MapiValue::Guid(_) => 16,
            MapiValue::Error(_) => 4,
            MapiValue::MultiI16(values) => 4 + values.len() * 2,
            MapiValue::MultiI32(values) => 4 + values.len() * 4,
            MapiValue::MultiI64(values) => 4 + values.len() * 8,
            MapiValue::MultiString(values) => {
                4 + values
                    .iter()
                    .map(|value| value.encode_utf16().count() * 2 + 2)
                    .sum::<usize>()
            }
            MapiValue::MultiBinary(values) => {
                4 + values.iter().map(|value| 2 + value.len()).sum::<usize>()
            }
            MapiValue::MultiGuid(values) => 4 + values.len() * 16,
        }
    }

    pub(in crate::mapi) fn cmp_value(&self, other: &MapiValue) -> Ordering {
        format!("{self:?}").cmp(&format!("{other:?}"))
    }
}

pub(in crate::mapi) async fn attachment_stream_data<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    input_handle: u32,
    open_mode: u8,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<(Vec<u8>, Option<StreamWriteTarget>)> {
    match session.handles.get(&input_handle)?.clone() {
        MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        } if open_mode == 0 => {
            let attachment = snapshot.attachment_for_message(folder_id, message_id, attach_num)?;
            let content = store
                .fetch_attachment_content(principal.account_id, &attachment.file_reference)
                .await
                .ok()??;
            Some((content.blob_bytes, None))
        }
        MapiObject::PendingAttachment { data, .. } => match open_mode {
            0 => Some((data, None)),
            1 => Some((
                data,
                Some(StreamWriteTarget::PendingAttachment(input_handle)),
            )),
            2 => {
                if let Some(MapiObject::PendingAttachment { data, .. }) =
                    session.handles.get_mut(&input_handle)
                {
                    data.clear();
                }
                Some((
                    Vec::new(),
                    Some(StreamWriteTarget::PendingAttachment(input_handle)),
                ))
            }
            _ => None,
        },
        MapiObject::SavedAttachment { file_reference, .. } if open_mode == 0 => {
            let content = store
                .fetch_attachment_content(principal.account_id, &file_reference)
                .await
                .ok()??;
            Some((content.blob_bytes, None))
        }
        _ => None,
    }
}

pub(in crate::mapi) async fn open_stream_data<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    input_handle: u32,
    property_tag: u32,
    open_mode: u8,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Option<(Vec<u8>, Option<StreamWriteTarget>)> {
    match property_tag {
        PID_TAG_ATTACH_DATA_BINARY => {
            attachment_stream_data(store, principal, session, input_handle, open_mode, snapshot)
                .await
        }
        PID_TAG_BODY_STRING8
        | PID_TAG_BODY_W
        | PID_TAG_RTF_COMPRESSED
        | PID_TAG_BODY_HTML_W
        | PID_TAG_HTML_BINARY => message_body_stream_data(
            session,
            input_handle,
            property_tag,
            open_mode,
            mailboxes,
            emails,
            snapshot,
        ),
        _ => property_stream_data(
            session,
            input_handle,
            property_tag,
            open_mode,
            mailboxes,
            principal.account_id,
            snapshot,
        ),
    }
}

fn property_stream_data(
    session: &mut MapiSession,
    input_handle: u32,
    property_tag: u32,
    open_mode: u8,
    mailboxes: &[JmapMailbox],
    mailbox_guid: Uuid,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<(Vec<u8>, Option<StreamWriteTarget>)> {
    let object = session.handles.get(&input_handle)?;
    let writable_associated_config = matches!(
        (object, open_mode),
        (MapiObject::AssociatedConfig { .. }, 1 | 2)
    );
    let writable_common_view_named_view =
        matches!(
            (object, open_mode),
            (MapiObject::CommonViewNamedView { .. }, 1 | 2)
        ) && common_view_named_view_stream_property_is_writable(property_tag);
    if open_mode != 0 && !writable_associated_config && !writable_common_view_named_view {
        return None;
    }
    let allow_empty_missing_stream = !matches!(object, MapiObject::AssociatedConfig { .. });
    let value = match object {
        MapiObject::Folder {
            folder_id,
            properties,
        } => properties
            .get(&canonical_property_storage_tag(property_tag))
            .cloned()
            .or_else(|| {
                mailboxes
                    .iter()
                    .find(|mailbox| mapi_folder_id(mailbox) == *folder_id)
                    .and_then(|mailbox| {
                        mailbox_property_value_with_context_for_account(
                            mailbox,
                            mailboxes,
                            property_tag,
                            mailbox_guid,
                        )
                    })
            }),
        MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            saved_message,
        } => snapshot
            .associated_config_message_for_id(*config_id)
            .or_else(|| saved_message.clone())
            .filter(|message| message.folder_id == *folder_id)
            .and_then(|message| {
                associated_config_property_value_with_mailbox_guid(
                    &message,
                    mailbox_guid,
                    property_tag,
                )
            }),
        MapiObject::CommonViewNamedView { folder_id, view_id } => snapshot
            .named_view_message_for_folder_and_id(*folder_id, *view_id)
            .and_then(|message| {
                common_view_named_view_property_value(&message, mailbox_guid, property_tag)
            }),
        _ => return None,
    };
    let stream = match value {
        Some(value) => mapi_value_stream_bytes(property_tag, value)?,
        None if allow_empty_missing_stream || writable_associated_config => {
            empty_stream_bytes_for_property_tag(property_tag)?
        }
        None => return None,
    };
    let target = if writable_associated_config {
        Some(StreamWriteTarget::AssociatedConfigProperty {
            handle: input_handle,
            property_tag,
        })
    } else if writable_common_view_named_view {
        Some(StreamWriteTarget::VolatileProperty)
    } else {
        None
    };
    Some((stream, target))
}

fn common_view_named_view_stream_property_is_writable(property_tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(property_tag),
        PID_TAG_VIEW_DESCRIPTOR_BINARY
            | OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835
            | PID_TAG_VIEW_DESCRIPTOR_STRINGS_W
            | OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C
            | OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
    )
}

fn mapi_value_stream_bytes(property_tag: u32, value: MapiValue) -> Option<Vec<u8>> {
    match value {
        MapiValue::Binary(value) => Some(value),
        MapiValue::String(value)
            if canonical_property_storage_tag(property_tag)
                == PID_TAG_VIEW_DESCRIPTOR_STRINGS_W =>
        {
            Some(utf16_bytes(&value))
        }
        MapiValue::String(value) if property_tag_type(property_tag) == 0x001E => {
            Some(string8z_bytes(&value))
        }
        MapiValue::String(value) => Some(utf16z_bytes(&value)),
        _ => None,
    }
}

fn empty_stream_bytes_for_property_tag(property_tag: u32) -> Option<Vec<u8>> {
    match property_tag_type(property_tag) {
        0x0102 => Some(Vec::new()),
        0x001E => Some(string8z_bytes("")),
        0x001F => Some(utf16z_bytes("")),
        _ => None,
    }
}

fn property_tag_type(property_tag: u32) -> u32 {
    property_tag & 0x0000_FFFF
}

pub(in crate::mapi) fn message_body_stream_data(
    session: &MapiSession,
    input_handle: u32,
    property_tag: u32,
    open_mode: u8,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Option<(Vec<u8>, Option<StreamWriteTarget>)> {
    let (body_text, body_html) = match session.handles.get(&input_handle)? {
        MapiObject::Message {
            folder_id,
            message_id,
            saved_email,
            ..
        } if open_mode == 0 => {
            let email = message_for_id(*folder_id, *message_id, mailboxes, emails)
                .or(saved_email.as_ref().map(|saved| &saved.email))?;
            (email.body_text.clone(), email.body_html_sanitized.clone())
        }
        MapiObject::PendingMessage { properties, .. }
        | MapiObject::PendingAssociatedMessage { properties, .. } => match open_mode {
            0 | 1 => (
                pending_text_property(properties, &[PID_TAG_BODY_W]),
                optional_pending_text_property(properties, &[PID_TAG_BODY_HTML_W])
                    .or_else(|| pending_html_binary_property(properties)),
            ),
            2 => (String::new(), Some(String::new())),
            _ => return None,
        },
        MapiObject::PublicFolderItem {
            folder_id,
            item_id,
            properties,
        } => match open_mode {
            0 | 1 => {
                let item = snapshot.public_folder_item_for_id(*folder_id, *item_id)?;
                (
                    optional_pending_text_property(properties, &[PID_TAG_BODY_W])
                        .unwrap_or_else(|| item.item.body_text.clone()),
                    optional_pending_text_property(properties, &[PID_TAG_BODY_HTML_W])
                        .or_else(|| pending_html_binary_property(properties))
                        .or_else(|| item.item.body_html_sanitized.clone()),
                )
            }
            2 => (String::new(), Some(String::new())),
            _ => return None,
        },
        MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            saved_message,
        } if open_mode == 0 => {
            let message = snapshot
                .associated_config_message_for_id(*config_id)
                .or_else(|| saved_message.clone())
                .filter(|message| message.folder_id == *folder_id)?;
            let body_text = match associated_config_property_value(&message, PID_TAG_BODY_W) {
                Some(MapiValue::String(value)) => value,
                _ => String::new(),
            };
            let body_html = match associated_config_property_value(&message, PID_TAG_BODY_HTML_W) {
                Some(MapiValue::String(value)) => Some(value),
                _ => match associated_config_property_value(&message, PID_TAG_HTML_BINARY) {
                    Some(MapiValue::Binary(value)) => String::from_utf8(value).ok(),
                    Some(MapiValue::String(value)) => Some(value),
                    _ => None,
                },
            };
            (body_text, body_html)
        }
        _ => return None,
    };

    let body_html = body_html.or_else(|| html_body_from_plain_text(&body_text));
    let stream = match (property_tag, open_mode) {
        (_, 2) => Vec::new(),
        (PID_TAG_BODY_STRING8, _) => string8z_bytes(&body_text),
        (PID_TAG_BODY_W, _) => utf16z_bytes(&body_text),
        (PID_TAG_RTF_COMPRESSED, _) => uncompressed_rtf_body(&body_text),
        (PID_TAG_BODY_HTML_W, _) => utf16z_bytes(body_html.as_deref().unwrap_or("")),
        (PID_TAG_HTML_BINARY, _) => body_html.unwrap_or_default().into_bytes(),
        _ => return None,
    };
    let target = match (session.handles.get(&input_handle), open_mode) {
        (Some(MapiObject::PendingMessage { .. }), 1 | 2) => {
            Some(StreamWriteTarget::PendingMessageProperty {
                handle: input_handle,
                property_tag,
            })
        }
        (Some(MapiObject::PendingAssociatedMessage { .. }), 1 | 2) => {
            Some(StreamWriteTarget::PendingAssociatedMessageProperty {
                handle: input_handle,
                property_tag,
            })
        }
        (Some(MapiObject::PublicFolderItem { .. }), 1 | 2) => {
            Some(StreamWriteTarget::PublicFolderItemProperty {
                handle: input_handle,
                property_tag,
            })
        }
        _ => None,
    };
    Some((stream, target))
}

pub(in crate::mapi) fn utf16z_bytes(value: &str) -> Vec<u8> {
    let mut bytes = value
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes
}

fn utf16_bytes(value: &str) -> Vec<u8> {
    value
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>()
}

pub(in crate::mapi) fn string8z_bytes(value: &str) -> Vec<u8> {
    let mut bytes = value
        .bytes()
        .map(|byte| if byte.is_ascii() { byte } else { b'?' })
        .collect::<Vec<_>>();
    bytes.push(0);
    bytes
}

pub(in crate::mapi) fn pending_html_binary_property(
    properties: &HashMap<u32, MapiValue>,
) -> Option<String> {
    properties
        .get(&PID_TAG_HTML_BINARY)
        .and_then(|value| match value {
            MapiValue::Binary(bytes) => String::from_utf8(bytes.clone()).ok(),
            MapiValue::String(value) => Some(value.clone()),
            _ => None,
        })
}

pub(in crate::mapi) fn pending_html_property(
    properties: &HashMap<u32, MapiValue>,
) -> Option<String> {
    optional_pending_text_property(properties, &[PID_TAG_BODY_HTML_W])
        .or_else(|| pending_html_binary_property(properties))
        .filter(|value| !value.trim().is_empty())
}

pub(in crate::mapi) fn write_stream(
    session: &mut MapiSession,
    stream_handle: u32,
    bytes: &[u8],
) -> Option<usize> {
    let (updated_data, writable_target, written) = {
        let Some(MapiObject::AttachmentStream {
            data,
            position,
            writable_target: Some(writable_target),
        }) = session.handles.get_mut(&stream_handle)
        else {
            return None;
        };
        let start = *position;
        let end = start.checked_add(bytes.len())?;
        if data.len() < end {
            data.resize(end, 0);
        }
        data[start..end].copy_from_slice(bytes);
        *position = end;
        (data.clone(), *writable_target, bytes.len())
    };

    sync_stream_target(session, writable_target, updated_data)?;
    Some(written)
}

pub(in crate::mapi) fn resolve_writable_stream_handle(
    session: &MapiSession,
    requested_handle: u32,
) -> Option<u32> {
    if matches!(
        session.handles.get(&requested_handle),
        Some(MapiObject::AttachmentStream { .. })
    ) {
        return Some(requested_handle);
    }
    if !matches!(
        session.handles.get(&requested_handle),
        Some(MapiObject::AssociatedConfig { .. })
    ) {
        return None;
    }

    let mut matches = session
        .handles
        .iter()
        .filter_map(|(handle, object)| match object {
            MapiObject::AttachmentStream {
                writable_target:
                    Some(StreamWriteTarget::AssociatedConfigProperty {
                        handle: target_handle,
                        ..
                    }),
                ..
            } if *target_handle == requested_handle => Some(*handle),
            _ => None,
        });
    let handle = matches.next()?;
    matches.next().is_none().then_some(handle)
}

pub(in crate::mapi) fn stream_write_error(
    session: &MapiSession,
    stream_handle: u32,
) -> Option<StreamWriteError> {
    match session.handles.get(&stream_handle) {
        Some(MapiObject::AttachmentStream {
            writable_target: None,
            ..
        }) => Some(StreamWriteError::AccessDenied),
        Some(MapiObject::AttachmentStream { .. }) => None,
        _ => Some(StreamWriteError::NotFound),
    }
}

pub(in crate::mapi) fn stream_write_error_code(error: StreamWriteError) -> u32 {
    match error {
        StreamWriteError::NotFound => 0x8004_010F,
        StreamWriteError::AccessDenied => 0x8003_0005,
    }
}

pub(in crate::mapi) fn copy_stream(
    session: &mut MapiSession,
    source_handle: u32,
    destination_handle: u32,
    byte_count: u64,
) -> Option<(usize, usize)> {
    let requested = usize::try_from(byte_count).ok()?;
    let chunk = {
        let Some(MapiObject::AttachmentStream { data, position, .. }) =
            session.handles.get_mut(&source_handle)
        else {
            return None;
        };
        let end = position.saturating_add(requested).min(data.len());
        let chunk = data[*position..end].to_vec();
        *position = end;
        chunk
    };
    let written = write_stream(session, destination_handle, &chunk)?;
    Some((chunk.len(), written))
}

pub(in crate::mapi) fn sync_stream_target(
    session: &mut MapiSession,
    target: StreamWriteTarget,
    data: Vec<u8>,
) -> Option<()> {
    match target {
        StreamWriteTarget::PendingAttachment(handle) => {
            if let Some(MapiObject::PendingAttachment {
                data: attachment_data,
                ..
            }) = session.handles.get_mut(&handle)
            {
                *attachment_data = data;
                Some(())
            } else {
                None
            }
        }
        StreamWriteTarget::PendingMessageProperty {
            handle,
            property_tag,
        } => {
            let value = stream_property_value(property_tag, data)?;
            if let Some(MapiObject::PendingMessage { properties, .. }) =
                session.handles.get_mut(&handle)
            {
                properties.insert(canonical_property_storage_tag(property_tag), value);
                Some(())
            } else {
                None
            }
        }
        StreamWriteTarget::PendingAssociatedMessageProperty {
            handle,
            property_tag,
        } => {
            let value = stream_property_value(property_tag, data)?;
            if let Some(MapiObject::PendingAssociatedMessage { properties, .. }) =
                session.handles.get_mut(&handle)
            {
                properties.insert(canonical_property_storage_tag(property_tag), value);
                Some(())
            } else {
                None
            }
        }
        StreamWriteTarget::AssociatedConfigProperty {
            handle,
            property_tag,
        } => {
            let value = stream_property_value(property_tag, data)?;
            if let Some(MapiObject::AssociatedConfig {
                saved_message: Some(message),
                ..
            }) = session.handles.get_mut(&handle)
            {
                let mut properties = mapi_properties_from_json(&message.properties_json);
                properties.insert(canonical_property_storage_tag(property_tag), value);
                message.properties_json = mapi_properties_to_json(&properties);
                Some(())
            } else {
                None
            }
        }
        StreamWriteTarget::PublicFolderItemProperty {
            handle,
            property_tag,
        } => {
            let value = stream_property_value(property_tag, data)?;
            if let Some(MapiObject::PublicFolderItem { properties, .. }) =
                session.handles.get_mut(&handle)
            {
                properties.insert(canonical_property_storage_tag(property_tag), value);
                Some(())
            } else {
                None
            }
        }
        StreamWriteTarget::VolatileProperty => Some(()),
    }
}

pub(in crate::mapi) fn stream_property_value(
    property_tag: u32,
    data: Vec<u8>,
) -> Option<MapiValue> {
    match property_tag {
        PID_TAG_BODY_STRING8 => Some(MapiValue::String(decode_string8_stream_value(&data))),
        PID_TAG_BODY_W | PID_TAG_BODY_HTML_W => {
            Some(MapiValue::String(decode_utf16_stream_value(&data)?))
        }
        PID_TAG_HTML_BINARY => Some(MapiValue::Binary(data)),
        _ if property_tag_type(property_tag) == 0x0102 => Some(MapiValue::Binary(data)),
        _ => None,
    }
}

pub(in crate::mapi) fn decode_string8_stream_value(data: &[u8]) -> String {
    let value = data
        .strip_suffix(&[0])
        .or_else(|| data.strip_suffix(&[0, 0]))
        .unwrap_or(data);
    String::from_utf8_lossy(value).into_owned()
}

pub(in crate::mapi) fn decode_utf16_stream_value(data: &[u8]) -> Option<String> {
    let even_len = data.len() - (data.len() % 2);
    let mut units = data[..even_len]
        .chunks_exact(2)
        .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]))
        .collect::<Vec<_>>();
    if units.last().is_some_and(|unit| *unit == 0) {
        units.pop();
    }
    String::from_utf16(&units).ok()
}

pub(in crate::mapi) fn set_attachment_stream_size(
    session: &mut MapiSession,
    stream_handle: u32,
    stream_size: u64,
) -> Option<()> {
    let requested_size = usize::try_from(stream_size).ok()?;
    if requested_size > i32::MAX as usize {
        return None;
    }

    let (updated_data, writable_target) = {
        let Some(MapiObject::AttachmentStream {
            data,
            position,
            writable_target: Some(writable_target),
        }) = session.handles.get_mut(&stream_handle)
        else {
            return None;
        };
        data.resize(requested_size, 0);
        *position = (*position).min(data.len());
        (data.clone(), *writable_target)
    };

    sync_stream_target(session, writable_target, updated_data)
}

pub(in crate::mapi) fn pending_message_size(properties: &HashMap<u32, MapiValue>) -> i64 {
    let subject = pending_text_property(
        properties,
        &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
    );
    let body = pending_body_text_property(properties);
    subject
        .len()
        .saturating_add(body.len())
        .min(i64::MAX as usize) as i64
}

fn pending_body_text_property(properties: &HashMap<u32, MapiValue>) -> String {
    let body_text = pending_text_property(properties, &[PID_TAG_BODY_W]);
    if !body_text.trim().is_empty() {
        return body_text;
    }
    pending_html_property(properties)
        .map(|value| plain_text_from_html_body(&value))
        .unwrap_or_default()
}

pub(in crate::mapi) fn pending_text_property(
    properties: &HashMap<u32, MapiValue>,
    tags: &[u32],
) -> String {
    tags.iter()
        .find_map(|tag| {
            properties
                .get(tag)
                .and_then(|value| value.clone().into_text())
        })
        .unwrap_or_default()
}

pub(in crate::mapi) fn optional_pending_text_property(
    properties: &HashMap<u32, MapiValue>,
    tags: &[u32],
) -> Option<String> {
    tags.iter()
        .find_map(|tag| {
            properties
                .get(tag)
                .and_then(|value| value.clone().into_text())
        })
        .filter(|value| !value.trim().is_empty())
}

fn plain_text_from_html_body(html: &str) -> String {
    let mut text = String::new();
    let mut tag = String::new();
    let mut in_tag = false;
    for ch in html.chars() {
        match (in_tag, ch) {
            (false, '<') => {
                in_tag = true;
                tag.clear();
            }
            (true, '>') => {
                in_tag = false;
                if html_tag_is_line_break(&tag) && !text.ends_with('\n') {
                    text.push('\n');
                }
            }
            (true, _) => tag.push(ch),
            (false, _) => text.push(ch),
        }
    }
    decode_basic_html_entities(&text)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn html_tag_is_line_break(tag: &str) -> bool {
    let tag_name = tag
        .trim()
        .trim_start_matches('/')
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .trim_end_matches('/')
        .to_ascii_lowercase();
    matches!(tag_name.as_str(), "br" | "p" | "div" | "li")
}

fn decode_basic_html_entities(value: &str) -> String {
    value
        .replace("&nbsp;", " ")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

pub(in crate::mapi) fn default_mapping_rights() -> CollaborationRights {
    CollaborationRights {
        may_read: true,
        may_write: true,
        may_delete: true,
        may_share: false,
    }
}

pub(in crate::mapi) fn default_contact_for_mapping(
    account_id: Uuid,
    collection_id: &str,
) -> AccessibleContact {
    AccessibleContact {
        id: Uuid::nil(),
        collection_id: collection_id.to_string(),
        owner_account_id: account_id,
        owner_email: String::new(),
        owner_display_name: String::new(),
        rights: default_mapping_rights(),
        name: String::new(),
        role: String::new(),
        email: String::new(),
        phone: String::new(),
        team: String::new(),
        notes: String::new(),
        ..Default::default()
    }
}

pub(in crate::mapi) fn default_event_for_mapping(
    account_id: Uuid,
    collection_id: &str,
) -> AccessibleEvent {
    AccessibleEvent {
        id: Uuid::nil(),
        uid: Uuid::nil().to_string(),
        collection_id: collection_id.to_string(),
        owner_account_id: account_id,
        owner_email: String::new(),
        owner_display_name: String::new(),
        rights: default_mapping_rights(),
        date: "1970-01-01".to_string(),
        time: "00:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 0,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: String::new(),
        location: String::new(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: serialize_calendar_participants_metadata(
            &CalendarParticipantsMetadata::default(),
        ),
        notes: String::new(),
        body_html: String::new(),
    }
}

pub(in crate::mapi) fn default_event_input(
    account_id: Uuid,
    id: Option<Uuid>,
) -> UpsertClientEventInput {
    UpsertClientEventInput {
        id,
        account_id,
        uid: String::new(),
        date: "1970-01-01".to_string(),
        time: "00:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 0,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: String::new(),
        location: String::new(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: serialize_calendar_participants_metadata(
            &CalendarParticipantsMetadata::default(),
        ),
        notes: String::new(),
        body_html: String::new(),
    }
}

pub(in crate::mapi) fn default_task_for_mapping(
    account_id: Uuid,
    collection_id: &str,
) -> ClientTask {
    ClientTask {
        id: Uuid::nil(),
        owner_account_id: account_id,
        owner_email: String::new(),
        owner_display_name: String::new(),
        is_owned: true,
        rights: default_mapping_rights(),
        task_list_id: Uuid::nil(),
        task_list_sort_order: 0,
        title: String::new(),
        description: String::new(),
        status: "needs-action".to_string(),
        due_at: None,
        completed_at: None,
        recurrence_rule: String::new(),
        sort_order: if matches!(collection_id, "tasks" | "default") {
            0
        } else {
            10
        },
        updated_at: "1970-01-01T00:00:00Z".to_string(),
    }
}

pub(in crate::mapi) fn default_note_for_mapping() -> ClientNote {
    ClientNote {
        id: Uuid::nil(),
        title: String::new(),
        body_text: String::new(),
        color: "yellow".to_string(),
        categories_json: "[]".to_string(),
        created_at: "1970-01-01T00:00:00Z".to_string(),
        updated_at: "1970-01-01T00:00:00Z".to_string(),
    }
}

pub(in crate::mapi) fn default_journal_entry_for_mapping() -> JournalEntry {
    JournalEntry {
        id: Uuid::nil(),
        subject: String::new(),
        body_text: String::new(),
        entry_type: String::new(),
        message_class: "IPM.Activity".to_string(),
        starts_at: None,
        ends_at: None,
        occurred_at: None,
        companies_json: "[]".to_string(),
        contacts_json: "[]".to_string(),
        created_at: "1970-01-01T00:00:00Z".to_string(),
        updated_at: "1970-01-01T00:00:00Z".to_string(),
    }
}

pub(in crate::mapi) fn contact_input_from_mapi(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: &AccessibleContact,
    properties: &HashMap<u32, MapiValue>,
) -> UpsertClientContactInput {
    let mut structured_name = existing.structured_name.clone();
    if let Some(value) =
        optional_pending_text_property(properties, &[PID_TAG_DISPLAY_NAME_PREFIX_W])
    {
        structured_name.prefix = value;
    }
    if let Some(value) = optional_pending_text_property(properties, &[PID_TAG_GIVEN_NAME_W]) {
        structured_name.given = value;
    }
    if let Some(value) = optional_pending_text_property(properties, &[PID_TAG_MIDDLE_NAME_W]) {
        structured_name.middle = value;
    }
    if let Some(value) = optional_pending_text_property(properties, &[PID_TAG_SURNAME_W]) {
        structured_name.family = value;
    }
    if let Some(value) = optional_pending_text_property(properties, &[PID_TAG_GENERATION_W]) {
        structured_name.suffix = value;
    }
    if let Some(value) = optional_pending_text_property(properties, &[PID_TAG_NICKNAME_W]) {
        structured_name.nickname = value;
    }
    let name = optional_pending_text_property(
        properties,
        &[
            PID_TAG_DISPLAY_NAME_W,
            PID_TAG_SUBJECT_W,
            PID_TAG_NORMALIZED_SUBJECT_W,
        ],
    )
    .or_else(|| {
        (!structured_name.given.trim().is_empty() || !structured_name.family.trim().is_empty())
            .then(|| contact_display_name_from_structured(&structured_name))
            .filter(|value| !value.trim().is_empty())
    })
    .unwrap_or_else(|| existing.name.clone());
    let email = optional_pending_text_property(
        properties,
        &[PID_TAG_SMTP_ADDRESS_W, PID_TAG_EMAIL_ADDRESS_W],
    )
    .unwrap_or_else(|| existing.email.clone());
    let mobile_phone =
        optional_pending_text_property(properties, &[PID_TAG_MOBILE_TELEPHONE_NUMBER_W]);
    let business_phone =
        optional_pending_text_property(properties, &[PID_TAG_BUSINESS_TELEPHONE_NUMBER_W]);
    let home_phone = optional_pending_text_property(properties, &[PID_TAG_HOME_TELEPHONE_NUMBER_W]);
    let primary_phone =
        optional_pending_text_property(properties, &[PID_TAG_PRIMARY_TELEPHONE_NUMBER_W]);
    let phone = mobile_phone
        .clone()
        .or_else(|| business_phone.clone())
        .or_else(|| home_phone.clone())
        .or(primary_phone.clone())
        .unwrap_or_else(|| existing.phone.clone());
    let company = optional_pending_text_property(properties, &[PID_TAG_COMPANY_NAME_W])
        .unwrap_or_else(|| contact_organization_name(existing));
    let department = optional_pending_text_property(properties, &[PID_TAG_DEPARTMENT_NAME_W])
        .or_else(|| optional_pending_text_property(properties, &[PID_TAG_COMPANY_NAME_W]))
        .unwrap_or_else(|| existing.team.clone());
    let title = optional_pending_text_property(properties, &[PID_TAG_TITLE_W])
        .unwrap_or_else(|| contact_job_title(existing));
    let personal_url = optional_pending_text_property(properties, &[PID_TAG_PERSONAL_HOME_PAGE_W]);
    let business_url = optional_pending_text_property(properties, &[PID_TAG_BUSINESS_HOME_PAGE_W]);
    UpsertClientContactInput {
        id,
        account_id,
        name,
        role: title.clone(),
        email: email.clone(),
        phone: phone.clone(),
        team: department,
        notes: optional_pending_text_property(properties, &[PID_TAG_BODY_W])
            .unwrap_or_else(|| existing.notes.clone()),
        structured_name,
        emails_json: Some(update_primary_labeled_json(
            &existing.emails_json,
            "email",
            "work",
            &email,
        )),
        phones_json: Some(contact_phones_json_from_mapi(
            existing,
            &phone,
            mobile_phone.as_deref(),
            business_phone.as_deref().or(primary_phone.as_deref()),
            home_phone.as_deref(),
        )),
        urls_json: Some(contact_urls_json_from_mapi(
            &existing.urls_json,
            personal_url.as_deref(),
            business_url.as_deref(),
        )),
        organization_name: company,
        job_title: title,
        ..Default::default()
    }
}

fn contact_display_name_from_structured(name: &lpe_storage::ContactNameFields) -> String {
    [
        name.prefix.as_str(),
        name.given.as_str(),
        name.middle.as_str(),
        name.family.as_str(),
        name.suffix.as_str(),
    ]
    .into_iter()
    .map(str::trim)
    .filter(|value| !value.is_empty())
    .collect::<Vec<_>>()
    .join(" ")
}

fn update_primary_labeled_json(
    existing: &serde_json::Value,
    key: &str,
    label: &str,
    value: &str,
) -> serde_json::Value {
    let mut rows = existing.as_array().cloned().unwrap_or_default();
    if let Some(row) = rows.first_mut() {
        if let Some(object) = row.as_object_mut() {
            object.insert(
                key.to_string(),
                serde_json::Value::String(value.trim().to_string()),
            );
            object.insert(
                "label".to_string(),
                serde_json::Value::String(label.to_string()),
            );
            object.insert("isDefault".to_string(), serde_json::Value::Bool(true));
        }
    } else if !value.trim().is_empty() {
        rows.push(serde_json::json!({
            key: value.trim(),
            "label": label,
            "isDefault": true
        }));
    }
    serde_json::Value::Array(rows)
}

fn contact_phones_json_from_mapi(
    existing: &AccessibleContact,
    primary: &str,
    mobile: Option<&str>,
    business: Option<&str>,
    home: Option<&str>,
) -> serde_json::Value {
    let mut rows = Vec::new();
    push_labeled_value(&mut rows, "phone", "mobile", mobile);
    push_labeled_value(&mut rows, "phone", "work", business.or(Some(primary)));
    push_labeled_value(&mut rows, "phone", "home", home);
    if rows.is_empty() {
        existing.phones_json.clone()
    } else {
        serde_json::Value::Array(rows)
    }
}

fn contact_urls_json_from_mapi(
    existing: &serde_json::Value,
    personal: Option<&str>,
    business: Option<&str>,
) -> serde_json::Value {
    let mut rows = existing.as_array().cloned().unwrap_or_default();
    upsert_labeled_value(&mut rows, "url", "home", personal);
    upsert_labeled_value(&mut rows, "url", "work", business);
    serde_json::Value::Array(rows)
}

fn push_labeled_value(
    rows: &mut Vec<serde_json::Value>,
    key: &str,
    label: &str,
    value: Option<&str>,
) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        rows.push(serde_json::json!({ key: value, "label": label }));
    }
}

fn upsert_labeled_value(
    rows: &mut Vec<serde_json::Value>,
    key: &str,
    label: &str,
    value: Option<&str>,
) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    if let Some(row) = rows.iter_mut().find(|row| {
        row.get("label")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|current| current.eq_ignore_ascii_case(label))
    }) {
        if let Some(object) = row.as_object_mut() {
            object.insert(
                key.to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
    } else {
        rows.push(serde_json::json!({ key: value, "label": label }));
    }
}

fn reject_unsupported_mapi_contact_properties(properties: &HashMap<u32, MapiValue>) -> Result<()> {
    for tag in properties.keys() {
        let supported = matches!(
            canonical_property_storage_tag(*tag),
            PID_TAG_DISPLAY_NAME_W
                | PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_DISPLAY_NAME_PREFIX_W
                | PID_TAG_GIVEN_NAME_W
                | PID_TAG_MIDDLE_NAME_W
                | PID_TAG_SURNAME_W
                | PID_TAG_GENERATION_W
                | PID_TAG_NICKNAME_W
                | PID_TAG_TITLE_W
                | PID_TAG_SMTP_ADDRESS_W
                | PID_TAG_EMAIL_ADDRESS_W
                | PID_TAG_MOBILE_TELEPHONE_NUMBER_W
                | PID_TAG_BUSINESS_TELEPHONE_NUMBER_W
                | PID_TAG_HOME_TELEPHONE_NUMBER_W
                | PID_TAG_PRIMARY_TELEPHONE_NUMBER_W
                | PID_TAG_COMPANY_NAME_W
                | PID_TAG_DEPARTMENT_NAME_W
                | PID_TAG_PERSONAL_HOME_PAGE_W
                | PID_TAG_BUSINESS_HOME_PAGE_W
                | PID_TAG_BODY_W
        );
        if !supported {
            return Err(anyhow!(
                "MAPI contact property {tag:#010X} is outside the canonical contact subset"
            ));
        }
    }
    Ok(())
}

pub(in crate::mapi) fn note_input_from_mapi(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: &ClientNote,
    properties: &HashMap<u32, MapiValue>,
) -> UpsertClientNoteInput {
    UpsertClientNoteInput {
        id,
        account_id,
        title: optional_pending_text_property(
            properties,
            &[
                PID_TAG_SUBJECT_W,
                PID_TAG_NORMALIZED_SUBJECT_W,
                PID_TAG_DISPLAY_NAME_W,
            ],
        )
        .unwrap_or_else(|| existing.title.clone()),
        body_text: optional_pending_text_property(properties, &[PID_TAG_BODY_W])
            .unwrap_or_else(|| existing.body_text.clone()),
        color: properties
            .get(&PID_LID_NOTE_COLOR_TAG)
            .and_then(MapiValue::as_i64)
            .map(note_color_name)
            .unwrap_or(&existing.color)
            .to_string(),
        categories_json: existing.categories_json.clone(),
    }
}

fn reject_unsupported_mapi_note_properties(properties: &HashMap<u32, MapiValue>) -> Result<()> {
    for tag in properties.keys() {
        let supported = matches!(
            canonical_property_storage_tag(*tag),
            PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_DISPLAY_NAME_W
                | PID_TAG_BODY_W
                | PID_LID_NOTE_COLOR_TAG
        );
        if !supported {
            return Err(anyhow!(
                "MAPI note property {tag:#010X} is outside the canonical note subset"
            ));
        }
    }
    Ok(())
}

pub(in crate::mapi) fn journal_entry_input_from_mapi(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: &JournalEntry,
    properties: &HashMap<u32, MapiValue>,
) -> UpsertJournalEntryInput {
    UpsertJournalEntryInput {
        id,
        account_id,
        subject: optional_pending_text_property(
            properties,
            &[
                PID_TAG_SUBJECT_W,
                PID_TAG_NORMALIZED_SUBJECT_W,
                PID_TAG_DISPLAY_NAME_W,
            ],
        )
        .unwrap_or_else(|| existing.subject.clone()),
        body_text: optional_pending_text_property(properties, &[PID_TAG_BODY_W])
            .unwrap_or_else(|| existing.body_text.clone()),
        entry_type: optional_pending_text_property(
            properties,
            &[
                PID_LID_LOG_TYPE_W_TAG,
                PID_LID_LOG_TYPE_STRING8_TAG,
                PID_LID_LOG_TYPE_DESC_W_TAG,
                PID_LID_LOG_TYPE_DESC_STRING8_TAG,
            ],
        )
        .unwrap_or_else(|| existing.entry_type.clone()),
        message_class: optional_pending_text_property(properties, &[PID_TAG_MESSAGE_CLASS_W])
            .unwrap_or_else(|| existing.message_class.clone()),
        starts_at: properties
            .get(&PID_TAG_START_DATE)
            .or_else(|| properties.get(&PID_LID_COMMON_START_TAG))
            .or_else(|| properties.get(&PID_LID_LOG_START_TAG))
            .and_then(MapiValue::as_i64)
            .and_then(filetime_to_date_time)
            .map(|(date, time)| format!("{date}T{time}:00Z"))
            .or_else(|| existing.starts_at.clone()),
        ends_at: properties
            .get(&PID_TAG_END_DATE)
            .or_else(|| properties.get(&PID_LID_COMMON_END_TAG))
            .or_else(|| properties.get(&PID_LID_LOG_END_TAG))
            .and_then(MapiValue::as_i64)
            .and_then(filetime_to_date_time)
            .map(|(date, time)| format!("{date}T{time}:00Z"))
            .or_else(|| existing.ends_at.clone()),
        occurred_at: existing.occurred_at.clone(),
        companies_json: json_from_mapi_multi_string(
            properties,
            PID_LID_COMPANIES_TAG,
            &existing.companies_json,
        ),
        contacts_json: json_from_mapi_multi_string_value(properties, PID_LID_CONTACTS_TAG)
            .or_else(|| {
                optional_pending_text_property(
                    properties,
                    &[
                        PID_LID_CONTACT_LINK_NAME_W_TAG,
                        PID_LID_CONTACT_LINK_NAME_STRING8_TAG,
                    ],
                )
                .and_then(|value| {
                    let names = contact_names_from_link_name(&value);
                    (!names.is_empty()).then(|| serde_json::to_string(&names).ok())
                })
                .flatten()
            })
            .unwrap_or_else(|| existing.contacts_json.clone()),
    }
}

fn reject_unsupported_mapi_journal_entry_properties(
    properties: &HashMap<u32, MapiValue>,
) -> Result<()> {
    for tag in properties.keys() {
        let supported = matches!(
            canonical_property_storage_tag(*tag),
            PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_DISPLAY_NAME_W
                | PID_TAG_BODY_W
                | PID_TAG_MESSAGE_CLASS_W
                | PID_TAG_START_DATE
                | PID_TAG_END_DATE
                | PID_LID_COMMON_START_TAG
                | PID_LID_COMMON_END_TAG
                | PID_LID_LOG_START_TAG
                | PID_LID_LOG_END_TAG
                | PID_LID_LOG_TYPE_W_TAG
                | PID_LID_LOG_TYPE_STRING8_TAG
                | PID_LID_LOG_TYPE_DESC_W_TAG
                | PID_LID_LOG_TYPE_DESC_STRING8_TAG
                | PID_LID_COMPANIES_TAG
                | PID_LID_CONTACTS_TAG
                | PID_LID_CONTACT_LINK_NAME_W_TAG
                | PID_LID_CONTACT_LINK_NAME_STRING8_TAG
                | PID_LID_CONTACT_LINK_ENTRY_TAG
                | PID_LID_CONTACT_LINK_SEARCH_KEY_TAG
        );
        if !supported {
            return Err(anyhow!(
                "MAPI journal property {tag:#010X} is outside the canonical journal subset"
            ));
        }
    }
    Ok(())
}

pub(in crate::mapi) fn task_input_from_mapi(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: &ClientTask,
    collection_id: Option<&str>,
    properties: &HashMap<u32, MapiValue>,
) -> UpsertClientTaskInput {
    let title = optional_pending_text_property(
        properties,
        &[
            PID_TAG_SUBJECT_W,
            PID_TAG_NORMALIZED_SUBJECT_W,
            PID_TAG_DISPLAY_NAME_W,
        ],
    )
    .unwrap_or_else(|| existing.title.clone());
    let status = properties
        .get(&PID_TAG_FLAG_STATUS)
        .and_then(MapiValue::as_i64)
        .map(|value| {
            if value == FOLLOWUP_COMPLETE as i64 {
                "completed"
            } else {
                "needs-action"
            }
        })
        .unwrap_or(&existing.status)
        .to_string();
    let due_at = properties
        .get(&PID_TAG_END_DATE)
        .and_then(MapiValue::as_i64)
        .and_then(filetime_to_date_time)
        .map(|(date, time)| format!("{date}T{time}:00Z"))
        .or_else(|| existing.due_at.clone());
    UpsertClientTaskInput {
        id,
        principal_account_id: account_id,
        account_id,
        task_list_id: collection_id
            .and_then(|value| Uuid::parse_str(value).ok())
            .or_else(|| (existing.task_list_id != Uuid::nil()).then_some(existing.task_list_id)),
        title,
        description: optional_pending_text_property(properties, &[PID_TAG_BODY_W])
            .unwrap_or_else(|| existing.description.clone()),
        status,
        due_at,
        completed_at: existing.completed_at.clone(),
        recurrence_rule: existing.recurrence_rule.clone(),
        sort_order: existing.sort_order,
    }
}

fn reject_unsupported_mapi_task_properties(properties: &HashMap<u32, MapiValue>) -> Result<()> {
    for tag in properties.keys() {
        let supported = matches!(
            canonical_property_storage_tag(*tag),
            PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_DISPLAY_NAME_W
                | PID_TAG_BODY_W
                | PID_TAG_FLAG_STATUS
                | PID_TAG_END_DATE
        );
        if !supported {
            return Err(anyhow!(
                "MAPI task property {tag:#010X} is outside the canonical task subset"
            ));
        }
    }
    Ok(())
}

pub(in crate::mapi) fn event_input_from_mapi(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: &AccessibleEvent,
    properties: &HashMap<u32, MapiValue>,
) -> Result<UpsertClientEventInput> {
    reject_unsupported_mapi_event_properties(properties)?;
    let participants = event_participants_from_mapi(existing, properties);
    let recurrence = properties
        .get(&PID_LID_APPOINTMENT_RECUR_TAG)
        .and_then(|value| match value {
            MapiValue::Binary(value) => Some(appointment_recurrence_from_mapi(value)),
            _ => None,
        })
        .transpose()?;
    let start_filetime = properties
        .get(&PID_TAG_START_DATE)
        .or_else(|| properties.get(&PID_LID_APPOINTMENT_START_WHOLE_TAG))
        .or_else(|| properties.get(&PID_LID_COMMON_START_TAG))
        .and_then(MapiValue::as_i64);
    let end_filetime = properties
        .get(&PID_TAG_END_DATE)
        .or_else(|| properties.get(&PID_LID_APPOINTMENT_END_WHOLE_TAG))
        .or_else(|| properties.get(&PID_LID_COMMON_END_TAG))
        .and_then(MapiValue::as_i64)
        .or_else(|| {
            let start = start_filetime?;
            let duration = properties
                .get(&PID_LID_APPOINTMENT_DURATION_TAG)
                .and_then(MapiValue::as_i64)?;
            Some(start.saturating_add(duration.max(0).saturating_mul(600_000_000)))
        });
    let start = start_filetime
        .and_then(filetime_to_date_time)
        .unwrap_or_else(|| (existing.date.clone(), existing.time.clone()));
    let end = end_filetime.and_then(filetime_to_date_time);
    let duration_minutes = match (start_filetime, end_filetime) {
        (Some(start), Some(end)) if end >= start => {
            ((end - start) / 10_000_000 / 60).clamp(0, i64::from(i32::MAX)) as i32
        }
        _ => existing.duration_minutes,
    };
    let (date, time) = start;
    Ok(UpsertClientEventInput {
        id,
        account_id,
        uid: existing.uid.clone(),
        date,
        time,
        time_zone: optional_pending_text_property(
            properties,
            &[PID_LID_TIME_ZONE_DESCRIPTION_W_TAG],
        )
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| existing.time_zone.clone()),
        duration_minutes: end
            .map(|_| duration_minutes)
            .unwrap_or(existing.duration_minutes),
        all_day: properties
            .get(&PID_LID_APPOINTMENT_SUB_TYPE_TAG)
            .and_then(MapiValue::as_bool)
            .unwrap_or(existing.all_day),
        status: calendar_status_from_mapi(properties).unwrap_or_else(|| existing.status.clone()),
        sequence: existing.sequence,
        recurrence_rule: recurrence
            .as_ref()
            .map(|recurrence| recurrence.recurrence_rule.clone())
            .unwrap_or_else(|| existing.recurrence_rule.clone()),
        recurrence_json: recurrence
            .as_ref()
            .map(|recurrence| recurrence.recurrence_json.clone())
            .unwrap_or_else(|| existing.recurrence_json.clone()),
        recurrence_exceptions_json: recurrence
            .as_ref()
            .map(|recurrence| recurrence.recurrence_exceptions_json.clone())
            .unwrap_or_else(|| existing.recurrence_exceptions_json.clone()),
        title: optional_pending_text_property(
            properties,
            &[
                PID_TAG_SUBJECT_W,
                PID_TAG_NORMALIZED_SUBJECT_W,
                PID_TAG_DISPLAY_NAME_W,
            ],
        )
        .unwrap_or_else(|| existing.title.clone()),
        location: optional_pending_text_property(
            properties,
            &[PID_TAG_LOCATION_W, PID_LID_LOCATION_W_TAG],
        )
        .unwrap_or_else(|| existing.location.clone()),
        organizer_json: participants.organizer_json,
        attendees: participants.attendees,
        attendees_json: participants.attendees_json,
        notes: optional_pending_text_property(properties, &[PID_TAG_BODY_W])
            .unwrap_or_else(|| existing.notes.clone()),
        body_html: optional_pending_text_property(properties, &[PID_TAG_BODY_HTML_W])
            .unwrap_or_else(|| existing.body_html.clone()),
    })
}

pub(in crate::mapi) fn meeting_response_event_input_from_mapi(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: &AccessibleEvent,
    properties: &HashMap<u32, MapiValue>,
) -> Result<Option<UpsertClientEventInput>> {
    let Some(message_class) =
        optional_pending_text_property(properties, &[PID_TAG_MESSAGE_CLASS_W])
    else {
        return Ok(None);
    };
    let partstat = match message_class.trim().to_ascii_lowercase().as_str() {
        "ipm.schedule.meeting.resp.pos" => "accepted",
        "ipm.schedule.meeting.resp.tent" => "tentative",
        "ipm.schedule.meeting.resp.neg" => "declined",
        _ => return Ok(None),
    };
    for (tag, value) in properties {
        if matches!(value, MapiValue::Binary(_)) {
            return Err(anyhow!(
                "MAPI binary calendar recurrence or meeting payloads are not supported"
            ));
        }
        let supported = matches!(
            *tag,
            PID_TAG_MESSAGE_CLASS_W
                | PID_TAG_SENDER_NAME_W
                | PID_TAG_SENDER_EMAIL_ADDRESS_W
                | PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_DISPLAY_NAME_W
                | PID_TAG_BODY_W
        );
        if !supported {
            return Err(anyhow!(
                "MAPI meeting response property {tag:#010X} is outside the canonical calendar subset"
            ));
        }
    }
    let email = optional_pending_text_property(properties, &[PID_TAG_SENDER_EMAIL_ADDRESS_W])
        .map(|value| normalize_calendar_email(&value))
        .unwrap_or_default();
    let common_name = optional_pending_text_property(properties, &[PID_TAG_SENDER_NAME_W])
        .unwrap_or_default()
        .trim()
        .to_string();
    if email.is_empty() && common_name.is_empty() {
        bail!("MAPI meeting response requires sender identity");
    }

    let mut metadata = parse_calendar_participants_metadata(&existing.attendees_json);
    let mut matched = false;
    for attendee in &mut metadata.attendees {
        let email_matches = !email.is_empty()
            && normalize_calendar_email(&attendee.email).eq_ignore_ascii_case(&email);
        let name_matches = email.is_empty()
            && !common_name.is_empty()
            && attendee.common_name.eq_ignore_ascii_case(&common_name);
        if email_matches || name_matches {
            attendee.partstat = partstat.to_string();
            matched = true;
        }
    }
    if !matched {
        metadata.attendees.push(CalendarParticipantMetadata {
            email,
            common_name,
            role: "REQ-PARTICIPANT".to_string(),
            partstat: partstat.to_string(),
            rsvp: false,
        });
    }
    let attendees_json = serialize_calendar_participants_metadata(&metadata);
    let attendees = calendar_attendee_labels(&metadata);
    Ok(Some(UpsertClientEventInput {
        id,
        account_id,
        uid: existing.uid.clone(),
        date: existing.date.clone(),
        time: existing.time.clone(),
        time_zone: existing.time_zone.clone(),
        duration_minutes: existing.duration_minutes,
        all_day: existing.all_day,
        status: existing.status.clone(),
        sequence: existing.sequence,
        recurrence_rule: existing.recurrence_rule.clone(),
        recurrence_json: existing.recurrence_json.clone(),
        recurrence_exceptions_json: existing.recurrence_exceptions_json.clone(),
        title: existing.title.clone(),
        location: existing.location.clone(),
        organizer_json: existing.organizer_json.clone(),
        attendees,
        attendees_json,
        notes: existing.notes.clone(),
        body_html: existing.body_html.clone(),
    }))
}

struct MapiEventParticipants {
    organizer_json: String,
    attendees: String,
    attendees_json: String,
}

fn event_participants_from_mapi(
    existing: &AccessibleEvent,
    properties: &HashMap<u32, MapiValue>,
) -> MapiEventParticipants {
    let mut metadata = parse_calendar_participants_metadata(&existing.attendees_json);
    if let Some(organizer) = organizer_from_mapi(properties) {
        metadata.organizer = Some(organizer);
    }
    if let Some(attendees) = attendees_from_mapi(properties) {
        metadata.attendees = attendees;
    }
    let attendees_json = serialize_calendar_participants_metadata(&metadata);
    let organizer_json = metadata
        .organizer
        .as_ref()
        .and_then(|organizer| serde_json::to_string(organizer).ok())
        .unwrap_or_else(|| existing.organizer_json.clone());
    MapiEventParticipants {
        organizer_json,
        attendees: calendar_attendee_labels(&metadata),
        attendees_json,
    }
}

fn organizer_from_mapi(properties: &HashMap<u32, MapiValue>) -> Option<CalendarOrganizerMetadata> {
    let email = optional_pending_text_property(properties, &[PID_TAG_SENDER_EMAIL_ADDRESS_W])
        .map(|value| normalize_calendar_email(&value))
        .unwrap_or_default();
    let common_name = optional_pending_text_property(properties, &[PID_TAG_SENDER_NAME_W])
        .unwrap_or_default()
        .trim()
        .to_string();
    (!email.is_empty() || !common_name.is_empty())
        .then_some(CalendarOrganizerMetadata { email, common_name })
}

fn attendees_from_mapi(
    properties: &HashMap<u32, MapiValue>,
) -> Option<Vec<CalendarParticipantMetadata>> {
    let required = optional_pending_text_property(
        properties,
        &[
            PID_TAG_DISPLAY_TO_W,
            PID_LID_TO_ATTENDEES_STRING_W_TAG,
            PID_LID_ALL_ATTENDEES_STRING_W_TAG,
        ],
    );
    let optional = optional_pending_text_property(
        properties,
        &[PID_TAG_DISPLAY_CC_W, PID_LID_CC_ATTENDEES_STRING_W_TAG],
    );
    if required.is_none() && optional.is_none() {
        return None;
    }
    let mut attendees = Vec::new();
    attendees.extend(calendar_participants_from_display_string(
        required.as_deref().unwrap_or_default(),
        "REQ-PARTICIPANT",
    ));
    attendees.extend(calendar_participants_from_display_string(
        optional.as_deref().unwrap_or_default(),
        "OPT-PARTICIPANT",
    ));
    Some(attendees)
}

fn calendar_participants_from_display_string(
    value: &str,
    role: &str,
) -> Vec<CalendarParticipantMetadata> {
    value
        .split([',', ';'])
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| CalendarParticipantMetadata {
            email: if value.contains('@') {
                normalize_calendar_email(value)
            } else {
                String::new()
            },
            common_name: value.to_string(),
            role: role.to_string(),
            partstat: "needs-action".to_string(),
            rsvp: false,
        })
        .collect()
}

fn calendar_status_from_mapi_busy_status(value: i64) -> String {
    match value {
        0 => "cancelled",
        1 => "tentative",
        _ => "confirmed",
    }
    .to_string()
}

fn calendar_status_from_mapi(properties: &HashMap<u32, MapiValue>) -> Option<String> {
    let state_flags = properties
        .get(&PID_LID_APPOINTMENT_STATE_FLAGS_TAG)
        .and_then(MapiValue::as_i64);
    if state_flags.map(|flags| flags & 0x0000_0004 != 0) == Some(true) {
        return Some("cancelled".to_string());
    }
    properties
        .get(&PID_LID_BUSY_STATUS_TAG)
        .and_then(MapiValue::as_i64)
        .map(calendar_status_from_mapi_busy_status)
}

fn calendar_recurrence_blob(event: &AccessibleEvent) -> Option<Vec<u8>> {
    let recurrence = recurrence_pattern_from_canonical(event).ok()?;
    let mut value = Vec::new();
    value.extend_from_slice(&0x3004u16.to_le_bytes());
    value.extend_from_slice(&0x3004u16.to_le_bytes());
    value.extend_from_slice(&recurrence.frequency.to_le_bytes());
    value.extend_from_slice(&recurrence.pattern_type.to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
    value.extend_from_slice(&recurrence.first_date_time.to_le_bytes());
    value.extend_from_slice(&recurrence.period.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    for extra in &recurrence.pattern_extra {
        value.extend_from_slice(&extra.to_le_bytes());
    }
    value.extend_from_slice(&recurrence.end_type.to_le_bytes());
    value.extend_from_slice(&recurrence.count.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(
        &((recurrence.deleted_dates.len() + recurrence.modified_exceptions.len()) as u32)
            .to_le_bytes(),
    );
    for deleted in &recurrence.deleted_dates {
        value.extend_from_slice(&deleted.to_le_bytes());
    }
    for modified in &recurrence.modified_exceptions {
        value.extend_from_slice(&modified.original_start.to_le_bytes());
    }
    value.extend_from_slice(&(recurrence.modified_exceptions.len() as u32).to_le_bytes());
    for modified in &recurrence.modified_exceptions {
        value.extend_from_slice(&modified.original_start.to_le_bytes());
    }
    value.extend_from_slice(&recurrence_minutes_since_1601(&event.date).to_le_bytes());
    value.extend_from_slice(&recurrence.end_date.to_le_bytes());
    value.extend_from_slice(&0x0000_3006u32.to_le_bytes());
    value.extend_from_slice(&0x0000_3009u32.to_le_bytes());
    value.extend_from_slice(&event_start_minutes(event).to_le_bytes());
    value.extend_from_slice(&event_end_minutes(event).to_le_bytes());
    value.extend_from_slice(&(recurrence.modified_exceptions.len() as u16).to_le_bytes());
    for modified in &recurrence.modified_exceptions {
        value.extend_from_slice(&modified.start.to_le_bytes());
        value.extend_from_slice(&modified.end.to_le_bytes());
        value.extend_from_slice(&modified.original_start.to_le_bytes());
        let override_flags = recurrence_exception_override_flags(modified);
        value.extend_from_slice(&override_flags.to_le_bytes());
        if let Some(title) = modified.title.as_deref() {
            append_recur_ansi_string(&mut value, title);
        }
        if let Some(location) = modified.location.as_deref() {
            append_recur_ansi_string(&mut value, location);
        }
    }
    value.extend_from_slice(&0u32.to_le_bytes());
    for modified in &recurrence.modified_exceptions {
        let override_flags = recurrence_exception_override_flags(modified);
        value.extend_from_slice(&4u32.to_le_bytes());
        value.extend_from_slice(&0u32.to_le_bytes());
        value.extend_from_slice(&0u32.to_le_bytes());
        if override_flags != 0 {
            value.extend_from_slice(&modified.start.to_le_bytes());
            value.extend_from_slice(&modified.end.to_le_bytes());
            value.extend_from_slice(&modified.original_start.to_le_bytes());
            if let Some(title) = modified.title.as_deref() {
                append_recur_wide_string(&mut value, title);
            }
            if let Some(location) = modified.location.as_deref() {
                append_recur_wide_string(&mut value, location);
            }
            value.extend_from_slice(&0u32.to_le_bytes());
        }
    }
    value.extend_from_slice(&0u32.to_le_bytes());
    Some(value)
}

fn recurrence_exception_override_flags(exception: &CanonicalRecurrenceException) -> u16 {
    let mut flags = 0u16;
    if exception
        .title
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        flags |= 0x0001;
    }
    if exception
        .location
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        flags |= 0x0010;
    }
    flags
}

struct CanonicalRecurrencePattern {
    frequency: u16,
    pattern_type: u16,
    first_date_time: u32,
    period: u32,
    pattern_extra: Vec<u32>,
    end_type: u32,
    count: u32,
    end_date: u32,
    deleted_dates: Vec<u32>,
    modified_exceptions: Vec<CanonicalRecurrenceException>,
}

struct CanonicalRecurrenceException {
    original_start: u32,
    start: u32,
    end: u32,
    title: Option<String>,
    location: Option<String>,
}

fn recurrence_pattern_from_canonical(
    event: &AccessibleEvent,
) -> Result<CanonicalRecurrencePattern> {
    let parts = parse_canonical_recurrence_rule(&event.recurrence_rule);
    let frequency = recurrence_rule_value(&parts, "FREQ").unwrap_or_default();
    let interval = recurrence_rule_value(&parts, "INTERVAL")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1)
        .max(1);
    let by_day = recurrence_rule_value(&parts, "BYDAY").unwrap_or_default();
    let by_month =
        recurrence_rule_value(&parts, "BYMONTH").and_then(|value| value.parse::<u32>().ok());
    let by_month_day =
        recurrence_rule_value(&parts, "BYMONTHDAY").and_then(|value| value.parse::<u32>().ok());
    let by_set_pos =
        recurrence_rule_value(&parts, "BYSETPOS").and_then(|value| value.parse::<i32>().ok());
    let (frequency, pattern_type, period, pattern_extra) = match frequency.as_str() {
        "DAILY" => (
            0x200Au16,
            0x0000u16,
            interval.saturating_mul(1440),
            Vec::new(),
        ),
        "WEEKLY" => (
            0x200Bu16,
            0x0001u16,
            interval,
            vec![recurrence_day_mask(&by_day)?],
        ),
        "MONTHLY" if by_month_day == Some(31) => (0x200Cu16, 0x0004u16, interval, vec![31]),
        "MONTHLY" if by_month_day.is_some() => {
            (0x200Cu16, 0x0002u16, interval, vec![by_month_day.unwrap()])
        }
        "MONTHLY" if !by_day.is_empty() && by_set_pos.is_some() => (
            0x200Cu16,
            0x0003u16,
            interval,
            vec![
                recurrence_day_mask(&by_day)?,
                recurrence_set_position_to_mapi(by_set_pos.unwrap())?,
            ],
        ),
        "YEARLY" if by_month_day.is_some() => {
            (0x200Du16, 0x0002u16, 12, vec![by_month_day.unwrap()])
        }
        "YEARLY" if !by_day.is_empty() && by_set_pos.is_some() => (
            0x200Du16,
            0x0003u16,
            12,
            vec![
                recurrence_day_mask(&by_day)?,
                recurrence_set_position_to_mapi(by_set_pos.unwrap())?,
            ],
        ),
        _ => bail!("unsupported canonical recurrence rule"),
    };
    if period == 0 {
        bail!("unsupported canonical recurrence interval");
    }
    let (end_type, count, end_date) = if let Some(count) =
        recurrence_rule_value(&parts, "COUNT").and_then(|value| value.parse::<u32>().ok())
    {
        (
            0x0000_2022,
            count,
            recurrence_minutes_since_1601(&event.date),
        )
    } else if let Some(until) = recurrence_rule_value(&parts, "UNTIL") {
        (
            0x0000_2021,
            0,
            recurrence_minutes_since_1601(&until_date(&until)),
        )
    } else {
        (0x0000_2023, 0, recurrence_minutes_since_1601(&event.date))
    };
    if end_type == 0x0000_2022 && count == 0 {
        bail!("unsupported canonical recurrence count");
    }
    Ok(CanonicalRecurrencePattern {
        frequency,
        pattern_type,
        first_date_time: recurrence_first_date_minutes(event, by_month, by_month_day),
        period,
        pattern_extra,
        end_type,
        count,
        end_date,
        deleted_dates: recurrence_deleted_dates_from_json(&event.recurrence_exceptions_json),
        modified_exceptions: recurrence_modified_exceptions_from_json(
            &event.recurrence_exceptions_json,
        ),
    })
}

fn parse_canonical_recurrence_rule(rule: &str) -> Vec<(String, String)> {
    rule.split(';')
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            Some((
                key.trim().to_ascii_uppercase(),
                value.trim().to_ascii_uppercase(),
            ))
        })
        .collect()
}

fn recurrence_rule_value(parts: &[(String, String)], key: &str) -> Option<String> {
    parts
        .iter()
        .find_map(|(candidate, value)| (candidate == key).then_some(value.clone()))
}

fn recurrence_first_date_minutes(
    event: &AccessibleEvent,
    by_month: Option<u32>,
    by_month_day: Option<u32>,
) -> u32 {
    if let Some(month) = by_month {
        let day = by_month_day.or_else(|| {
            event
                .date
                .get(8..10)
                .and_then(|value| value.parse::<u32>().ok())
        });
        let year = event
            .date
            .get(0..4)
            .and_then(|value| value.parse::<i32>().ok());
        if (1..=12).contains(&month)
            && day.is_some_and(|day| (1..=31).contains(&day))
            && year.is_some()
        {
            return recurrence_minutes_since_1601(&format!(
                "{:04}-{month:02}-{:02}",
                year.unwrap(),
                day.unwrap()
            ));
        }
    }
    recurrence_minutes_since_1601(&event.date)
}

fn recurrence_day_mask(value: &str) -> Result<u32> {
    let mut mask = 0u32;
    for day in value
        .split(',')
        .map(str::trim)
        .filter(|day| !day.is_empty())
    {
        mask |= match day {
            "SU" => 0x01,
            "MO" => 0x02,
            "TU" => 0x04,
            "WE" => 0x08,
            "TH" => 0x10,
            "FR" => 0x20,
            "SA" => 0x40,
            _ => bail!("unsupported canonical recurrence day"),
        };
    }
    if mask == 0 {
        bail!("unsupported canonical recurrence day");
    }
    Ok(mask)
}

fn recurrence_set_position_to_mapi(value: i32) -> Result<u32> {
    match value {
        1..=4 => Ok(value as u32),
        -1 => Ok(5),
        _ => bail!("unsupported canonical recurrence set position"),
    }
}

fn recurrence_deleted_dates_from_json(value: &str) -> Vec<u32> {
    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default()
        .into_iter()
        .filter(|value| value.get("excluded").and_then(|value| value.as_bool()) == Some(true))
        .filter_map(|value| {
            value
                .get("recurrenceId")
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
        .map(|date| recurrence_minutes_since_1601(&date))
        .collect()
}

fn recurrence_modified_exceptions_from_json(value: &str) -> Vec<CanonicalRecurrenceException> {
    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default()
        .into_iter()
        .filter(|value| value.get("excluded").and_then(|value| value.as_bool()) != Some(true))
        .filter_map(|value| {
            let recurrence_id = value.get("recurrenceId")?.as_str()?;
            let start = value.get("start")?.as_str()?;
            let end = value.get("end")?.as_str()?;
            Some(CanonicalRecurrenceException {
                original_start: recurrence_minutes_since_1601(recurrence_id),
                start: recurrence_datetime_minutes_since_1601(start)?,
                end: recurrence_datetime_minutes_since_1601(end)?,
                title: value
                    .get("title")
                    .and_then(|value| value.as_str())
                    .map(str::to_string),
                location: value
                    .get("location")
                    .and_then(|value| value.as_str())
                    .map(str::to_string),
            })
        })
        .filter(|exception| exception.start < exception.end)
        .collect()
}

fn until_date(value: &str) -> String {
    if value.len() >= 8 && value.as_bytes()[0..8].iter().all(u8::is_ascii_digit) {
        format!("{}-{}-{}", &value[0..4], &value[4..6], &value[6..8])
    } else {
        value.get(0..10).unwrap_or(value).to_string()
    }
}

fn event_start_minutes(event: &AccessibleEvent) -> u32 {
    time_to_minutes(&event.time)
}

fn event_end_minutes(event: &AccessibleEvent) -> u32 {
    event_start_minutes(event)
        .saturating_add(event.duration_minutes.max(1) as u32)
        .min(24 * 60)
}

fn time_to_minutes(time: &str) -> u32 {
    let hour = time
        .get(0..2)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0)
        .min(23);
    let minute = time
        .get(3..5)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0)
        .min(59);
    hour * 60 + minute
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MapiAppointmentRecurrence {
    recurrence_rule: String,
    recurrence_json: String,
    recurrence_exceptions_json: String,
}

struct MapiRecurrenceException {
    original_start: u32,
    start: u32,
    end: u32,
    title: Option<String>,
    location: Option<String>,
    override_flags: u16,
}

fn appointment_recurrence_from_mapi(value: &[u8]) -> Result<MapiAppointmentRecurrence> {
    let mut offset = 0usize;
    let reader_version = read_recur_u16(value, &mut offset)?;
    let writer_version = read_recur_u16(value, &mut offset)?;
    if reader_version != 0x3004 || writer_version != 0x3004 {
        bail!("unsupported MAPI calendar recurrence version");
    }
    let frequency = read_recur_u16(value, &mut offset)?;
    let pattern_type = read_recur_u16(value, &mut offset)?;
    let calendar_type = read_recur_u16(value, &mut offset)?;
    if calendar_type != 0 {
        bail!("unsupported MAPI calendar recurrence calendar type");
    }
    let first_date_time = read_recur_u32(value, &mut offset)?;
    let period = read_recur_u32(value, &mut offset)?;
    if period == 0 {
        bail!("unsupported MAPI calendar recurrence interval");
    }
    let sliding_flag = read_recur_u32(value, &mut offset)?;
    if sliding_flag != 0 {
        bail!("unsupported MAPI calendar recurrence sliding flag");
    }

    let pattern = read_recur_pattern(
        value,
        &mut offset,
        frequency,
        pattern_type,
        period,
        first_date_time,
    )?;
    let end_type = read_recur_u32(value, &mut offset)?;
    let occurrence_count = read_recur_u32(value, &mut offset)?;
    let _first_dow = read_recur_u32(value, &mut offset)?;
    let deleted = read_recur_dates(value, &mut offset)?;
    let modified = read_recur_dates(value, &mut offset)?;
    if modified.len() > deleted.len() {
        bail!("unsupported MAPI calendar recurrence modified instance list");
    }
    let _start_date = read_recur_u32(value, &mut offset)?;
    let end_date = read_recur_u32(value, &mut offset)?;
    let reader_version2 = read_recur_u32(value, &mut offset)?;
    let writer_version2 = read_recur_u32(value, &mut offset)?;
    if reader_version2 != 0x0000_3006 || !matches!(writer_version2, 0x0000_3008 | 0x0000_3009) {
        bail!("unsupported MAPI appointment recurrence version");
    }
    let _start_time_offset = read_recur_u32(value, &mut offset)?;
    let _end_time_offset = read_recur_u32(value, &mut offset)?;
    let exception_count = read_recur_u16(value, &mut offset)?;
    if usize::from(exception_count) != modified.len() {
        bail!("unsupported MAPI calendar recurrence exception payload");
    }
    let exceptions = read_recur_exception_infos(value, &mut offset, usize::from(exception_count))?;
    let reserved_block1_size = read_recur_u32(value, &mut offset)?;
    if reserved_block1_size != 0 {
        bail!("unsupported MAPI calendar recurrence reserved block");
    }
    read_recur_extended_exceptions(value, &mut offset, writer_version2, &exceptions)?;
    let reserved_block2_size = read_recur_u32(value, &mut offset)?;
    if reserved_block2_size != 0 {
        bail!("unsupported MAPI calendar recurrence reserved block");
    }

    let mut rule_parts = vec![format!("FREQ={}", pattern.frequency)];
    let mut json_parts = vec![format!(
        "\"frequency\":\"{}\"",
        pattern.frequency.to_ascii_lowercase()
    )];
    if pattern.interval != 1 {
        rule_parts.push(format!("INTERVAL={}", pattern.interval));
        json_parts.push(format!("\"interval\":{}", pattern.interval));
    }
    match end_type {
        0x0000_2022 => {
            if occurrence_count == 0 {
                bail!("unsupported MAPI calendar recurrence count");
            }
            rule_parts.push(format!("COUNT={occurrence_count}"));
            json_parts.push(format!("\"count\":{occurrence_count}"));
        }
        0x0000_2021 => {
            let until = recurrence_date_yyyymmdd(end_date)?;
            rule_parts.push(format!("UNTIL={until}"));
            json_parts.push(format!("\"until\":\"{until}\""));
        }
        0x0000_2023 | 0xFFFF_FFFF => {}
        _ => bail!("unsupported MAPI calendar recurrence end type"),
    }
    if !pattern.by_day.is_empty() {
        rule_parts.push(format!("BYDAY={}", pattern.by_day.join(",")));
        json_parts.push(format!(
            "\"byDay\":[{}]",
            pattern
                .by_day
                .iter()
                .map(|day| format!("\"{day}\""))
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(day) = pattern.by_month_day {
        rule_parts.push(format!("BYMONTHDAY={day}"));
        json_parts.push(format!("\"byMonthDay\":{day}"));
    }
    if let Some(month) = pattern.by_month {
        rule_parts.push(format!("BYMONTH={month}"));
        json_parts.push(format!("\"byMonth\":{month}"));
    }
    if let Some(position) = pattern.by_set_pos {
        rule_parts.push(format!("BYSETPOS={position}"));
        json_parts.push(format!("\"bySetPosition\":{position}"));
    }

    let modified_set = modified.iter().copied().collect::<HashSet<_>>();
    let mut overrides = deleted
        .into_iter()
        .filter(|date| !modified_set.contains(date))
        .map(|date| {
            recurrence_date_string(date)
                .map(|date| format!(r#"{{"recurrenceId":"{date}","excluded":true}}"#))
        })
        .collect::<Result<Vec<_>>>()?;
    for exception in exceptions {
        let recurrence_id = recurrence_date_string(exception.original_start)?;
        let start = recurrence_datetime_string(exception.start)?;
        let end = recurrence_datetime_string(exception.end)?;
        let mut override_value = serde_json::json!({
            "recurrenceId": recurrence_id,
            "start": start,
            "end": end,
        });
        if let Some(title) = exception.title {
            override_value["title"] = serde_json::Value::String(title);
        }
        if let Some(location) = exception.location {
            override_value["location"] = serde_json::Value::String(location);
        }
        overrides.push(override_value.to_string());
    }

    Ok(MapiAppointmentRecurrence {
        recurrence_rule: rule_parts.join(";"),
        recurrence_json: format!("{{{}}}", json_parts.join(",")),
        recurrence_exceptions_json: format!("[{}]", overrides.join(",")),
    })
}

struct MapiRecurPattern {
    frequency: &'static str,
    interval: u32,
    by_day: Vec<&'static str>,
    by_month: Option<u32>,
    by_month_day: Option<u32>,
    by_set_pos: Option<i32>,
}

fn read_recur_pattern(
    value: &[u8],
    offset: &mut usize,
    frequency: u16,
    pattern_type: u16,
    period: u32,
    first_date_time: u32,
) -> Result<MapiRecurPattern> {
    match (frequency, pattern_type) {
        (0x200A, 0x0000) => Ok(MapiRecurPattern {
            frequency: "DAILY",
            interval: (period / 1440).max(1),
            by_day: Vec::new(),
            by_month: None,
            by_month_day: None,
            by_set_pos: None,
        }),
        (0x200B, 0x0001) => {
            let mask = read_recur_u32(value, offset)?;
            Ok(MapiRecurPattern {
                frequency: "WEEKLY",
                interval: period,
                by_day: recurrence_days_from_mask(mask)?,
                by_month: None,
                by_month_day: None,
                by_set_pos: None,
            })
        }
        (0x200C, 0x0002) => {
            let day = read_recur_u32(value, offset)?;
            if !(1..=31).contains(&day) {
                bail!("unsupported MAPI monthly recurrence day");
            }
            Ok(MapiRecurPattern {
                frequency: "MONTHLY",
                interval: period,
                by_day: Vec::new(),
                by_month: None,
                by_month_day: Some(day),
                by_set_pos: None,
            })
        }
        (0x200C, 0x0004) => {
            let day = read_recur_u32(value, offset)?;
            if day != 31 {
                bail!("unsupported MAPI month-end recurrence day");
            }
            Ok(MapiRecurPattern {
                frequency: "MONTHLY",
                interval: period,
                by_day: Vec::new(),
                by_month: None,
                by_month_day: Some(31),
                by_set_pos: None,
            })
        }
        (0x200C, 0x0003) | (0x200D, 0x0003) => {
            let mask = read_recur_u32(value, offset)?;
            let n = read_recur_u32(value, offset)?;
            let set_pos = match n {
                1..=4 => n as i32,
                5 => -1,
                _ => bail!("unsupported MAPI monthly nth recurrence position"),
            };
            Ok(MapiRecurPattern {
                frequency: if frequency == 0x200D {
                    "YEARLY"
                } else {
                    "MONTHLY"
                },
                interval: if frequency == 0x200D { 1 } else { period },
                by_day: recurrence_days_from_mask(mask)?,
                by_month: (frequency == 0x200D)
                    .then(|| recurrence_month_from_minutes(first_date_time))
                    .transpose()?,
                by_month_day: None,
                by_set_pos: Some(set_pos),
            })
        }
        (0x200D, 0x0002) => {
            let day = read_recur_u32(value, offset)?;
            if period != 12 || !(1..=31).contains(&day) {
                bail!("unsupported MAPI yearly recurrence");
            }
            Ok(MapiRecurPattern {
                frequency: "YEARLY",
                interval: 1,
                by_day: Vec::new(),
                by_month: Some(recurrence_month_from_minutes(first_date_time)?),
                by_month_day: Some(day),
                by_set_pos: None,
            })
        }
        _ => bail!("unsupported MAPI calendar recurrence pattern"),
    }
}

fn recurrence_days_from_mask(mask: u32) -> Result<Vec<&'static str>> {
    let days = [
        (0x01, "SU"),
        (0x02, "MO"),
        (0x04, "TU"),
        (0x08, "WE"),
        (0x10, "TH"),
        (0x20, "FR"),
        (0x40, "SA"),
    ]
    .into_iter()
    .filter_map(|(bit, day)| (mask & bit != 0).then_some(day))
    .collect::<Vec<_>>();
    if days.is_empty() || mask & !0x7F != 0 {
        bail!("unsupported MAPI recurrence day mask");
    }
    Ok(days)
}

fn read_recur_dates(value: &[u8], offset: &mut usize) -> Result<Vec<u32>> {
    let count = read_recur_u32(value, offset)? as usize;
    let mut dates = Vec::with_capacity(count);
    for _ in 0..count {
        dates.push(read_recur_u32(value, offset)?);
    }
    Ok(dates)
}

fn read_recur_exception_infos(
    value: &[u8],
    offset: &mut usize,
    count: usize,
) -> Result<Vec<MapiRecurrenceException>> {
    let mut exceptions = Vec::with_capacity(count);
    for _ in 0..count {
        let start = read_recur_u32(value, offset)?;
        let end = read_recur_u32(value, offset)?;
        let original_start = read_recur_u32(value, offset)?;
        let override_flags = read_recur_u16(value, offset)?;
        if start >= end {
            bail!("unsupported MAPI calendar recurrence exception time range");
        }
        if override_flags & !0x0011 != 0 {
            bail!("unsupported MAPI calendar recurrence exception override");
        }
        let title = if override_flags & 0x0001 != 0 {
            Some(read_recur_ansi_string(value, offset)?)
        } else {
            None
        };
        let location = if override_flags & 0x0010 != 0 {
            Some(read_recur_ansi_string(value, offset)?)
        } else {
            None
        };
        exceptions.push(MapiRecurrenceException {
            original_start,
            start,
            end,
            title,
            location,
            override_flags,
        });
    }
    Ok(exceptions)
}

fn read_recur_extended_exceptions(
    value: &[u8],
    offset: &mut usize,
    writer_version2: u32,
    exceptions: &[MapiRecurrenceException],
) -> Result<()> {
    if writer_version2 < 0x0000_3009 {
        return Ok(());
    }
    for exception in exceptions {
        let change_highlight_size = read_recur_u32(value, offset)? as usize;
        skip_recur_bytes(value, offset, change_highlight_size)?;
        let reserved_block_ee1_size = read_recur_u32(value, offset)?;
        if reserved_block_ee1_size != 0 {
            bail!("unsupported MAPI calendar recurrence extended exception reserved block");
        }
        if exception.override_flags & 0x0011 != 0 {
            let extended_start = read_recur_u32(value, offset)?;
            let extended_end = read_recur_u32(value, offset)?;
            let extended_original = read_recur_u32(value, offset)?;
            if extended_start != exception.start
                || extended_end != exception.end
                || extended_original != exception.original_start
            {
                bail!("unsupported MAPI calendar recurrence extended exception mismatch");
            }
            if exception.override_flags & 0x0001 != 0 {
                let _ = read_recur_wide_string(value, offset)?;
            }
            if exception.override_flags & 0x0010 != 0 {
                let _ = read_recur_wide_string(value, offset)?;
            }
            let reserved_block_ee2_size = read_recur_u32(value, offset)?;
            if reserved_block_ee2_size != 0 {
                bail!("unsupported MAPI calendar recurrence extended exception reserved block");
            }
        }
    }
    Ok(())
}

fn read_recur_ansi_string(value: &[u8], offset: &mut usize) -> Result<String> {
    let length_with_nul = read_recur_u16(value, offset)?;
    let length = read_recur_u16(value, offset)? as usize;
    if usize::from(length_with_nul) < length {
        bail!("unsupported MAPI calendar recurrence exception string length");
    }
    let bytes = value
        .get(*offset..offset.saturating_add(length))
        .ok_or_else(|| anyhow!("truncated MAPI calendar recurrence"))?;
    *offset += length;
    Ok(String::from_utf8_lossy(bytes).into_owned())
}

fn read_recur_wide_string(value: &[u8], offset: &mut usize) -> Result<String> {
    let length = read_recur_u16(value, offset)? as usize;
    let byte_len = length.saturating_mul(2);
    let bytes = value
        .get(*offset..offset.saturating_add(byte_len))
        .ok_or_else(|| anyhow!("truncated MAPI calendar recurrence"))?;
    *offset += byte_len;
    let units = bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .collect::<Vec<_>>();
    Ok(String::from_utf16_lossy(&units))
}

fn append_recur_ansi_string(value: &mut Vec<u8>, text: &str) {
    let bytes = text.as_bytes();
    value.extend_from_slice(&((bytes.len() + 1) as u16).to_le_bytes());
    value.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    value.extend_from_slice(bytes);
}

fn append_recur_wide_string(value: &mut Vec<u8>, text: &str) {
    let units = text.encode_utf16().collect::<Vec<_>>();
    value.extend_from_slice(&(units.len() as u16).to_le_bytes());
    for unit in units {
        value.extend_from_slice(&unit.to_le_bytes());
    }
}

fn skip_recur_bytes(value: &[u8], offset: &mut usize, len: usize) -> Result<()> {
    value
        .get(*offset..offset.saturating_add(len))
        .ok_or_else(|| anyhow!("truncated MAPI calendar recurrence"))?;
    *offset += len;
    Ok(())
}

fn read_recur_u16(value: &[u8], offset: &mut usize) -> Result<u16> {
    let bytes = value
        .get(*offset..offset.saturating_add(2))
        .ok_or_else(|| anyhow!("truncated MAPI calendar recurrence"))?;
    *offset += 2;
    Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_recur_u32(value: &[u8], offset: &mut usize) -> Result<u32> {
    let bytes = value
        .get(*offset..offset.saturating_add(4))
        .ok_or_else(|| anyhow!("truncated MAPI calendar recurrence"))?;
    *offset += 4;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn recurrence_minutes_since_1601(date: &str) -> u32 {
    let year = date
        .get(0..4)
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(1970);
    let month = date
        .get(5..7)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    let day = date
        .get(8..10)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    let days = days_from_civil(i64::from(year), i64::from(month), i64::from(day))
        - days_from_civil(1601, 1, 1);
    days.max(0).saturating_mul(1440).min(i64::from(u32::MAX)) as u32
}

fn recurrence_datetime_minutes_since_1601(value: &str) -> Option<u32> {
    let date_minutes = recurrence_minutes_since_1601(value);
    let hour = value.get(11..13)?.parse::<u32>().ok()?.min(23);
    let minute = value.get(14..16)?.parse::<u32>().ok()?.min(59);
    Some(date_minutes.saturating_add(hour * 60 + minute))
}

fn recurrence_date_yyyymmdd(minutes_since_1601: u32) -> Result<String> {
    let date = recurrence_date_string(minutes_since_1601)?;
    Ok(date.replace('-', ""))
}

fn recurrence_date_string(minutes_since_1601: u32) -> Result<String> {
    let unix_days =
        days_from_civil(1601, 1, 1).saturating_add(i64::from(minutes_since_1601 / 1440));
    let (year, month, day) = civil_from_days(unix_days);
    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

fn recurrence_month_from_minutes(minutes_since_1601: u32) -> Result<u32> {
    let unix_days =
        days_from_civil(1601, 1, 1).saturating_add(i64::from(minutes_since_1601 / 1440));
    let (_, month, _) = civil_from_days(unix_days);
    if (1..=12).contains(&month) {
        Ok(month as u32)
    } else {
        bail!("unsupported MAPI yearly recurrence month")
    }
}

fn recurrence_datetime_string(minutes_since_1601: u32) -> Result<String> {
    let date = recurrence_date_string(minutes_since_1601)?;
    let minutes = minutes_since_1601 % 1440;
    Ok(format!("{date}T{:02}:{:02}:00", minutes / 60, minutes % 60))
}

pub(in crate::mapi) fn reject_unsupported_mapi_event_properties(
    properties: &HashMap<u32, MapiValue>,
) -> Result<()> {
    reject_unsupported_calendar_message_class(properties)?;
    for (tag, value) in properties {
        if matches!(value, MapiValue::Binary(_)) && *tag != PID_LID_APPOINTMENT_RECUR_TAG {
            return Err(anyhow!(
                "MAPI binary calendar recurrence or meeting payloads are not supported"
            ));
        }
        let supported = matches!(
            *tag,
            PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_DISPLAY_NAME_W
                | PID_TAG_BODY_W
                | PID_TAG_SENDER_NAME_W
                | PID_TAG_SENDER_EMAIL_ADDRESS_W
                | PID_TAG_DISPLAY_TO_W
                | PID_TAG_DISPLAY_CC_W
                | PID_TAG_CREATION_TIME
                | PID_TAG_LAST_MODIFICATION_TIME
                | PID_TAG_START_DATE
                | PID_TAG_END_DATE
                | PID_LID_COMMON_START_TAG
                | PID_LID_COMMON_END_TAG
                | PID_TAG_LOCATION_W
                | PID_LID_LOCATION_W_TAG
                | PID_TAG_BODY_HTML_W
                | PID_LID_BUSY_STATUS_TAG
                | PID_LID_APPOINTMENT_STATE_FLAGS_TAG
                | PID_LID_APPOINTMENT_START_WHOLE_TAG
                | PID_LID_APPOINTMENT_END_WHOLE_TAG
                | PID_LID_APPOINTMENT_DURATION_TAG
                | PID_LID_APPOINTMENT_SUB_TYPE_TAG
                | PID_LID_APPOINTMENT_RECUR_TAG
                | PID_LID_ALL_ATTENDEES_STRING_W_TAG
                | PID_LID_TO_ATTENDEES_STRING_W_TAG
                | PID_LID_CC_ATTENDEES_STRING_W_TAG
                | PID_LID_TIME_ZONE_DESCRIPTION_W_TAG
                | PID_TAG_MESSAGE_CLASS_W
        );
        if !supported {
            return Err(anyhow!(
                "MAPI calendar property {tag:#010X} is outside the canonical calendar subset"
            ));
        }
        if *tag == PID_LID_APPOINTMENT_STATE_FLAGS_TAG {
            let flags = value
                .as_i64()
                .ok_or_else(|| anyhow!("invalid MAPI appointment state flags value"))?;
            if flags < 0 || flags & !0x0000_0005 != 0 {
                return Err(anyhow!(
                    "unsupported MAPI appointment state flags {flags:#010X}"
                ));
            }
        }
    }
    Ok(())
}

pub(in crate::mapi) fn bounded_meeting_cancellation_from_mapi(
    properties: &HashMap<u32, MapiValue>,
) -> Result<bool> {
    let Some(message_class) =
        optional_pending_text_property(properties, &[PID_TAG_MESSAGE_CLASS_W])
    else {
        return Ok(false);
    };
    if !message_class
        .trim()
        .eq_ignore_ascii_case("IPM.Schedule.Meeting.Canceled")
    {
        return Ok(false);
    }
    for (tag, value) in properties {
        if matches!(value, MapiValue::Binary(_)) {
            return Err(anyhow!(
                "MAPI binary calendar recurrence or meeting payloads are not supported"
            ));
        }
        let supported = matches!(
            *tag,
            PID_TAG_MESSAGE_CLASS_W
                | PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_DISPLAY_NAME_W
                | PID_TAG_BODY_W
                | PID_TAG_START_DATE
                | PID_TAG_END_DATE
        );
        if !supported {
            return Err(anyhow!(
                "MAPI calendar cancellation property {tag:#010X} is outside the canonical calendar subset"
            ));
        }
    }
    Ok(true)
}

fn reject_unsupported_calendar_message_class(properties: &HashMap<u32, MapiValue>) -> Result<()> {
    let Some(message_class) =
        optional_pending_text_property(properties, &[PID_TAG_MESSAGE_CLASS_W])
    else {
        return Ok(());
    };
    let message_class = message_class.trim();
    if message_class.is_empty()
        || message_class.eq_ignore_ascii_case("IPM.Appointment")
        || message_class.eq_ignore_ascii_case("IPM.Schedule.Meeting.Request")
    {
        return Ok(());
    }
    Err(anyhow!(
        "MAPI calendar message class {message_class} is not mapped to canonical calendar state"
    ))
}

pub(in crate::mapi) fn pending_attachment_upload(
    attach_num: u32,
    properties: &HashMap<u32, MapiValue>,
    data: Vec<u8>,
) -> AttachmentUploadInput {
    let content_id = optional_pending_text_property(properties, &[PID_TAG_ATTACH_CONTENT_ID_W])
        .map(|value| value.trim().trim_matches(['<', '>']).to_string())
        .filter(|value| !value.is_empty());
    let hidden = properties
        .get(&PID_TAG_ATTACHMENT_HIDDEN)
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    AttachmentUploadInput {
        file_name: pending_attachment_file_name(attach_num, properties),
        media_type: pending_attachment_media_type(properties),
        disposition: Some(
            if content_id.is_some() || hidden {
                "inline"
            } else {
                "attachment"
            }
            .to_string(),
        ),
        content_id,
        blob_bytes: data,
    }
}

pub(in crate::mapi) fn pending_attachment_file_name(
    attach_num: u32,
    properties: &HashMap<u32, MapiValue>,
) -> String {
    optional_pending_text_property(
        properties,
        &[PID_TAG_ATTACH_LONG_FILENAME_W, PID_TAG_ATTACH_FILENAME_W],
    )
    .unwrap_or_else(|| format!("mapi-attachment-{attach_num}.bin"))
}

pub(in crate::mapi) fn pending_attachment_media_type(
    properties: &HashMap<u32, MapiValue>,
) -> String {
    optional_pending_text_property(properties, &[PID_TAG_ATTACH_MIME_TAG_W])
        .unwrap_or_else(|| "application/octet-stream".to_string())
}

pub(in crate::mapi) fn mapi_expected_attachment_kind(
    media_type: &str,
    file_name: &str,
) -> ExpectedKind {
    let media_type = media_type.trim().to_ascii_lowercase();
    let file_name = file_name.trim().to_ascii_lowercase();
    if matches!(
        media_type.as_str(),
        "application/pdf"
            | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            | "application/vnd.oasis.opendocument.text"
    ) || file_name.ends_with(".pdf")
        || file_name.ends_with(".docx")
        || file_name.ends_with(".odt")
    {
        ExpectedKind::SupportedAttachmentText
    } else {
        ExpectedKind::Any
    }
}

pub(in crate::mapi) fn jmap_import_from_pending_message(
    principal: &AccountPrincipal,
    mailbox: &JmapMailbox,
    properties: &HashMap<u32, MapiValue>,
    recipients: &[PendingRecipient],
    attachments: Vec<AttachmentUploadInput>,
) -> JmapImportedEmailInput {
    let subject = pending_text_property(
        properties,
        &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
    );
    let body_text = pending_body_text_property(properties);
    let from_address =
        optional_pending_text_property(properties, &[PID_TAG_SENDER_EMAIL_ADDRESS_W])
            .unwrap_or_else(|| principal.email.clone());
    let from_display = optional_pending_text_property(properties, &[PID_TAG_SENDER_NAME_W])
        .or_else(|| Some(principal.display_name.clone()));
    let internet_message_id =
        optional_pending_text_property(properties, &[PID_TAG_INTERNET_MESSAGE_ID_W]);
    let thread_id = match properties.get(&PID_TAG_CONVERSATION_INDEX) {
        Some(MapiValue::Binary(value)) => conversation_id_from_index(value),
        _ => None,
    };
    let size_octets = subject
        .len()
        .saturating_add(body_text.len())
        .min(i64::MAX as usize) as i64;
    let (to, cc, bcc) = pending_recipients_for_import(recipients);

    JmapImportedEmailInput {
        account_id: principal.account_id,
        submitted_by_account_id: principal.account_id,
        mailbox_id: mailbox.id,
        source: "mapi-save-message".to_string(),
        raw_message: None,
        from_display,
        from_address,
        sender_display: None,
        sender_address: None,
        to,
        cc,
        bcc,
        subject,
        body_text,
        body_html_sanitized: pending_html_property(properties),
        internet_message_id,
        mime_blob_ref: format!("mapi-save-message:{}", Uuid::new_v4()),
        size_octets,
        received_at: None,
        thread_id,
        attachments,
    }
}

pub(in crate::mapi) fn pending_recipients_for_import(
    recipients: &[PendingRecipient],
) -> (
    Vec<SubmittedRecipientInput>,
    Vec<SubmittedRecipientInput>,
    Vec<SubmittedRecipientInput>,
) {
    let mut to = Vec::new();
    let mut cc = Vec::new();
    let mut bcc = Vec::new();
    for recipient in recipients {
        let input = SubmittedRecipientInput {
            address: recipient.address.clone(),
            display_name: recipient.display_name.clone(),
        };
        match recipient.recipient_type {
            0x02 => cc.push(input),
            0x03 => bcc.push(input),
            _ => to.push(input),
        }
    }
    (to, cc, bcc)
}

pub(in crate::mapi) fn mapi_submit_from_pending_message(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    recipients: &[PendingRecipient],
) -> SubmitMessageInput {
    let subject = pending_text_property(
        properties,
        &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
    );
    let body_text = pending_body_text_property(properties);
    let from_address =
        optional_pending_submit_address(properties, &[PID_TAG_SENDER_EMAIL_ADDRESS_W])
            .unwrap_or_else(|| principal.email.clone());
    let from_display = optional_pending_text_property(properties, &[PID_TAG_SENDER_NAME_W])
        .or_else(|| Some(principal.display_name.clone()));
    let internet_message_id =
        optional_pending_text_property(properties, &[PID_TAG_INTERNET_MESSAGE_ID_W]);
    let (to, cc, bcc) = pending_recipients_for_import(recipients);

    SubmitMessageInput {
        draft_message_id: None,
        account_id: principal.account_id,
        submitted_by_account_id: principal.account_id,
        source: "mapi-submit-message".to_string(),
        from_display,
        from_address,
        sender_display: None,
        sender_address: None,
        to,
        cc,
        bcc,
        subject,
        body_text,
        body_html_sanitized: pending_html_property(properties),
        internet_message_id,
        mime_blob_ref: Some(format!("mapi-submit-message:{}", Uuid::new_v4())),
        size_octets: pending_message_size(properties),
        unread: Some(false),
        flagged: Some(false),
        attachments: Vec::new(),
    }
}

fn optional_pending_submit_address(
    properties: &HashMap<u32, MapiValue>,
    tags: &[u32],
) -> Option<String> {
    optional_pending_text_property(properties, tags).and_then(normalize_mapi_submit_address)
}

pub(in crate::mapi) fn normalize_mapi_submit_address(value: String) -> Option<String> {
    let trimmed = value.trim();
    let address = trimmed
        .strip_prefix("SMTP:")
        .or_else(|| trimmed.strip_prefix("smtp:"))
        .unwrap_or(trimmed)
        .trim();
    let normalized = lpe_storage::normalize_mailbox_email(address);
    (!normalized.is_empty()).then_some(normalized)
}

pub(in crate::mapi) fn mapi_submit_from_email(
    principal: &AccountPrincipal,
    email: &JmapEmail,
    attachments: Vec<AttachmentUploadInput>,
) -> SubmitMessageInput {
    SubmitMessageInput {
        draft_message_id: Some(email.id),
        account_id: principal.account_id,
        submitted_by_account_id: principal.account_id,
        source: "mapi-submit-message".to_string(),
        from_display: email.from_display.clone(),
        from_address: email.from_address.clone(),
        sender_display: email.sender_display.clone(),
        sender_address: email.sender_address.clone(),
        to: submitted_recipients_from_addresses(&email.to),
        cc: submitted_recipients_from_addresses(&email.cc),
        bcc: submitted_recipients_from_addresses(&email.bcc),
        subject: email.subject.clone(),
        body_text: email.body_text.clone(),
        body_html_sanitized: email.body_html_sanitized.clone(),
        internet_message_id: email.internet_message_id.clone(),
        mime_blob_ref: email.mime_blob_ref.clone(),
        size_octets: i64::try_from(email.size_octets).unwrap_or(i64::MAX),
        unread: Some(email.unread),
        flagged: Some(email.flagged),
        attachments,
    }
}

pub(in crate::mapi) fn submitted_recipients_from_addresses(
    addresses: &[JmapEmailAddress],
) -> Vec<SubmittedRecipientInput> {
    addresses
        .iter()
        .map(|address| SubmittedRecipientInput {
            address: address.address.clone(),
            display_name: address.display_name.clone(),
        })
        .collect()
}

pub(in crate::mapi) fn submitted_mapi_folder_id(
    submitted: &SubmittedMessage,
    mailboxes: &[JmapMailbox],
) -> u64 {
    mailboxes
        .iter()
        .find(|mailbox| mailbox.id == submitted.sent_mailbox_id)
        .map(mapi_folder_id)
        .unwrap_or(SENT_FOLDER_ID)
}

pub(in crate::mapi) fn apply_pending_recipient_changes(
    recipients: &mut Vec<PendingRecipient>,
    changes: Vec<PendingRecipientChange>,
) {
    for change in changes {
        match change {
            PendingRecipientChange::Delete(row_id) => {
                recipients.retain(|recipient| recipient.row_id != row_id);
            }
            PendingRecipientChange::Upsert(recipient) => {
                if let Some(existing) = recipients
                    .iter_mut()
                    .find(|existing| existing.row_id == recipient.row_id)
                {
                    *existing = recipient;
                } else {
                    recipients.push(recipient);
                }
            }
        }
    }
    recipients.sort_by_key(|recipient| recipient.row_id);
}

pub(in crate::mapi) fn hierarchy_display_name(
    hierarchy_values: &[(u32, MapiValue)],
    property_values: &[(u32, MapiValue)],
) -> Option<String> {
    hierarchy_values
        .iter()
        .chain(property_values.iter())
        .rev()
        .find_map(|(tag, value)| {
            (*tag == PID_TAG_DISPLAY_NAME_W)
                .then(|| value.as_text().map(str::trim).map(str::to_string))
                .flatten()
        })
        .filter(|value| !value.is_empty())
}

pub(in crate::mapi) fn imported_hierarchy_existing_mailbox<'a>(
    hierarchy_values: &[(u32, MapiValue)],
    display_name: &str,
    mailboxes: &'a [JmapMailbox],
) -> Option<&'a JmapMailbox> {
    let source_key = hierarchy_values
        .iter()
        .find_map(|(tag, value)| match (tag, value) {
            (tag, MapiValue::Binary(value)) if *tag == PID_TAG_SOURCE_KEY => Some(value.as_slice()),
            _ => None,
        });
    if let Some(source_key) = source_key {
        if let Some(mailbox) = mailboxes.iter().find(|mailbox| {
            mapi_mailstore::source_key_for_mailbox_folder(mailbox) == source_key
                || mapi_mailstore::source_key_for_uuid(&mailbox.id) == source_key
        }) {
            return Some(mailbox);
        }
    }

    mailboxes
        .iter()
        .find(|mailbox| mailbox.name.eq_ignore_ascii_case(display_name))
}

pub(in crate::mapi) fn system_folder_display_name(display_name: &str) -> bool {
    matches!(
        display_name.trim().to_ascii_lowercase().as_str(),
        "inbox"
            | "drafts"
            | "sent"
            | "sent items"
            | "deleted"
            | "deleted items"
            | "trash"
            | "outbox"
            | "sync issues"
            | "conflicts"
            | "local failures"
            | "server failures"
            | "junk e-mail"
            | "junk email"
            | "rss feeds"
            | "archive"
            | "conversation history"
    )
}

pub(in crate::mapi) async fn apply_canonical_message_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    message_id: u64,
    values: Vec<(u32, MapiValue)>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Result<()>
where
    S: ExchangeStore,
{
    let email = message_for_id(folder_id, message_id, mailboxes, emails)
        .ok_or_else(|| anyhow!("canonical MAPI message was not found"))?;
    let mut subject = None;
    let mut body_text = None;
    let mut followup_values = Vec::new();
    for (tag, value) in values {
        match tag {
            PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                subject = Some(
                    value
                        .into_text()
                        .ok_or_else(|| anyhow!("invalid PidTagSubject value"))?,
                );
            }
            PID_TAG_BODY_W => {
                body_text = Some(
                    value
                        .into_text()
                        .ok_or_else(|| anyhow!("invalid PidTagBody value"))?,
                );
            }
            _ => followup_values.push((tag, value)),
        }
    }

    if subject.is_some() || body_text.is_some() {
        store
            .update_jmap_email_content(
                principal.account_id,
                email.id,
                subject,
                body_text,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-set-message-content".to_string(),
                    subject: format!("message:{}", email.id),
                },
            )
            .await?;
    }
    let update = message_followup_update_from_mapi_values(followup_values)?;
    if message_followup_update_is_empty(&update) {
        return Ok(());
    }

    store
        .update_jmap_email_followup_flags(
            principal.account_id,
            email.id,
            update,
            AuditEntryInput {
                actor: principal.email.clone(),
                action: "mapi-set-message-properties".to_string(),
                subject: format!("message:{}", email.id),
            },
        )
        .await?;
    Ok(())
}

pub(in crate::mapi) fn message_followup_update_from_mapi_values(
    values: Vec<(u32, MapiValue)>,
) -> Result<lpe_storage::JmapEmailFollowupUpdate> {
    let mut update = lpe_storage::JmapEmailFollowupUpdate::default();
    for (tag, value) in values {
        match tag {
            PID_TAG_MESSAGE_FLAGS => {
                let flags = value
                    .into_u32()
                    .ok_or_else(|| anyhow!("invalid PidTagMessageFlags value"))?;
                update.unread = Some(flags & MSGFLAG_READ == 0);
            }
            PID_TAG_FLAG_STATUS => {
                let status = match value
                    .as_i64()
                    .ok_or_else(|| anyhow!("invalid PidTagFlagStatus value"))?
                {
                    0 => "none",
                    1 => "complete",
                    2 => "flagged",
                    _ => return Err(anyhow!("invalid PidTagFlagStatus value")),
                };
                update.flagged = Some(status != "none");
                update.followup_flag_status = Some(status.to_string());
            }
            PID_TAG_FOLLOWUP_ICON => {
                update.followup_icon = Some(
                    value
                        .as_i64()
                        .and_then(|value| i32::try_from(value).ok())
                        .ok_or_else(|| anyhow!("invalid PidTagFollowupIcon value"))?,
                );
            }
            PID_TAG_TODO_ITEM_FLAGS => {
                update.todo_item_flags = Some(
                    value
                        .as_i64()
                        .and_then(|value| i32::try_from(value).ok())
                        .ok_or_else(|| anyhow!("invalid PidTagToDoItemFlags value"))?,
                );
            }
            PID_TAG_FLAG_COMPLETE_TIME => {
                update.followup_completed_at = Some(
                    value
                        .as_i64()
                        .and_then(filetime_to_rfc3339_utc)
                        .ok_or_else(|| anyhow!("invalid PidTagFlagCompleteTime value"))?,
                );
            }
            PID_LID_TASK_START_DATE_TAG => {
                update.followup_start_at = Some(
                    value
                        .as_i64()
                        .and_then(filetime_to_rfc3339_utc)
                        .ok_or_else(|| anyhow!("invalid PidLidTaskStartDate value"))?,
                );
            }
            PID_LID_TASK_DUE_DATE_TAG => {
                update.followup_due_at = Some(
                    value
                        .as_i64()
                        .and_then(filetime_to_rfc3339_utc)
                        .ok_or_else(|| anyhow!("invalid PidLidTaskDueDate value"))?,
                );
            }
            PID_LID_REMINDER_SET_TAG => {
                update.reminder_set = Some(
                    value
                        .as_bool()
                        .ok_or_else(|| anyhow!("invalid PidLidReminderSet value"))?,
                );
            }
            PID_LID_REMINDER_TIME_TAG | PID_LID_REMINDER_SIGNAL_TIME_TAG => {
                update.reminder_at = Some(
                    value
                        .as_i64()
                        .and_then(filetime_to_rfc3339_utc)
                        .ok_or_else(|| anyhow!("invalid reminder time value"))?,
                );
            }
            PID_LID_FLAG_REQUEST_W_TAG => {
                update.followup_request = Some(
                    value
                        .into_text()
                        .ok_or_else(|| anyhow!("invalid PidLidFlagRequest value"))?,
                );
            }
            PID_TAG_SWAPPED_TODO_STORE => {
                let MapiValue::Binary(bytes) = value else {
                    return Err(anyhow!("invalid PidTagSwappedToDoStore value"));
                };
                update.swapped_todo_store_id = Some(
                    Uuid::from_slice(&bytes)
                        .map_err(|_| anyhow!("invalid PidTagSwappedToDoStore value"))?,
                );
            }
            PID_TAG_SWAPPED_TODO_DATA => {
                let MapiValue::Binary(bytes) = value else {
                    return Err(anyhow!("invalid PidTagSwappedToDoData value"));
                };
                parse_swapped_todo_data(&bytes)
                    .map_err(|error| anyhow!("invalid PidTagSwappedToDoData value: {error}"))?;
                update.swapped_todo_data = Some(bytes);
            }
            PID_NAME_KEYWORDS_TAG => {
                update.categories = Some(categories_from_mapi_value(value)?);
            }
            PID_TAG_SOURCE_KEY | PID_TAG_CHANGE_KEY | PID_TAG_PREDECESSOR_CHANGE_LIST => {}
            _ => return Err(anyhow!("canonical MAPI message property is not mutable")),
        }
    }
    Ok(update)
}

pub(in crate::mapi) fn message_followup_update_is_empty(
    update: &lpe_storage::JmapEmailFollowupUpdate,
) -> bool {
    update.unread.is_none()
        && update.flagged.is_none()
        && update.followup_flag_status.is_none()
        && update.followup_icon.is_none()
        && update.todo_item_flags.is_none()
        && update.followup_request.is_none()
        && update.followup_start_at.is_none()
        && update.followup_due_at.is_none()
        && update.followup_completed_at.is_none()
        && update.reminder_set.is_none()
        && update.reminder_at.is_none()
        && update.reminder_dismissed_at.is_none()
        && update.swapped_todo_store_id.is_none()
        && update.swapped_todo_data.is_none()
        && update.categories.is_none()
}

pub(in crate::mapi) fn categories_from_mapi_value(value: MapiValue) -> Result<Vec<String>> {
    let mut categories = match value {
        MapiValue::MultiString(values) => values,
        MapiValue::String(value) => vec![value],
        _ => return Err(anyhow!("invalid PidNameKeywords value")),
    };
    for category in &mut categories {
        *category = category.trim().to_string();
    }
    categories.retain(|category| !category.is_empty());
    categories.sort();
    categories.dedup();
    Ok(categories)
}

pub(in crate::mapi) fn filetime_to_rfc3339_utc(filetime: i64) -> Option<String> {
    filetime_to_date_time(filetime).map(|(date, time)| format!("{date}T{time}:00Z"))
}

#[derive(Debug, PartialEq, Eq)]
struct SwappedToDoData {
    flags: u32,
    todo_item_flags: Option<u32>,
    flag_request: Option<String>,
    start_minutes: Option<u32>,
    due_minutes: Option<u32>,
    reminder_minutes: Option<u32>,
    reminder_set: Option<bool>,
}

const SWAPPED_TODO_DATA_LEN: usize = 540;
const SWAPPED_TODO_DATA_VERSION: u32 = 1;
const SWAPPED_TODO_NO_DATE: u32 = 0x5AE9_80E0;
const SWAPPED_TODO_FLAG_TODO_ITEM: u32 = 0x0000_0001;
const SWAPPED_TODO_FLAG_START_DATE: u32 = 0x0000_0008;
const SWAPPED_TODO_FLAG_DUE_DATE: u32 = 0x0000_0010;
const SWAPPED_TODO_FLAG_FLAG_TO: u32 = 0x0000_0020;
const SWAPPED_TODO_FLAG_REMINDER_SET: u32 = 0x0000_0040;
const SWAPPED_TODO_FLAG_REMINDER: u32 = 0x0000_0080;
const SWAPPED_TODO_KNOWN_FLAGS: u32 = SWAPPED_TODO_FLAG_TODO_ITEM
    | SWAPPED_TODO_FLAG_START_DATE
    | SWAPPED_TODO_FLAG_DUE_DATE
    | SWAPPED_TODO_FLAG_FLAG_TO
    | SWAPPED_TODO_FLAG_REMINDER_SET
    | SWAPPED_TODO_FLAG_REMINDER;

fn parse_swapped_todo_data(bytes: &[u8]) -> Result<SwappedToDoData> {
    if bytes.len() != SWAPPED_TODO_DATA_LEN {
        return Err(anyhow!("expected {SWAPPED_TODO_DATA_LEN} bytes"));
    }
    let version = read_swapped_u32(bytes, 0)?;
    if version != SWAPPED_TODO_DATA_VERSION {
        return Err(anyhow!("unsupported version {version}"));
    }
    let flags = read_swapped_u32(bytes, 4)?;
    if flags & !SWAPPED_TODO_KNOWN_FLAGS != 0 {
        return Err(anyhow!(
            "unknown flags {:#010x}",
            flags & !SWAPPED_TODO_KNOWN_FLAGS
        ));
    }
    let todo_item_flags = (flags & SWAPPED_TODO_FLAG_TODO_ITEM != 0)
        .then(|| read_swapped_u32(bytes, 8))
        .transpose()?;
    let flag_request = if flags & SWAPPED_TODO_FLAG_FLAG_TO != 0 {
        Some(read_swapped_utf16z(
            bytes
                .get(12..524)
                .ok_or_else(|| anyhow!("truncated flag text"))?,
        )?)
    } else {
        None
    };
    let start_minutes =
        swapped_todo_minutes(bytes, 524, flags & SWAPPED_TODO_FLAG_START_DATE != 0)?;
    let due_minutes = swapped_todo_minutes(bytes, 528, flags & SWAPPED_TODO_FLAG_DUE_DATE != 0)?;
    let reminder_minutes =
        swapped_todo_minutes(bytes, 532, flags & SWAPPED_TODO_FLAG_REMINDER != 0)?;
    let reminder_set = if flags & SWAPPED_TODO_FLAG_REMINDER_SET != 0 {
        match read_swapped_u32(bytes, 536)? {
            0 => Some(false),
            1 => Some(true),
            value => return Err(anyhow!("invalid reminder boolean {value}")),
        }
    } else {
        None
    };
    Ok(SwappedToDoData {
        flags,
        todo_item_flags,
        flag_request,
        start_minutes,
        due_minutes,
        reminder_minutes,
        reminder_set,
    })
}

fn swapped_todo_minutes(bytes: &[u8], offset: usize, valid: bool) -> Result<Option<u32>> {
    if !valid {
        return Ok(None);
    }
    let value = read_swapped_u32(bytes, offset)?;
    Ok((value != SWAPPED_TODO_NO_DATE).then_some(value))
}

fn read_swapped_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| anyhow!("truncated u32"))?;
    Ok(u32::from_le_bytes(
        value.try_into().map_err(|_| anyhow!("invalid u32"))?,
    ))
}

fn read_swapped_utf16z(bytes: &[u8]) -> Result<String> {
    if bytes.len() % 2 != 0 {
        return Err(anyhow!("odd utf16 byte length"));
    }
    let units = bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .take_while(|unit| *unit != 0)
        .collect::<Vec<_>>();
    String::from_utf16(&units).map_err(|_| anyhow!("invalid utf16 flag text"))
}

pub(in crate::mapi) async fn apply_canonical_contact_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    contact_id: u64,
    values: Vec<(u32, MapiValue)>,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let contact = snapshot
        .contact_for_id(folder_id, contact_id)
        .ok_or_else(|| anyhow!("canonical MAPI contact was not found"))?;
    let properties = values.into_iter().collect::<HashMap<_, _>>();
    reject_unsupported_mapi_contact_properties(&properties)?;
    let input = contact_input_from_mapi(
        principal.account_id,
        Some(contact.canonical_id),
        &contact.contact,
        &properties,
    );
    store
        .update_accessible_contact(principal.account_id, contact.canonical_id, input)
        .await?;
    Ok(())
}

pub(in crate::mapi) async fn apply_canonical_event_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    event_id: u64,
    values: Vec<(u32, MapiValue)>,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    if mapi_calendar_event_mutation_suppressed(folder_id, snapshot) {
        bail!("guarded MAPI calendar event mutation is hidden");
    }

    enum EventPropertyMutation {
        None,
        Delete,
        Update(UpsertClientEventInput),
    }

    let event = snapshot
        .event_for_id(folder_id, event_id)
        .ok_or_else(|| anyhow!("canonical MAPI calendar event was not found"))?;
    let (properties, reminder_set, reminder_at) = split_reminder_property_values(values)?;
    let mutation = if properties.is_empty() {
        EventPropertyMutation::None
    } else if bounded_meeting_cancellation_from_mapi(&properties)? {
        EventPropertyMutation::Delete
    } else if let Some(input) = meeting_response_event_input_from_mapi(
        principal.account_id,
        Some(event.canonical_id),
        &event.event,
        &properties,
    )? {
        EventPropertyMutation::Update(input)
    } else {
        EventPropertyMutation::Update(event_input_from_mapi(
            principal.account_id,
            Some(event.canonical_id),
            &event.event,
            &properties,
        )?)
    };
    if matches!(mutation, EventPropertyMutation::Delete) {
        store
            .delete_accessible_event(principal.account_id, event.canonical_id)
            .await?;
        return Ok(());
    }
    if reminder_set.is_some() || reminder_at.is_some() {
        store
            .update_accessible_event_reminder(
                principal.account_id,
                event.canonical_id,
                reminder_set,
                reminder_at,
                None,
            )
            .await?;
    }
    if let EventPropertyMutation::Update(input) = mutation {
        store
            .update_accessible_event(principal.account_id, event.canonical_id, input)
            .await?;
    }
    Ok(())
}

fn mapi_calendar_event_mutation_suppressed(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> bool {
    folder_id == CALENDAR_FOLDER_ID
        || snapshot
            .collaboration_folder_for_id(folder_id)
            .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
}

pub(in crate::mapi) async fn apply_canonical_task_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    task_id: u64,
    values: Vec<(u32, MapiValue)>,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let task = snapshot
        .task_for_id(folder_id, task_id)
        .ok_or_else(|| anyhow!("canonical MAPI task was not found"))?;
    let (properties, reminder_set, reminder_at) = split_reminder_property_values(values)?;
    if reminder_set.is_some() || reminder_at.is_some() {
        store
            .update_accessible_task_reminder(
                principal.account_id,
                task.canonical_id,
                reminder_set,
                reminder_at,
                None,
                None,
            )
            .await?;
    }
    if properties.is_empty() {
        return Ok(());
    }
    reject_unsupported_mapi_task_properties(&properties)?;
    let input = task_input_from_mapi(
        principal.account_id,
        Some(task.canonical_id),
        &task.task,
        None,
        &properties,
    );
    store
        .update_accessible_task(principal.account_id, task.canonical_id, input)
        .await?;
    Ok(())
}

fn split_reminder_property_values(
    values: Vec<(u32, MapiValue)>,
) -> Result<(HashMap<u32, MapiValue>, Option<bool>, Option<String>)> {
    let mut properties = HashMap::new();
    let mut reminder_set = None;
    let mut reminder_at = None;
    for (tag, value) in values {
        match canonical_property_storage_tag(tag) {
            PID_LID_REMINDER_SET_TAG => {
                reminder_set = Some(
                    value
                        .as_bool()
                        .ok_or_else(|| anyhow!("invalid PidLidReminderSet value"))?,
                );
            }
            PID_LID_REMINDER_TIME_TAG | PID_LID_REMINDER_SIGNAL_TIME_TAG => {
                reminder_at = Some(
                    value
                        .as_i64()
                        .and_then(filetime_to_rfc3339_utc)
                        .ok_or_else(|| anyhow!("invalid reminder time value"))?,
                );
            }
            _ => {
                properties.insert(tag, value);
            }
        }
    }
    Ok((properties, reminder_set, reminder_at))
}

pub(in crate::mapi) async fn apply_canonical_note_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    note_id: u64,
    values: Vec<(u32, MapiValue)>,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let note = snapshot
        .note_for_id(folder_id, note_id)
        .ok_or_else(|| anyhow!("canonical MAPI note was not found"))?;
    let properties = values.into_iter().collect::<HashMap<_, _>>();
    reject_unsupported_mapi_note_properties(&properties)?;
    let input = note_input_from_mapi(
        principal.account_id,
        Some(note.canonical_id),
        &note.note,
        &properties,
    );
    store.upsert_mapi_note(input).await?;
    Ok(())
}

pub(in crate::mapi) async fn apply_canonical_journal_entry_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    journal_entry_id: u64,
    values: Vec<(u32, MapiValue)>,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let entry = snapshot
        .journal_entry_for_id(folder_id, journal_entry_id)
        .ok_or_else(|| anyhow!("canonical MAPI journal entry was not found"))?;
    let properties = values.into_iter().collect::<HashMap<_, _>>();
    reject_unsupported_mapi_journal_entry_properties(&properties)?;
    let input = journal_entry_input_from_mapi(
        principal.account_id,
        Some(entry.canonical_id),
        &entry.entry,
        &properties,
    );
    store.upsert_mapi_journal_entry(input).await?;
    Ok(())
}

pub(in crate::mapi) fn apply_mapi_property_values(
    object: Option<&mut MapiObject>,
    values: Vec<(u32, MapiValue)>,
) -> Result<()> {
    let values = values
        .into_iter()
        .map(|(tag, value)| (canonical_property_storage_tag(tag), value))
        .collect::<Vec<_>>();
    match object {
        Some(MapiObject::PendingMessage { properties, .. }) => {
            properties.extend(values);
            Ok(())
        }
        Some(MapiObject::PendingAssociatedMessage { properties, .. })
        | Some(MapiObject::PendingContact { properties, .. })
        | Some(MapiObject::PendingEvent { properties, .. })
        | Some(MapiObject::PendingTask { properties, .. })
        | Some(MapiObject::PendingNote { properties, .. })
        | Some(MapiObject::PendingJournalEntry { properties, .. })
        | Some(MapiObject::PendingConversationAction { properties, .. })
        | Some(MapiObject::PendingNavigationShortcut { properties, .. }) => {
            properties.extend(values);
            Ok(())
        }
        Some(MapiObject::PendingAttachment {
            properties, data, ..
        }) => {
            for (tag, value) in values {
                if tag == PID_TAG_ATTACH_DATA_BINARY {
                    if let MapiValue::Binary(bytes) = &value {
                        *data = bytes.clone();
                    }
                }
                properties.insert(tag, value);
            }
            Ok(())
        }
        Some(MapiObject::Folder {
            folder_id,
            properties,
        }) => {
            properties.extend(values.into_iter().filter(|(tag, _)| {
                *folder_id != ROOT_FOLDER_ID
                    || !is_default_folder_identification_property_tag(*tag)
                    || is_scalar_default_folder_entry_id_property_tag(*tag)
            }));
            Ok(())
        }
        Some(MapiObject::Logon | MapiObject::PublicFolderLogon) => Ok(()),
        _ => Err(anyhow!("MAPI object does not support property mutation")),
    }
}

pub(in crate::mapi) fn delete_mapi_properties(
    object: Option<&mut MapiObject>,
    property_tags: &[u32],
) -> Result<()> {
    let property_tags = property_tags
        .iter()
        .flat_map(|tag| [*tag, canonical_property_storage_tag(*tag)])
        .collect::<Vec<_>>();
    match object {
        Some(MapiObject::PendingMessage { properties, .. }) => {
            for tag in &property_tags {
                properties.remove(tag);
            }
            Ok(())
        }
        Some(MapiObject::PendingAssociatedMessage { properties, .. })
        | Some(MapiObject::PendingContact { properties, .. })
        | Some(MapiObject::PendingEvent { properties, .. })
        | Some(MapiObject::PendingTask { properties, .. })
        | Some(MapiObject::PendingNote { properties, .. })
        | Some(MapiObject::PendingJournalEntry { properties, .. })
        | Some(MapiObject::PendingConversationAction { properties, .. })
        | Some(MapiObject::PendingNavigationShortcut { properties, .. }) => {
            for tag in &property_tags {
                properties.remove(tag);
            }
            Ok(())
        }
        Some(MapiObject::PendingAttachment {
            properties, data, ..
        }) => {
            for tag in &property_tags {
                properties.remove(tag);
                if *tag == PID_TAG_ATTACH_DATA_BINARY {
                    data.clear();
                }
            }
            Ok(())
        }
        Some(MapiObject::Folder { properties, .. }) => {
            for tag in &property_tags {
                properties.remove(tag);
            }
            Ok(())
        }
        _ => Err(anyhow!("MAPI object does not support property deletion")),
    }
}

#[cfg(test)]
mod tests;
