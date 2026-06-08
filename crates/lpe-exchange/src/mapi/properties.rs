use super::rop::*;
use super::session::*;
use super::sync::*;
use super::tables::*;
use super::wire::MapiPropertyType;
use super::*;
use crate::mapi_store::{
    MapiAssociatedConfigMessage, MapiCommonViewNamedViewMessage, MapiConversationActionMessage,
    MapiMessage, MapiNavigationShortcutMessage, MapiPublicFolder,
};
use anyhow::bail;
use lpe_storage::{
    calendar_attendee_labels, normalize_calendar_email, parse_calendar_participants_metadata,
    serialize_calendar_participants_metadata, CalendarOrganizerMetadata,
    CalendarParticipantMetadata,
};

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
    And(Vec<MapiRestriction>),
    Or(Vec<MapiRestriction>),
    Not(Box<MapiRestriction>),
    Content {
        property_tag: u32,
        value: String,
    },
    Property {
        relop: u8,
        property_tag: u32,
        value: MapiValue,
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

pub(in crate::mapi) fn mapi_properties_to_json(
    properties: &HashMap<u32, MapiValue>,
) -> serde_json::Value {
    let mut values = serde_json::Map::new();
    for (tag, value) in properties {
        values.insert(format!("0x{tag:08x}"), mapi_value_to_json(value));
    }
    serde_json::Value::Object(values)
}

pub(in crate::mapi) fn mapi_properties_from_json(
    properties: &serde_json::Value,
) -> HashMap<u32, MapiValue> {
    properties
        .as_object()
        .map(|values| {
            values
                .iter()
                .filter_map(|(tag, value)| {
                    let tag = u32::from_str_radix(tag.trim_start_matches("0x"), 16).ok()?;
                    Some((tag, mapi_value_from_json(value)?))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn mapi_value_to_json(value: &MapiValue) -> serde_json::Value {
    match value {
        MapiValue::Bool(value) => serde_json::json!({"type": "bool", "value": value}),
        MapiValue::I16(value) => serde_json::json!({"type": "i16", "value": value}),
        MapiValue::I32(value) => serde_json::json!({"type": "i32", "value": value}),
        MapiValue::I64(value) => serde_json::json!({"type": "i64", "value": value}),
        MapiValue::U32(value) => serde_json::json!({"type": "u32", "value": value}),
        MapiValue::U64(value) => serde_json::json!({"type": "u64", "value": value}),
        MapiValue::F64(value) => {
            serde_json::json!({"type": "f64", "value": f64::from_bits(*value)})
        }
        MapiValue::String(value) => serde_json::json!({"type": "string", "value": value}),
        MapiValue::Binary(value) => {
            serde_json::json!({"type": "binary", "value": bytes_to_hex(value)})
        }
        MapiValue::Guid(value) => serde_json::json!({"type": "guid", "value": bytes_to_hex(value)}),
        MapiValue::Error(value) => serde_json::json!({"type": "error", "value": value}),
        MapiValue::MultiI16(values) => serde_json::json!({"type": "multi_i16", "value": values}),
        MapiValue::MultiI32(values) => serde_json::json!({"type": "multi_i32", "value": values}),
        MapiValue::MultiI64(values) => serde_json::json!({"type": "multi_i64", "value": values}),
        MapiValue::MultiString(values) => {
            serde_json::json!({"type": "multi_string", "value": values})
        }
        MapiValue::MultiBinary(values) => serde_json::json!({
            "type": "multi_binary",
            "value": values.iter().map(|value| bytes_to_hex(value)).collect::<Vec<_>>()
        }),
        MapiValue::MultiGuid(values) => serde_json::json!({
            "type": "multi_guid",
            "value": values.iter().map(|value| bytes_to_hex(value)).collect::<Vec<_>>()
        }),
    }
}

fn mapi_value_from_json(value: &serde_json::Value) -> Option<MapiValue> {
    let value_type = value.get("type")?.as_str()?;
    let value = value.get("value")?;
    match value_type {
        "bool" => Some(MapiValue::Bool(value.as_bool()?)),
        "i16" => Some(MapiValue::I16(value.as_i64()?.try_into().ok()?)),
        "i32" => Some(MapiValue::I32(value.as_i64()?.try_into().ok()?)),
        "i64" => Some(MapiValue::I64(value.as_i64()?)),
        "u32" => Some(MapiValue::U32(value.as_u64()?.try_into().ok()?)),
        "u64" => Some(MapiValue::U64(value.as_u64()?)),
        "f64" => Some(MapiValue::F64(value.as_f64()?.to_bits())),
        "string" => Some(MapiValue::String(value.as_str()?.to_string())),
        "binary" => Some(MapiValue::Binary(hex_to_bytes(value.as_str()?)?)),
        "guid" => Some(MapiValue::Guid(
            hex_to_bytes(value.as_str()?)?.try_into().ok()?,
        )),
        "error" => Some(MapiValue::Error(value.as_u64()?.try_into().ok()?)),
        "multi_i16" => Some(MapiValue::MultiI16(json_i64_values(value)?)),
        "multi_i32" => Some(MapiValue::MultiI32(json_i64_values(value)?)),
        "multi_i64" => Some(MapiValue::MultiI64(
            value
                .as_array()?
                .iter()
                .map(serde_json::Value::as_i64)
                .collect::<Option<Vec<_>>>()?,
        )),
        "multi_string" => Some(MapiValue::MultiString(
            value
                .as_array()?
                .iter()
                .map(|value| value.as_str().map(str::to_string))
                .collect::<Option<Vec<_>>>()?,
        )),
        "multi_binary" => Some(MapiValue::MultiBinary(json_hex_values(value)?)),
        "multi_guid" => Some(MapiValue::MultiGuid(
            json_hex_values(value)?
                .into_iter()
                .map(|value| value.try_into().ok())
                .collect::<Option<Vec<_>>>()?,
        )),
        _ => None,
    }
}

fn json_i64_values<T>(value: &serde_json::Value) -> Option<Vec<T>>
where
    T: TryFrom<i64>,
{
    value
        .as_array()?
        .iter()
        .map(|value| value.as_i64()?.try_into().ok())
        .collect()
}

fn json_hex_values(value: &serde_json::Value) -> Option<Vec<Vec<u8>>> {
    value
        .as_array()?
        .iter()
        .map(|value| hex_to_bytes(value.as_str()?))
        .collect()
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_to_bytes(value: &str) -> Option<Vec<u8>> {
    if value.len() % 2 != 0 {
        return None;
    }
    (0..value.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&value[index..index + 2], 16).ok())
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct MapiPropertyTag {
    raw: u32,
}

impl MapiPropertyTag {
    pub(in crate::mapi) fn new(raw: u32) -> Self {
        Self { raw }
    }

    pub(in crate::mapi) fn property_id(self) -> u16 {
        (self.raw >> 16) as u16
    }

    pub(in crate::mapi) fn property_type_code(self) -> u16 {
        (self.raw & 0xFFFF) as u16
    }

    pub(in crate::mapi) fn property_type(self) -> Option<MapiPropertyType> {
        MapiPropertyType::from_code(self.property_type_code())
    }
}

pub(crate) fn canonical_property_storage_tag(property_tag: u32) -> u32 {
    let tag = MapiPropertyTag::new(property_tag);
    if tag.property_id() >= FIRST_NAMED_PROPERTY_ID {
        return property_tag;
    }
    match tag.property_type() {
        Some(MapiPropertyType::String8) => (property_tag & 0xFFFF_0000) | 0x001F,
        Some(MapiPropertyType::MultipleString8) => (property_tag & 0xFFFF_0000) | 0x101F,
        _ => property_tag,
    }
}

pub(in crate::mapi) const PID_TAG_DISPLAY_NAME_W: u32 = 0x3001_001F;
pub(in crate::mapi) const PID_TAG_CONTENT_COUNT: u32 = 0x3602_0003;
pub(in crate::mapi) const PID_TAG_CONTENT_UNREAD_COUNT: u32 = 0x3603_0003;
pub(in crate::mapi) const PID_TAG_SUBFOLDERS: u32 = 0x360A_000B;
pub(in crate::mapi) const PID_TAG_FOLDER_TYPE: u32 = 0x3601_0003;
pub(in crate::mapi) const PID_TAG_CONTAINER_CLASS_W: u32 = 0x3613_001F;
pub(in crate::mapi) const PID_TAG_DEFAULT_VIEW_ENTRY_ID: u32 = 0x3616_0102;
pub(in crate::mapi) const PID_TAG_ASSOCIATED_CONTENT_COUNT: u32 = 0x3617_0003;
pub(in crate::mapi) const PID_TAG_VALID_FOLDER_MASK: u32 = 0x35DF_0003;
pub(in crate::mapi) const PID_TAG_IPM_SUBTREE_ENTRY_ID: u32 = 0x35E0_0102;
pub(in crate::mapi) const PID_TAG_IPM_OUTBOX_ENTRY_ID: u32 = 0x35E2_0102;
pub(in crate::mapi) const PID_TAG_IPM_WASTEBASKET_ENTRY_ID: u32 = 0x35E3_0102;
pub(in crate::mapi) const PID_TAG_IPM_SENTMAIL_ENTRY_ID: u32 = 0x35E4_0102;
pub(in crate::mapi) const PID_TAG_VIEWS_ENTRY_ID: u32 = 0x35E5_0102;
pub(in crate::mapi) const PID_TAG_COMMON_VIEWS_ENTRY_ID: u32 = 0x35E6_0102;
pub(in crate::mapi) const PID_TAG_FINDER_ENTRY_ID: u32 = 0x35E7_0102;
pub(in crate::mapi) const PID_TAG_IPM_ARCHIVE_ENTRY_ID: u32 = 0x35FF_0102;
pub(in crate::mapi) const PID_TAG_IPM_APPOINTMENT_ENTRY_ID: u32 = 0x36D0_0102;
pub(in crate::mapi) const PID_TAG_IPM_CONTACT_ENTRY_ID: u32 = 0x36D1_0102;
pub(in crate::mapi) const PID_TAG_IPM_JOURNAL_ENTRY_ID: u32 = 0x36D2_0102;
pub(in crate::mapi) const PID_TAG_IPM_NOTE_ENTRY_ID: u32 = 0x36D3_0102;
pub(in crate::mapi) const PID_TAG_IPM_TASK_ENTRY_ID: u32 = 0x36D4_0102;
pub(in crate::mapi) const PID_TAG_REM_ONLINE_ENTRY_ID: u32 = 0x36D5_0102;
pub(in crate::mapi) const PID_TAG_REM_OFFLINE_ENTRY_ID: u32 = 0x36D6_0102;
pub(in crate::mapi) const PID_TAG_IPM_DRAFTS_ENTRY_ID: u32 = 0x36D7_0102;
pub(in crate::mapi) const PID_TAG_ADDITIONAL_REN_ENTRY_IDS: u32 = 0x36D8_1102;
pub(in crate::mapi) const PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX: u32 = 0x36D9_0102;
pub(in crate::mapi) const PID_TAG_FREE_BUSY_ENTRY_IDS: u32 = 0x36E4_1102;
pub(in crate::mapi) const PID_TAG_HIER_REV: u32 = 0x4082_0040;
pub(in crate::mapi) const PID_TAG_FOLDER_ID: u32 = 0x6748_0014;
pub(in crate::mapi) const PID_TAG_PARENT_FOLDER_ID: u32 = 0x6749_0014;
pub(in crate::mapi) const PID_TAG_MESSAGE_CLASS_STRING8: u32 = 0x001A_001E;
pub(in crate::mapi) const PID_TAG_MESSAGE_CLASS_W: u32 = 0x001A_001F;
pub(in crate::mapi) const PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8: u32 = 0x36E5_001E;
pub(in crate::mapi) const PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W: u32 = 0x36E5_001F;
pub(in crate::mapi) const PID_TAG_DEFAULT_FORM_NAME_W: u32 = 0x36E6_001F;
pub(in crate::mapi) const PID_TAG_EXTENDED_FOLDER_FLAGS: u32 = 0x36DA_0102;
pub(in crate::mapi) const PID_TAG_FOLDER_FORM_FLAGS: u32 = 0x36DE_0003;
pub(in crate::mapi) const PID_TAG_FOLDER_WEBVIEWINFO: u32 = 0x36DF_0102;
pub(in crate::mapi) const PID_TAG_FOLDER_XVIEWINFO_E: u32 = 0x36E0_0102;
pub(in crate::mapi) const PID_TAG_FOLDER_VIEWS_ONLY: u32 = 0x36E1_0003;
pub(in crate::mapi) const PID_TAG_FOLDER_FORM_STORAGE: u32 = 0x36EB_0102;
pub(in crate::mapi) const PID_TAG_ARCHIVE_TAG: u32 = 0x3018_0102;
pub(in crate::mapi) const PID_TAG_POLICY_TAG: u32 = 0x3019_0102;
pub(in crate::mapi) const PID_TAG_RETENTION_PERIOD: u32 = 0x301A_0003;
pub(in crate::mapi) const PID_TAG_RETENTION_FLAGS: u32 = 0x301D_0003;
pub(in crate::mapi) const PID_TAG_ARCHIVE_PERIOD: u32 = 0x301E_0003;
pub(in crate::mapi) const PID_TAG_RIGHTS: u32 = 0x6639_0003;
pub(in crate::mapi) const PID_TAG_ACL_MEMBER_NAME_W: u32 = 0x6672_001F;
pub(in crate::mapi) const PID_TAG_FOLDER_VIEWLIST_FLAGS: u32 = 0x672D_0003;

pub(in crate::mapi) fn is_acl_member_name_property_tag(property_tag: u32) -> bool {
    MapiPropertyTag::new(property_tag).property_id()
        == MapiPropertyTag::new(PID_TAG_ACL_MEMBER_NAME_W).property_id()
}
pub(in crate::mapi) const PID_TAG_SUBJECT_W: u32 = 0x0037_001F;
pub(in crate::mapi) const PID_TAG_SENDER_NAME_W: u32 = 0x0C1A_001F;
pub(in crate::mapi) const PID_TAG_SENDER_ADDRESS_TYPE_W: u32 = 0x0C1E_001F;
pub(in crate::mapi) const PID_TAG_SENDER_EMAIL_ADDRESS_W: u32 = 0x0C1F_001F;
pub(in crate::mapi) const PID_TAG_RECIPIENT_TYPE: u32 = 0x0C15_0003;
pub(in crate::mapi) const PID_TAG_CLIENT_SUBMIT_TIME: u32 = 0x0039_0040;
pub(in crate::mapi) const PID_TAG_ORIGINAL_MESSAGE_CLASS_W: u32 = 0x004B_001F;
pub(in crate::mapi) const PID_TAG_DISPLAY_BCC_W: u32 = 0x0E02_001F;
pub(in crate::mapi) const PID_TAG_DISPLAY_CC_W: u32 = 0x0E03_001F;
pub(in crate::mapi) const PID_TAG_DISPLAY_TO_W: u32 = 0x0E04_001F;
pub(in crate::mapi) const PID_TAG_MESSAGE_DELIVERY_TIME: u32 = 0x0E06_0040;
pub(in crate::mapi) const PID_TAG_MESSAGE_FLAGS: u32 = 0x0E07_0003;
pub(in crate::mapi) const PID_TAG_MESSAGE_SIZE: u32 = 0x0E08_0003;
pub(in crate::mapi) const PID_TAG_MESSAGE_SIZE_EXTENDED: u32 = 0x0E08_0014;
pub(in crate::mapi) const PID_TAG_PARENT_ENTRY_ID: u32 = 0x0E09_0102;
pub(in crate::mapi) const OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B: u32 = 0x0E0B_0102;
pub(in crate::mapi) const PID_TAG_MESSAGE_STATUS: u32 = 0x0E17_0003;
pub(in crate::mapi) const PID_TAG_HAS_ATTACHMENTS: u32 = 0x0E1B_000B;
pub(in crate::mapi) const PID_TAG_NORMALIZED_SUBJECT_W: u32 = 0x0E1D_001F;
pub(in crate::mapi) const PID_TAG_RTF_IN_SYNC: u32 = 0x0E1F_000B;
pub(in crate::mapi) const PID_TAG_ASSOCIATED_SHARING_PROVIDER: u32 = 0x0EA0_0048;
pub(in crate::mapi) const PID_TAG_READ: u32 = 0x0E69_000B;
pub(in crate::mapi) const PID_TAG_CONVERSATION_TOPIC_W: u32 = 0x0070_001F;
pub(in crate::mapi) const PID_TAG_CONVERSATION_INDEX: u32 = 0x0071_0102;
pub(in crate::mapi) const PID_TAG_TRANSPORT_MESSAGE_HEADERS_W: u32 = 0x007D_001F;
pub(in crate::mapi) const PID_TAG_ACCESS: u32 = 0x0FF4_0003;
pub(in crate::mapi) const PID_TAG_ACCESS_LEVEL: u32 = 0x0FF7_0003;
pub(in crate::mapi) const PID_TAG_ROW_TYPE: u32 = 0x0FF5_0003;
pub(in crate::mapi) const PID_TAG_INSTANCE_KEY: u32 = 0x0FF6_0102;
pub(in crate::mapi) const PID_TAG_RECORD_KEY: u32 = 0x0FF9_0102;
pub(in crate::mapi) const PID_TAG_ENTRY_ID: u32 = 0x0FFF_0102;
pub(in crate::mapi) const PID_TAG_SEARCH_KEY: u32 = 0x300B_0102;
pub(in crate::mapi) const PID_TAG_BODY_STRING8: u32 = 0x1000_001E;
pub(in crate::mapi) const PID_TAG_BODY_W: u32 = 0x1000_001F;
pub(in crate::mapi) const PID_TAG_RTF_COMPRESSED: u32 = 0x1009_0102;
pub(in crate::mapi) const PID_TAG_BODY_HTML_W: u32 = 0x1013_001F;
pub(in crate::mapi) const PID_TAG_NATIVE_BODY: u32 = 0x1016_0003;
pub(in crate::mapi) const PID_TAG_ATTRIBUTE_HIDDEN: u32 = 0x10F4_000B;

pub(in crate::mapi) const FOLDER_ROOT: u32 = 0;
pub(in crate::mapi) const FOLDER_GENERIC: u32 = 1;
pub(in crate::mapi) const FOLDER_SEARCH: u32 = 2;
pub(in crate::mapi) const MAPI_ACCESS_MODIFY: u32 = 0x0000_0001;
pub(in crate::mapi) const MAPI_ACCESS_READ: u32 = 0x0000_0002;
pub(in crate::mapi) const MAPI_ACCESS_DELETE: u32 = 0x0000_0004;
pub(in crate::mapi) const MAPI_ACCESS_CREATE_HIERARCHY: u32 = 0x0000_0008;
pub(in crate::mapi) const MAPI_ACCESS_CREATE_CONTENTS: u32 = 0x0000_0010;
pub(in crate::mapi) const MAPI_ACCESS_CREATE_ASSOCIATED: u32 = 0x0000_0020;
pub(in crate::mapi) const MAPI_FOLDER_ACCESS: u32 = MAPI_ACCESS_MODIFY
    | MAPI_ACCESS_READ
    | MAPI_ACCESS_DELETE
    | MAPI_ACCESS_CREATE_HIERARCHY
    | MAPI_ACCESS_CREATE_CONTENTS
    | MAPI_ACCESS_CREATE_ASSOCIATED;
pub(in crate::mapi) const MAPI_MESSAGE_ACCESS: u32 =
    MAPI_ACCESS_MODIFY | MAPI_ACCESS_READ | MAPI_ACCESS_DELETE;
pub(in crate::mapi) const MSGFLAG_READ: u32 = 0x0000_0001;
pub(in crate::mapi) const MSGFLAG_UNSENT: u32 = 0x0000_0008;
pub(in crate::mapi) const FOLLOWUP_COMPLETE: u32 = 0x0000_0001;
pub(in crate::mapi) const FOLLOWUP_FLAGGED: u32 = 0x0000_0002;
pub(in crate::mapi) const PID_TAG_HTML_BINARY: u32 = 0x1013_0102;
pub(in crate::mapi) const PID_TAG_INTERNET_MESSAGE_ID_W: u32 = 0x1035_001F;
pub(in crate::mapi) const PID_TAG_FLAG_STATUS: u32 = 0x1090_0003;
pub(in crate::mapi) const PID_TAG_FLAG_COMPLETE_TIME: u32 = 0x1091_0040;
pub(in crate::mapi) const PID_TAG_FOLLOWUP_ICON: u32 = 0x1095_0003;
pub(in crate::mapi) const PID_TAG_TODO_ITEM_FLAGS: u32 = 0x0E2B_0003;
pub(in crate::mapi) const PID_TAG_SWAPPED_TODO_STORE: u32 = 0x0E2C_0102;
pub(in crate::mapi) const PID_TAG_SWAPPED_TODO_DATA: u32 = 0x0E2D_0102;
pub(in crate::mapi) const PID_TAG_SENDER_SMTP_ADDRESS_W: u32 = 0x5D01_001F;
pub(in crate::mapi) const PID_TAG_INTERNET_CODEPAGE: u32 = 0x3FDE_0003;
pub(in crate::mapi) const PID_TAG_MESSAGE_LOCALE_ID: u32 = 0x3FF1_0003;
pub(in crate::mapi) const PID_TAG_LAST_MODIFICATION_TIME: u32 = 0x3008_0040;
pub(in crate::mapi) const PID_TAG_HIERARCHY_CHANGE_NUMBER: u32 = 0x663E_0003;
pub(in crate::mapi) const PID_TAG_SOURCE_KEY: u32 = 0x65E0_0102;
pub(in crate::mapi) const PID_TAG_PARENT_SOURCE_KEY: u32 = 0x65E1_0102;
pub(in crate::mapi) const PID_TAG_CHANGE_KEY: u32 = 0x65E2_0102;
pub(in crate::mapi) const PID_TAG_PREDECESSOR_CHANGE_LIST: u32 = 0x65E3_0102;
pub(in crate::mapi) const PID_TAG_LOCAL_COMMIT_TIME: u32 = 0x6709_0040;
pub(in crate::mapi) const PID_TAG_LOCAL_COMMIT_TIME_MAX: u32 = 0x670A_0040;
pub(in crate::mapi) const PID_TAG_DELETED_COUNT_TOTAL: u32 = 0x670B_0003;
pub(in crate::mapi) const PID_TAG_SERIALIZED_REPLID_GUID_MAP: u32 = 0x6638_0102;
pub(in crate::mapi) const PID_TAG_RESOURCE_FLAGS: u32 = 0x3009_0003;
pub(in crate::mapi) const PID_TAG_USER_ENTRY_ID: u32 = 0x6619_0102;
pub(in crate::mapi) const PID_TAG_MAILBOX_OWNER_ENTRY_ID: u32 = 0x661B_0102;
pub(in crate::mapi) const PID_TAG_MAILBOX_OWNER_NAME_W: u32 = 0x661C_001F;
pub(in crate::mapi) const PID_TAG_IPM_PUBLIC_FOLDERS_ENTRY_ID: u32 = 0x6631_0102;
pub(in crate::mapi) const PID_TAG_SERVER_TYPE_DISPLAY_NAME_W: u32 = 0x341D_001F;
pub(in crate::mapi) const PID_TAG_SERVER_CONNECTED_ICON: u32 = 0x341E_0102;
pub(in crate::mapi) const PID_TAG_SERVER_ACCOUNT_ICON: u32 = 0x341F_0102;
pub(in crate::mapi) const PID_TAG_OUTLOOK_STORE_STATE: u32 = 0x346F_0003;
pub(in crate::mapi) const PID_TAG_PRIVATE: u32 = 0x0E5C_000B;
pub(in crate::mapi) const PID_TAG_USER_GUID: u32 = 0x6707_0102;
pub(in crate::mapi) const PID_TAG_PROHIBIT_RECEIVE_QUOTA: u32 = 0x666A_0003;
pub(in crate::mapi) const PID_TAG_MAX_SUBMIT_MESSAGE_SIZE: u32 = 0x666D_0003;
pub(in crate::mapi) const PID_TAG_PROHIBIT_SEND_QUOTA: u32 = 0x666E_0003;
pub(in crate::mapi) const PID_TAG_STORAGE_QUOTA_LIMIT: u32 = 0x3FF5_0003;
pub(in crate::mapi) const PID_TAG_PST_PATH_W: u32 = 0x6700_001F;
pub(in crate::mapi) const PID_TAG_OST_OSTID: u32 = 0x7C04_0102;
pub(in crate::mapi) const PID_TAG_SENT_MAIL_SVR_EID: u32 = 0x6740_00FB;
pub(in crate::mapi) const PID_TAG_MID: u32 = 0x674A_0014;
pub(in crate::mapi) const PID_TAG_INST_ID: u32 = 0x674D_0014;
pub(in crate::mapi) const PID_TAG_INSTANCE_NUM: u32 = 0x674E_0003;
pub(in crate::mapi) const PID_TAG_CHANGE_NUMBER: u32 = 0x67A4_0014;
pub(in crate::mapi) const PID_TAG_ASSOCIATED: u32 = 0x67AA_000B;
pub(in crate::mapi) const PID_TAG_WLINK_GROUP_HEADER_ID: u32 = 0x6842_0048;
pub(in crate::mapi) const PID_TAG_WLINK_SAVE_STAMP: u32 = 0x6847_0003;
pub(in crate::mapi) const PID_TAG_WLINK_TYPE: u32 = 0x6849_0003;
pub(in crate::mapi) const PID_TAG_WLINK_FLAGS: u32 = 0x684A_0003;
pub(in crate::mapi) const PID_TAG_WLINK_ORDINAL: u32 = 0x684B_0102;
pub(in crate::mapi) const PID_TAG_WLINK_ENTRY_ID: u32 = 0x684C_0102;
pub(in crate::mapi) const PID_TAG_WLINK_RECORD_KEY: u32 = 0x684D_0102;
pub(in crate::mapi) const PID_TAG_WLINK_STORE_ENTRY_ID: u32 = 0x684E_0102;
pub(in crate::mapi) const PID_TAG_WLINK_FOLDER_TYPE: u32 = 0x684F_0048;
pub(in crate::mapi) const PID_TAG_WLINK_GROUP_CLSID: u32 = 0x6850_0048;
pub(in crate::mapi) const PID_TAG_WLINK_GROUP_NAME_W: u32 = 0x6851_001F;
pub(in crate::mapi) const PID_TAG_WLINK_SECTION: u32 = 0x6852_0003;
pub(in crate::mapi) const PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID: u32 = 0x6891_0102;
pub(in crate::mapi) const PID_TAG_ATTACH_DATA_BINARY: u32 = 0x3701_0102;
pub(in crate::mapi) const PID_TAG_VIEW_DESCRIPTOR_CLSID: u32 = 0x6833_0048;
pub(in crate::mapi) const PID_TAG_VIEW_DESCRIPTOR_FLAGS: u32 = 0x6834_0003;
pub(in crate::mapi) const OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835: u32 = 0x6835_0102;
pub(in crate::mapi) const PID_TAG_VIEW_DESCRIPTOR_VERSION: u32 = 0x683A_0003;
pub(in crate::mapi) const PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE: u32 = 0x683E_0102;
pub(in crate::mapi) const PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE: u32 = 0x6841_0003;
pub(in crate::mapi) const PID_TAG_VIEW_DESCRIPTOR_BINARY: u32 = 0x7001_0102;
pub(in crate::mapi) const PID_TAG_ATTACH_SIZE: u32 = 0x0E20_0003;
pub(in crate::mapi) const PID_TAG_ATTACH_NUM: u32 = 0x0E21_0003;
pub(in crate::mapi) const PID_TAG_ATTACH_FILENAME_W: u32 = 0x3704_001F;
pub(in crate::mapi) const PID_TAG_ATTACH_METHOD: u32 = 0x3705_0003;
pub(in crate::mapi) const ATTACH_BY_VALUE: u32 = 1;
pub(in crate::mapi) const PID_TAG_ATTACH_LONG_FILENAME_W: u32 = 0x3707_001F;
pub(in crate::mapi) const PID_TAG_RENDERING_POSITION: u32 = 0x370B_0003;
pub(in crate::mapi) const PID_TAG_ATTACH_MIME_TAG_W: u32 = 0x370E_001F;
pub(in crate::mapi) const PID_TAG_EMAIL_ADDRESS_W: u32 = 0x3003_001F;
pub(in crate::mapi) const PID_TAG_SMTP_ADDRESS_W: u32 = 0x39FE_001F;
pub(in crate::mapi) const PID_TAG_GIVEN_NAME_W: u32 = 0x3A06_001F;
pub(in crate::mapi) const PID_TAG_BUSINESS_TELEPHONE_NUMBER_W: u32 = 0x3A08_001F;
pub(in crate::mapi) const PID_TAG_HOME_TELEPHONE_NUMBER_W: u32 = 0x3A09_001F;
pub(in crate::mapi) const PID_TAG_SURNAME_W: u32 = 0x3A11_001F;
pub(in crate::mapi) const PID_TAG_COMPANY_NAME_W: u32 = 0x3A16_001F;
pub(in crate::mapi) const PID_TAG_TITLE_W: u32 = 0x3A17_001F;
pub(in crate::mapi) const PID_TAG_MOBILE_TELEPHONE_NUMBER_W: u32 = 0x3A1C_001F;
pub(in crate::mapi) const PID_TAG_START_DATE: u32 = 0x0060_0040;
pub(in crate::mapi) const PID_TAG_END_DATE: u32 = 0x0061_0040;
pub(in crate::mapi) const PID_TAG_LOCATION_W: u32 = 0x3FFB_001F;
pub(crate) const FIRST_NAMED_PROPERTY_ID: u16 = 0x8001;
pub(crate) const MAX_NAMED_PROPERTY_ID: u16 = 0xFFFE;
pub(in crate::mapi) const PS_MAPI_GUID: [u8; 16] = [
    0x28, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];
pub(in crate::mapi) const PS_INTERNET_HEADERS_GUID: [u8; 16] = [
    0x86, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];
pub(in crate::mapi) const PSETID_COMMON_GUID: [u8; 16] = [
    0x08, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];
pub(in crate::mapi) const PS_PUBLIC_STRINGS_GUID: [u8; 16] = [
    0x29, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];
pub(in crate::mapi) const PSETID_SHARING_GUID: [u8; 16] = [
    0x40, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];
pub(in crate::mapi) const OUTLOOK_SHARING_PROVIDER_GUID: [u8; 16] = [
    0xAE, 0xF0, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];
pub(in crate::mapi) const PSETID_LOG_GUID: [u8; 16] = [
    0x0A, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];
pub(in crate::mapi) const PSETID_NOTE_GUID: [u8; 16] = [
    0x0E, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];
pub(in crate::mapi) const PSETID_TASK_GUID: [u8; 16] = [
    0x03, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];
pub(in crate::mapi) const PSETID_APPOINTMENT_GUID: [u8; 16] = [
    0x02, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];
pub(in crate::mapi) const PSETID_MEETING_GUID: [u8; 16] = [
    0x90, 0xDA, 0xD8, 0x6E, 0x0B, 0x45, 0x1B, 0x10, 0x98, 0xDA, 0x00, 0xAA, 0x00, 0x3F, 0x13, 0x05,
];
pub(in crate::mapi) const PSETID_POST_RSS_GUID: [u8; 16] = [
    0x41, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];

pub(in crate::mapi) const PID_LID_GLOBAL_OBJECT_ID: u32 = 0x0000_0003;
pub(in crate::mapi) const PID_LID_CLEAN_GLOBAL_OBJECT_ID: u32 = 0x0000_0023;
pub(in crate::mapi) const PID_LID_GLOBAL_OBJECT_ID_NAMED_ID: u16 = 0x8001;
pub(in crate::mapi) const PID_LID_CLEAN_GLOBAL_OBJECT_ID_NAMED_ID: u16 = 0x8002;
pub(in crate::mapi) const PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG:
    u32 = 0x8010_0102;
pub(in crate::mapi) const PID_LID_COMMON_START: u32 = 0x0000_8516;
pub(in crate::mapi) const PID_LID_COMMON_END: u32 = 0x0000_8517;
pub(in crate::mapi) const PID_LID_REMINDER_TIME: u32 = 0x0000_8502;
pub(in crate::mapi) const PID_LID_REMINDER_SET: u32 = 0x0000_8503;
pub(in crate::mapi) const PID_LID_REMINDER_DELTA: u32 = 0x0000_8501;
pub(in crate::mapi) const PID_LID_REMINDER_OVERRIDE: u32 = 0x0000_851C;
pub(in crate::mapi) const PID_LID_REMINDER_PLAY_SOUND: u32 = 0x0000_851E;
pub(in crate::mapi) const PID_LID_REMINDER_FILE_PARAMETER: u32 = 0x0000_851F;
pub(in crate::mapi) const PID_LID_FLAG_REQUEST: u32 = 0x0000_8530;
pub(in crate::mapi) const PID_LID_REMINDER_SIGNAL_TIME: u32 = 0x0000_8560;
pub(in crate::mapi) const PID_LID_PERCENT_COMPLETE: u32 = 0x0000_8102;
pub(in crate::mapi) const PID_LID_TASK_START_DATE: u32 = 0x0000_8104;
pub(in crate::mapi) const PID_LID_TASK_DUE_DATE: u32 = 0x0000_8105;
pub(in crate::mapi) const PID_LID_APPOINTMENT_START_WHOLE: u32 = 0x0000_820D;
pub(in crate::mapi) const PID_LID_APPOINTMENT_END_WHOLE: u32 = 0x0000_820E;
pub(in crate::mapi) const PID_LID_BUSY_STATUS: u32 = 0x0000_8205;
pub(in crate::mapi) const PID_LID_LOCATION: u32 = 0x0000_8208;
pub(in crate::mapi) const PID_LID_APPOINTMENT_DURATION: u32 = 0x0000_8213;
pub(in crate::mapi) const PID_LID_APPOINTMENT_SUB_TYPE: u32 = 0x0000_8215;
pub(in crate::mapi) const PID_LID_APPOINTMENT_RECUR: u32 = 0x0000_8216;
pub(in crate::mapi) const PID_LID_APPOINTMENT_STATE_FLAGS: u32 = 0x0000_8217;
pub(in crate::mapi) const PID_LID_ALL_ATTENDEES_STRING: u32 = 0x0000_8238;
pub(in crate::mapi) const PID_LID_TO_ATTENDEES_STRING: u32 = 0x0000_823B;
pub(in crate::mapi) const PID_LID_CC_ATTENDEES_STRING: u32 = 0x0000_823C;
pub(in crate::mapi) const PID_LID_TIME_ZONE_STRUCT: u32 = 0x0000_8233;
pub(in crate::mapi) const PID_LID_TIME_ZONE_DESCRIPTION: u32 = 0x0000_8234;
pub(in crate::mapi) const PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY: u32 = 0x0000_825E;
pub(in crate::mapi) const PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY: u32 = 0x0000_825F;
pub(in crate::mapi) const PID_LID_COMPANIES: u32 = 0x0000_8539;
pub(in crate::mapi) const PID_LID_CONTACTS: u32 = 0x0000_853A;
pub(in crate::mapi) const PID_LID_CONTACT_LINK_NAME: u32 = 0x0000_8586;
pub(in crate::mapi) const PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID: u32 = 0x0000_85C6;
pub(in crate::mapi) const PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID: u32 = 0x0000_85C7;
pub(in crate::mapi) const PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME: u32 = 0x0000_85C8;
pub(in crate::mapi) const PID_LID_CONVERSATION_PROCESSED: u32 = 0x0000_85C9;
pub(in crate::mapi) const PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME: u32 = 0x0000_85CA;
pub(in crate::mapi) const PID_LID_CONVERSATION_ACTION_VERSION: u32 = 0x0000_85CB;
pub(in crate::mapi) const PID_LID_LOG_TYPE: u32 = 0x0000_8700;
pub(in crate::mapi) const PID_LID_LOG_START: u32 = 0x0000_8706;
pub(in crate::mapi) const PID_LID_LOG_DURATION: u32 = 0x0000_8707;
pub(in crate::mapi) const PID_LID_LOG_END: u32 = 0x0000_8708;
pub(in crate::mapi) const PID_LID_LOG_FLAGS: u32 = 0x0000_870C;
pub(in crate::mapi) const PID_LID_LOG_TYPE_DESC: u32 = 0x0000_8712;
pub(in crate::mapi) const PID_LID_NOTE_COLOR: u32 = 0x0000_8B00;
pub(in crate::mapi) const PID_LID_NOTE_HEIGHT: u32 = 0x0000_8B02;
pub(in crate::mapi) const PID_LID_NOTE_WIDTH: u32 = 0x0000_8B03;
pub(in crate::mapi) const PID_LID_NOTE_X: u32 = 0x0000_8B04;
pub(in crate::mapi) const PID_LID_NOTE_Y: u32 = 0x0000_8B05;
pub(in crate::mapi) const PID_LID_POST_RSS_CHANNEL_LINK: u32 = 0x0000_8900;
pub(in crate::mapi) const PID_LID_POST_RSS_ITEM_LINK: u32 = 0x0000_8901;
pub(in crate::mapi) const PID_LID_POST_RSS_ITEM_HASH: u32 = 0x0000_8902;
pub(in crate::mapi) const PID_LID_POST_RSS_ITEM_GUID: u32 = 0x0000_8903;
pub(in crate::mapi) const PID_LID_POST_RSS_CHANNEL: u32 = 0x0000_8904;
pub(in crate::mapi) const PID_LID_POST_RSS_ITEM_XML: u32 = 0x0000_8905;
pub(in crate::mapi) const PID_LID_POST_RSS_SUBSCRIPTION: u32 = 0x0000_8906;

pub(in crate::mapi) const PID_LID_COMMON_START_TAG: u32 = 0x8516_0040;
pub(in crate::mapi) const PID_LID_COMMON_END_TAG: u32 = 0x8517_0040;
pub(in crate::mapi) const PID_LID_GLOBAL_OBJECT_ID_TAG: u32 = 0x8001_0102;
pub(in crate::mapi) const PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG: u32 = 0x8002_0102;
pub(in crate::mapi) const PID_LID_APPOINTMENT_START_WHOLE_TAG: u32 = 0x820D_0040;
pub(in crate::mapi) const PID_LID_APPOINTMENT_END_WHOLE_TAG: u32 = 0x820E_0040;
pub(in crate::mapi) const PID_LID_BUSY_STATUS_TAG: u32 = 0x8205_0003;
pub(in crate::mapi) const PID_LID_LOCATION_W_TAG: u32 = 0x8208_001F;
pub(in crate::mapi) const PID_LID_APPOINTMENT_DURATION_TAG: u32 = 0x8213_0003;
pub(in crate::mapi) const PID_LID_APPOINTMENT_SUB_TYPE_TAG: u32 = 0x8215_000B;
pub(in crate::mapi) const PID_LID_APPOINTMENT_RECUR_TAG: u32 = 0x8216_0102;
pub(in crate::mapi) const PID_LID_APPOINTMENT_STATE_FLAGS_TAG: u32 = 0x8217_0003;
pub(in crate::mapi) const PID_LID_ALL_ATTENDEES_STRING_W_TAG: u32 = 0x8238_001F;
pub(in crate::mapi) const PID_LID_TO_ATTENDEES_STRING_W_TAG: u32 = 0x823B_001F;
pub(in crate::mapi) const PID_LID_CC_ATTENDEES_STRING_W_TAG: u32 = 0x823C_001F;
pub(in crate::mapi) const PID_LID_TIME_ZONE_STRUCT_TAG: u32 = 0x8233_0102;
pub(in crate::mapi) const PID_LID_TIME_ZONE_DESCRIPTION_W_TAG: u32 = 0x8234_001F;
pub(in crate::mapi) const PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG: u32 =
    0x825E_0102;
pub(in crate::mapi) const PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG: u32 =
    0x825F_0102;
pub(in crate::mapi) const PID_LID_REMINDER_TIME_TAG: u32 = 0x8502_0040;
pub(in crate::mapi) const PID_LID_REMINDER_SET_TAG: u32 = 0x8503_000B;
pub(in crate::mapi) const PID_LID_REMINDER_DELTA_TAG: u32 = 0x8501_0003;
pub(in crate::mapi) const PID_LID_REMINDER_OVERRIDE_TAG: u32 = 0x851C_000B;
pub(in crate::mapi) const PID_LID_REMINDER_PLAY_SOUND_TAG: u32 = 0x851E_000B;
pub(in crate::mapi) const PID_LID_REMINDER_FILE_PARAMETER_W_TAG: u32 = 0x851F_001F;
pub(in crate::mapi) const PID_LID_FLAG_REQUEST_W_TAG: u32 = 0x8530_001F;
pub(in crate::mapi) const PID_LID_REMINDER_SIGNAL_TIME_TAG: u32 = 0x8560_0040;
pub(in crate::mapi) const PID_LID_PERCENT_COMPLETE_TAG: u32 = 0x8102_0005;
pub(in crate::mapi) const PID_LID_TASK_START_DATE_TAG: u32 = 0x8104_0040;
pub(in crate::mapi) const PID_LID_TASK_DUE_DATE_TAG: u32 = 0x8105_0040;
pub(in crate::mapi) const PID_LID_COMPANIES_TAG: u32 = 0x8539_101F;
pub(in crate::mapi) const PID_LID_CONTACTS_TAG: u32 = 0x853A_101F;
pub(in crate::mapi) const PID_LID_CONTACT_LINK_NAME_W_TAG: u32 = 0x8586_001F;
pub(in crate::mapi) const PID_LID_CONTACT_LINK_NAME_STRING8_TAG: u32 = 0x8586_001E;
pub(in crate::mapi) const PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG: u32 = 0x85C6_0102;
pub(in crate::mapi) const PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG: u32 = 0x85C7_0102;
pub(in crate::mapi) const PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG: u32 = 0x85C8_0040;
pub(in crate::mapi) const PID_LID_CONVERSATION_PROCESSED_TAG: u32 = 0x85C9_0003;
pub(in crate::mapi) const PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG: u32 = 0x85CA_0040;
pub(in crate::mapi) const PID_LID_CONVERSATION_ACTION_VERSION_TAG: u32 = 0x85CB_0003;
pub(in crate::mapi) const PID_NAME_KEYWORDS_TAG: u32 = 0x9000_101F;
pub(in crate::mapi) const PID_LID_LOG_TYPE_W_TAG: u32 = 0x8700_001F;
pub(in crate::mapi) const PID_LID_LOG_TYPE_STRING8_TAG: u32 = 0x8700_001E;
pub(in crate::mapi) const PID_LID_LOG_START_TAG: u32 = 0x8706_0040;
pub(in crate::mapi) const PID_LID_LOG_DURATION_TAG: u32 = 0x8707_0003;
pub(in crate::mapi) const PID_LID_LOG_END_TAG: u32 = 0x8708_0040;
pub(in crate::mapi) const PID_LID_LOG_FLAGS_TAG: u32 = 0x870C_0003;
pub(in crate::mapi) const PID_LID_LOG_TYPE_DESC_W_TAG: u32 = 0x8712_001F;
pub(in crate::mapi) const PID_LID_LOG_TYPE_DESC_STRING8_TAG: u32 = 0x8712_001E;
pub(in crate::mapi) const PID_LID_NOTE_COLOR_TAG: u32 = 0x8B00_0003;
pub(in crate::mapi) const PID_LID_NOTE_HEIGHT_TAG: u32 = 0x8B02_0003;
pub(in crate::mapi) const PID_LID_NOTE_WIDTH_TAG: u32 = 0x8B03_0003;
pub(in crate::mapi) const PID_LID_NOTE_X_TAG: u32 = 0x8B04_0003;
pub(in crate::mapi) const PID_LID_NOTE_Y_TAG: u32 = 0x8B05_0003;
pub(in crate::mapi) const PID_LID_POST_RSS_CHANNEL_LINK_W_TAG: u32 = 0x8900_001F;
pub(in crate::mapi) const PID_LID_POST_RSS_ITEM_LINK_W_TAG: u32 = 0x8901_001F;
pub(in crate::mapi) const PID_LID_POST_RSS_ITEM_HASH_TAG: u32 = 0x8902_0003;
pub(in crate::mapi) const PID_LID_POST_RSS_ITEM_GUID_W_TAG: u32 = 0x8903_001F;
pub(in crate::mapi) const PID_LID_POST_RSS_CHANNEL_W_TAG: u32 = 0x8904_001F;
pub(in crate::mapi) const PID_LID_POST_RSS_ITEM_XML_W_TAG: u32 = 0x8905_001F;
pub(in crate::mapi) const PID_LID_POST_RSS_SUBSCRIPTION_W_TAG: u32 = 0x8906_001F;

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
            (PID_LID_APPOINTMENT_SUB_TYPE, PSETID_APPOINTMENT_GUID),
            (PID_LID_APPOINTMENT_RECUR, PSETID_APPOINTMENT_GUID),
            (PID_LID_APPOINTMENT_STATE_FLAGS, PSETID_APPOINTMENT_GUID),
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
            (PID_LID_COMPANIES, PSETID_COMMON_GUID),
            (PID_LID_CONTACTS, PSETID_COMMON_GUID),
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
        0x9000,
        MapiNamedProperty {
            guid: PS_PUBLIC_STRINGS_GUID,
            kind: MapiNamedPropertyKind::Name("Keywords".to_string()),
        },
    )))
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
        PID_TAG_MAX_SUBMIT_MESSAGE_SIZE => Some(MapiValue::U32(35 * 1024)),
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

fn mailbox_owner_entry_id(principal: &AccountPrincipal) -> Vec<u8> {
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

    let mut row_count = 0usize;
    let mut rows = Vec::new();
    match object {
        Some(MapiObject::Message {
            folder_id,
            message_id,
            saved_email,
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
            let recipients = message_recipients(email);
            if start >= recipients.len() {
                return rop_error_response(0x0F, input_handle_index, 0x8004_010F);
            }
            for (offset, recipient) in recipients
                .into_iter()
                .enumerate()
                .skip(start)
                .take(u8::MAX as usize)
            {
                write_u32(&mut rows, offset as u32);
                rows.push(recipient.recipient_type);
                rows.extend_from_slice(&0x0FFFu16.to_le_bytes());
                rows.extend_from_slice(&0u16.to_le_bytes());
                let row = serialize_recipient_row(recipient.address);
                rows.extend_from_slice(&(row.len() as u16).to_le_bytes());
                rows.extend_from_slice(&row);
                row_count += 1;
            }
        }
        Some(MapiObject::PendingMessage { recipients, .. }) => {
            if start >= recipients.len() {
                return rop_error_response(0x0F, input_handle_index, 0x8004_010F);
            }
            for recipient in recipients.iter().skip(start).take(u8::MAX as usize) {
                write_u32(&mut rows, recipient.row_id);
                rows.push(recipient.recipient_type);
                rows.extend_from_slice(&0x0FFFu16.to_le_bytes());
                rows.extend_from_slice(&0u16.to_le_bytes());
                let row = serialize_pending_recipient_row(recipient);
                rows.extend_from_slice(&(row.len() as u16).to_le_bytes());
                rows.extend_from_slice(&row);
                row_count += 1;
            }
        }
        _ => return rop_error_response(0x0F, input_handle_index, 0x0000_04B9),
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
    restriction_matches(restriction, |property_tag| {
        email_property_value(email, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_contact(
    restriction: Option<&MapiRestriction>,
    contact: &AccessibleContact,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        contact_property_value(
            contact,
            mapi_item_id(&contact.id),
            CONTACTS_FOLDER_ID,
            property_tag,
        )
    })
}

pub(in crate::mapi) fn restriction_matches_event(
    restriction: Option<&MapiRestriction>,
    event: &AccessibleEvent,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        event_property_value(
            event,
            mapi_item_id(&event.id),
            CALENDAR_FOLDER_ID,
            property_tag,
        )
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
        } => value_for(*property_tag)
            .and_then(|property| property.into_text())
            .is_some_and(|property| {
                property
                    .to_ascii_lowercase()
                    .contains(&value.to_ascii_lowercase())
            }),
        MapiRestriction::Property {
            relop,
            property_tag,
            value,
        } => value_for(*property_tag)
            .is_some_and(|property| compare_mapi_values(&property, value, *relop)),
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
        PID_TAG_SUBFOLDERS => Some(MapiValue::Bool(mailbox_has_subfolders(mailbox, mailboxes))),
        PID_TAG_FOLDER_TYPE => Some(MapiValue::U32(if mailbox.role == "__mapi_search" {
            FOLDER_SEARCH
        } else {
            FOLDER_GENERIC
        })),
        PID_TAG_ACCESS | PID_TAG_RIGHTS => Some(MapiValue::U32(MAPI_FOLDER_ACCESS)),
        PID_TAG_EXTENDED_FOLDER_FLAGS => Some(MapiValue::Binary(extended_folder_flags())),
        PID_TAG_ARCHIVE_TAG | PID_TAG_POLICY_TAG => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_RETENTION_PERIOD | PID_TAG_RETENTION_FLAGS | PID_TAG_ARCHIVE_PERIOD => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_FOLDER_WEBVIEWINFO | PID_TAG_FOLDER_XVIEWINFO_E => {
            Some(MapiValue::Binary(Vec::new()))
        }
        PID_TAG_FOLDER_FORM_FLAGS | PID_TAG_FOLDER_VIEWS_ONLY | PID_TAG_FOLDER_VIEWLIST_FLAGS => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_DEFAULT_FORM_NAME_W => Some(MapiValue::String(String::new())),
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

pub(in crate::mapi) fn mapi_mailbox_display_name(mailbox: &JmapMailbox) -> String {
    if mailbox.role.eq_ignore_ascii_case("inbox") {
        "Inbox".to_string()
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
        "IPF.Contact" | "IPF.Contact.MOC.QuickContacts" => Some("IPM.Contact"),
        "IPF.Task" => Some("IPM.Task"),
        "IPF.StickyNote" => Some("IPM.StickyNote"),
        "IPF.Journal" => Some("IPM.Activity"),
        "IPF.Configuration" => Some("IPM.Configuration"),
        _ => None,
    }
}

pub(in crate::mapi) fn extended_folder_flags() -> Vec<u8> {
    vec![0x01, 0x04, 0x00, 0x00, 0x10, 0x00]
}

fn mailbox_has_subfolders(mailbox: &JmapMailbox, mailboxes: &[JmapMailbox]) -> bool {
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
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_FOLDER_ACCESS)),
        PID_TAG_CONTAINER_CLASS_W => Some(MapiValue::String(
            collaboration_folder_message_class(folder.kind).to_string(),
        )),
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => {
            default_post_message_class_for_container_class(collaboration_folder_message_class(
                folder.kind,
            ))
            .map(|message_class| MapiValue::String(message_class.to_string()))
        }
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(folder.id)),
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
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_FOLDER_ACCESS)),
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
    match property_tag {
        PID_TAG_MID => Some(MapiValue::U64(mapi_message_id(email))),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_CONVERSATION_TOPIC_W => {
            Some(MapiValue::String(email.subject.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W | PID_TAG_ORIGINAL_MESSAGE_CLASS_W => Some(MapiValue::String(
            message_class_for_email(email).to_string(),
        )),
        PID_TAG_MESSAGE_DELIVERY_TIME
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
        PID_TAG_MESSAGE_STATUS => Some(MapiValue::U32(0)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(message_flags(email))),
        PID_TAG_READ => Some(MapiValue::Bool(!email.unread)),
        PID_TAG_FLAG_STATUS => Some(MapiValue::U32(mapi_mailstore::canonical_flag_status(email))),
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
        PID_NAME_KEYWORDS_TAG => Some(MapiValue::MultiString(email.categories.clone())),
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
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I64(email.size_octets)),
        PID_TAG_SENDER_NAME_W => Some(MapiValue::String(
            email
                .from_display
                .clone()
                .unwrap_or_else(|| email.from_address.clone()),
        )),
        PID_TAG_SENDER_ADDRESS_TYPE_W => Some(MapiValue::String("SMTP".to_string())),
        PID_TAG_SENDER_EMAIL_ADDRESS_W => Some(MapiValue::String(email.from_address.clone())),
        PID_TAG_SENDER_SMTP_ADDRESS_W => Some(MapiValue::String(email.from_address.clone())),
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
        PID_TAG_SEARCH_KEY => Some(MapiValue::Binary(message_search_key(email.id))),
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
        PID_TAG_TRANSPORT_MESSAGE_HEADERS_W => Some(MapiValue::String(transport_headers(email))),
        _ => None,
    }
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
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I32(128)),
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
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(message.id & 0x00FF_FFFF_FFFF_FFFF)),
        PID_TAG_WLINK_SAVE_STAMP => Some(MapiValue::U32(wlink_save_stamp(message))),
        PID_TAG_WLINK_TYPE => Some(MapiValue::U32(message.shortcut_type)),
        PID_TAG_WLINK_FLAGS => Some(MapiValue::U32(message.flags)),
        PID_TAG_WLINK_SECTION => Some(MapiValue::U32(message.section)),
        PID_TAG_WLINK_ORDINAL => Some(MapiValue::Binary(wlink_ordinal_bytes(message.ordinal))),
        property_tag
            if property_tag_id(property_tag) == property_tag_id(PID_TAG_WLINK_GROUP_HEADER_ID)
                && message.shortcut_type == 4 =>
        {
            let group_id = message
                .group_header_id
                .map(|group_id| *group_id.as_bytes())
                .unwrap_or_else(default_wlink_group_guid);
            Some(wlink_guid_property_value(requested_property_tag, group_id))
        }
        property_tag
            if property_tag_id(property_tag) == property_tag_id(PID_TAG_WLINK_GROUP_CLSID)
                && message.shortcut_type != 4 =>
        {
            let group_id = message
                .group_header_id
                .map(|group_id| *group_id.as_bytes())
                .unwrap_or_else(default_wlink_group_guid);
            Some(wlink_guid_property_value(requested_property_tag, group_id))
        }
        PID_TAG_WLINK_GROUP_NAME_W if message.shortcut_type != 4 => {
            Some(MapiValue::String(wlink_group_name(message)))
        }
        PID_TAG_WLINK_ENTRY_ID if message.shortcut_type != 4 => message
            .target_folder_id
            .and_then(|folder_id| {
                crate::mapi::identity::folder_entry_id_from_object_id(account_id, folder_id)
            })
            .map(MapiValue::Binary),
        PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG
            if message.shortcut_type != 4 =>
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
        PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID if message.shortcut_type != 4 => Some(
            MapiValue::Binary(mapi_mailstore::private_store_entry_id(account_id)),
        ),
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
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I32(128)),
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
            Some(MapiValue::Binary(minimal_view_descriptor_binary()))
        }
        PID_TAG_VIEW_DESCRIPTOR_VERSION => Some(MapiValue::U32(message.view_type)),
        PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE => Some(MapiValue::U32(0)),
        OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B => Some(MapiValue::Binary(Vec::new())),
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

fn minimal_view_descriptor_binary() -> Vec<u8> {
    let mut value = Vec::with_capacity(96);
    value.extend_from_slice(&[0; 8]);
    value.extend_from_slice(&8u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&1u32.to_le_bytes());
    value.extend_from_slice(&u32::MAX.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&[0; 24]);
    value.extend_from_slice(&1u16.to_le_bytes());
    value.extend_from_slice(&4u16.to_le_bytes());
    value.extend_from_slice(&7u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&0x28u32.to_le_bytes());
    value.extend_from_slice(&[0; 12]);
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&4u32.to_le_bytes());
    value
}

fn property_tag_id(property_tag: u32) -> u32 {
    property_tag & 0xFFFF_0000
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

#[cfg(test)]
pub(crate) fn default_wlink_group_uuid() -> Uuid {
    Uuid::from_bytes(default_wlink_group_guid())
}

fn wlink_group_name(message: &MapiNavigationShortcutMessage) -> String {
    if message.group_name.trim().is_empty() {
        "Mail".to_string()
    } else {
        message.group_name.clone()
    }
}

fn wlink_save_stamp(message: &MapiNavigationShortcutMessage) -> u32 {
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

fn common_view_named_view_folder_type_guid() -> [u8; 16] {
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

fn wlink_ordinal_bytes(value: u32) -> Vec<u8> {
    if value <= u8::MAX as u32 {
        vec![value as u8]
    } else {
        value
            .to_be_bytes()
            .into_iter()
            .skip_while(|byte| *byte == 0)
            .collect()
    }
}

pub(in crate::mapi) fn conversation_action_property_value(
    message: &MapiConversationActionMessage,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    let action = &message.action;
    match property_tag {
        PID_TAG_MID => Some(MapiValue::U64(message.id)),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
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

fn message_search_key(message_id: Uuid) -> Vec<u8> {
    let mut value = Vec::with_capacity(23);
    value.extend_from_slice(b"LPEMSG:");
    value.extend_from_slice(message_id.as_bytes());
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
    if action.subject.trim().is_empty() {
        "Conv.Action".to_string()
    } else {
        action.subject.clone()
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
        PID_TAG_MID => Some(MapiValue::U64(item_id)),
        PID_TAG_DISPLAY_NAME_W | PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
            Some(MapiValue::String(contact.name.clone()))
        }
        PID_TAG_GIVEN_NAME_W => contact
            .name
            .split_whitespace()
            .next()
            .map(|value| MapiValue::String(value.to_string())),
        PID_TAG_SURNAME_W => contact
            .name
            .split_whitespace()
            .last()
            .filter(|value| *value != contact.name)
            .map(|value| MapiValue::String(value.to_string())),
        PID_TAG_EMAIL_ADDRESS_W | PID_TAG_SMTP_ADDRESS_W => {
            Some(MapiValue::String(contact.email.clone()))
        }
        PID_TAG_MOBILE_TELEPHONE_NUMBER_W
        | PID_TAG_BUSINESS_TELEPHONE_NUMBER_W
        | PID_TAG_HOME_TELEPHONE_NUMBER_W => Some(MapiValue::String(contact.phone.clone())),
        PID_TAG_COMPANY_NAME_W => Some(MapiValue::String(contact.team.clone())),
        PID_TAG_TITLE_W => Some(MapiValue::String(contact.role.clone())),
        PID_TAG_BODY_W => Some(MapiValue::String(contact.notes.clone())),
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Contact".to_string())),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I64(contact_size(contact))),
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
        PID_TAG_MID => Some(MapiValue::U64(item_id)),
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
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I64(event_size(event))),
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
        PID_LID_APPOINTMENT_SUB_TYPE_TAG => Some(MapiValue::Bool(event.all_day)),
        PID_LID_APPOINTMENT_STATE_FLAGS_TAG => Some(MapiValue::I32(appointment_state_flags(event))),
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
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I64(task_size(task))),
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
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I64(note_size(note))),
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
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I64(journal_entry_size(entry))),
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
            json_string_array(&entry.contacts_json)
                .into_iter()
                .next()
                .map(MapiValue::String)
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

fn json_from_mapi_multi_string(
    properties: &HashMap<u32, MapiValue>,
    tag: u32,
    existing: &str,
) -> String {
    match properties.get(&tag) {
        Some(MapiValue::MultiString(values)) => {
            serde_json::to_string(values).unwrap_or_else(|_| existing.to_string())
        }
        Some(MapiValue::String(value)) if !value.trim().is_empty() => {
            serde_json::to_string(&vec![value.clone()]).unwrap_or_else(|_| existing.to_string())
        }
        _ => existing.to_string(),
    }
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

pub(in crate::mapi) fn attachment_property_value(
    attachment: &MapiAttachment,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    match property_tag {
        PID_TAG_ATTACH_NUM => Some(MapiValue::U32(attachment.attach_num)),
        PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
            Some(MapiValue::String(attachment.file_name.clone()))
        }
        PID_TAG_ATTACH_MIME_TAG_W => Some(MapiValue::String(attachment.media_type.clone())),
        PID_TAG_ATTACH_SIZE => Some(MapiValue::U64(attachment.size_octets)),
        PID_TAG_ATTACH_METHOD => Some(MapiValue::U32(ATTACH_BY_VALUE)),
        PID_TAG_RENDERING_POSITION => Some(MapiValue::U32(u32::MAX)),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            attachment.file_reference.as_bytes().to_vec(),
        )),
        _ => None,
    }
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
    compare_ordering(left.cmp_value(right), relop)
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
    session: &MapiSession,
    input_handle: u32,
    property_tag: u32,
    open_mode: u8,
    mailboxes: &[JmapMailbox],
    mailbox_guid: Uuid,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<(Vec<u8>, Option<StreamWriteTarget>)> {
    if open_mode != 0 {
        return None;
    }
    let value = match session.handles.get(&input_handle)? {
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
        _ => return None,
    };
    let stream = match value {
        Some(value) => mapi_value_stream_bytes(property_tag, value)?,
        None => empty_stream_bytes_for_property_tag(property_tag)?,
    };
    Some((stream, None))
}

fn mapi_value_stream_bytes(property_tag: u32, value: MapiValue) -> Option<Vec<u8>> {
    match value {
        MapiValue::Binary(value) => Some(value),
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
    let name = optional_pending_text_property(
        properties,
        &[
            PID_TAG_DISPLAY_NAME_W,
            PID_TAG_SUBJECT_W,
            PID_TAG_NORMALIZED_SUBJECT_W,
        ],
    )
    .or_else(|| {
        let given = optional_pending_text_property(properties, &[PID_TAG_GIVEN_NAME_W]);
        let surname = optional_pending_text_property(properties, &[PID_TAG_SURNAME_W]);
        match (given, surname) {
            (Some(given), Some(surname)) => Some(format!("{given} {surname}")),
            (Some(given), None) => Some(given),
            (None, Some(surname)) => Some(surname),
            (None, None) => None,
        }
    })
    .unwrap_or_else(|| existing.name.clone());
    UpsertClientContactInput {
        id,
        account_id,
        name,
        role: optional_pending_text_property(properties, &[PID_TAG_TITLE_W])
            .unwrap_or_else(|| existing.role.clone()),
        email: optional_pending_text_property(
            properties,
            &[PID_TAG_SMTP_ADDRESS_W, PID_TAG_EMAIL_ADDRESS_W],
        )
        .unwrap_or_else(|| existing.email.clone()),
        phone: optional_pending_text_property(
            properties,
            &[
                PID_TAG_MOBILE_TELEPHONE_NUMBER_W,
                PID_TAG_BUSINESS_TELEPHONE_NUMBER_W,
                PID_TAG_HOME_TELEPHONE_NUMBER_W,
            ],
        )
        .unwrap_or_else(|| existing.phone.clone()),
        team: optional_pending_text_property(properties, &[PID_TAG_COMPANY_NAME_W])
            .unwrap_or_else(|| existing.team.clone()),
        notes: optional_pending_text_property(properties, &[PID_TAG_BODY_W])
            .unwrap_or_else(|| existing.notes.clone()),
    }
}

fn reject_unsupported_mapi_contact_properties(properties: &HashMap<u32, MapiValue>) -> Result<()> {
    for tag in properties.keys() {
        let supported = matches!(
            canonical_property_storage_tag(*tag),
            PID_TAG_DISPLAY_NAME_W
                | PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_GIVEN_NAME_W
                | PID_TAG_SURNAME_W
                | PID_TAG_TITLE_W
                | PID_TAG_SMTP_ADDRESS_W
                | PID_TAG_EMAIL_ADDRESS_W
                | PID_TAG_MOBILE_TELEPHONE_NUMBER_W
                | PID_TAG_BUSINESS_TELEPHONE_NUMBER_W
                | PID_TAG_HOME_TELEPHONE_NUMBER_W
                | PID_TAG_COMPANY_NAME_W
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
        contacts_json: json_from_mapi_multi_string(
            properties,
            PID_LID_CONTACTS_TAG,
            &existing.contacts_json,
        ),
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
    let days = days_from_civil(year, month, day) - days_from_civil(1601, 1, 1);
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
        Ok(month)
    } else {
        bail!("unsupported MAPI yearly recurrence month")
    }
}

fn recurrence_datetime_string(minutes_since_1601: u32) -> Result<String> {
    let date = recurrence_date_string(minutes_since_1601)?;
    let minutes = minutes_since_1601 % 1440;
    Ok(format!("{date}T{:02}:{:02}:00", minutes / 60, minutes % 60))
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    ((y + i64::from(m <= 2)) as i32, m as u32, d as u32)
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
    AttachmentUploadInput {
        file_name: pending_attachment_file_name(attach_num, properties),
        media_type: pending_attachment_media_type(properties),
        disposition: Some("attachment".to_string()),
        content_id: None,
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
        attachments: Vec::new(),
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
            PID_TAG_SOURCE_KEY => {}
            _ => return Err(anyhow!("canonical MAPI message property is not mutable")),
        }
    }

    if update.unread.is_none()
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
    {
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

pub(in crate::mapi) fn write_mapi_value(row: &mut Vec<u8>, property_tag: u32, value: &MapiValue) {
    match MapiPropertyTag::new(property_tag).property_type() {
        Some(MapiPropertyType::Integer16) => write_u16(
            row,
            value
                .clone()
                .into_u32()
                .and_then(|value| u16::try_from(value).ok())
                .unwrap_or_default(),
        ),
        Some(MapiPropertyType::Integer32) => {
            write_u32(row, value.clone().into_u32().unwrap_or_default())
        }
        Some(MapiPropertyType::Floating32) => {
            let value = match value {
                MapiValue::F64(value) if f64::from_bits(*value).is_finite() => {
                    f64::from_bits(*value) as f32
                }
                _ => 0.0,
            };
            row.extend_from_slice(&value.to_le_bytes());
        }
        Some(MapiPropertyType::Floating64) => {
            let value = match value {
                MapiValue::F64(value) if f64::from_bits(*value).is_finite() => {
                    f64::from_bits(*value)
                }
                _ => 0.0,
            };
            row.extend_from_slice(&value.to_le_bytes());
        }
        Some(MapiPropertyType::Error) => {
            write_u32(row, value.clone().into_u32().unwrap_or(0x8004_0102))
        }
        Some(MapiPropertyType::Boolean) => row.push(value.as_bool().unwrap_or_default() as u8),
        Some(MapiPropertyType::Integer64) | Some(MapiPropertyType::Time) => {
            let value = value.as_i64().unwrap_or_default().max(0) as u64;
            match property_tag {
                PID_TAG_FOLDER_ID | PID_TAG_PARENT_FOLDER_ID | PID_TAG_MID => {
                    write_object_id(row, value)
                }
                _ => write_u64(row, value),
            }
        }
        Some(MapiPropertyType::String8) => {
            write_ascii_z(row, &value.clone().into_text().unwrap_or_default())
        }
        Some(MapiPropertyType::String) => {
            write_utf16z(row, &value.clone().into_text().unwrap_or_default())
        }
        Some(MapiPropertyType::Guid) => match value {
            MapiValue::Guid(guid) => row.extend_from_slice(guid),
            _ => row.extend_from_slice(Uuid::nil().as_bytes()),
        },
        Some(MapiPropertyType::ServerId | MapiPropertyType::Binary) => match value {
            MapiValue::Binary(bytes) => write_rop_binary(row, bytes),
            _ => write_rop_binary(row, &[]),
        },
        Some(MapiPropertyType::MultipleInteger16) => match value {
            MapiValue::MultiI16(values) => write_multi_i16(row, values),
            _ => write_u32(row, 0),
        },
        Some(MapiPropertyType::MultipleInteger32) => match value {
            MapiValue::MultiI32(values) => write_multi_i32(row, values),
            _ => write_u32(row, 0),
        },
        Some(MapiPropertyType::MultipleInteger64) => match value {
            MapiValue::MultiI64(values) => write_multi_i64(row, values),
            _ => write_u32(row, 0),
        },
        Some(MapiPropertyType::MultipleString8) => match value {
            MapiValue::MultiString(values) => write_multi_string8(row, values),
            _ => write_u32(row, 0),
        },
        Some(MapiPropertyType::MultipleString) => match value {
            MapiValue::MultiString(values) => write_multi_string(row, values),
            _ => write_u32(row, 0),
        },
        Some(MapiPropertyType::MultipleGuid) => match value {
            MapiValue::MultiGuid(values) => write_multi_guid(row, values),
            _ => write_u32(row, 0),
        },
        Some(MapiPropertyType::MultipleBinary) => match value {
            MapiValue::MultiBinary(values) => write_multi_binary(row, values),
            _ => write_u32(row, 0),
        },
        None => write_property_default(row, property_tag),
    }
}

pub(in crate::mapi) fn parse_mapi_property_value(
    cursor: &mut Cursor<'_>,
    property_tag: u32,
) -> Result<MapiValue> {
    match MapiPropertyTag::new(property_tag).property_type() {
        Some(MapiPropertyType::Integer16) => Ok(MapiValue::I16(cursor.read_u16()? as i16)),
        Some(MapiPropertyType::Integer32) => Ok(MapiValue::I32(cursor.read_i32()?)),
        Some(MapiPropertyType::Floating32) => {
            let bytes = cursor.read_bytes(4)?;
            Ok(MapiValue::F64(
                (f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as f64).to_bits(),
            ))
        }
        Some(MapiPropertyType::Floating64) => {
            let bytes = cursor.read_bytes(8)?;
            Ok(MapiValue::F64(
                f64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ])
                .to_bits(),
            ))
        }
        Some(MapiPropertyType::Error) => Ok(MapiValue::Error(cursor.read_u32()?)),
        Some(MapiPropertyType::Boolean) => Ok(MapiValue::Bool(cursor.read_u8()? != 0)),
        Some(MapiPropertyType::Integer64) | Some(MapiPropertyType::Time) => {
            Ok(MapiValue::I64(cursor.read_i64()?))
        }
        Some(MapiPropertyType::String8) => Ok(MapiValue::String(cursor.read_ascii_z()?)),
        Some(MapiPropertyType::String) => Ok(MapiValue::String(cursor.read_utf16z()?)),
        Some(MapiPropertyType::Guid) => {
            let guid = cursor
                .read_bytes(16)?
                .try_into()
                .map_err(|_| anyhow!("invalid MAPI GUID property value"))?;
            Ok(MapiValue::Guid(guid))
        }
        Some(MapiPropertyType::ServerId | MapiPropertyType::Binary) => {
            let len = cursor.read_u16()? as usize;
            Ok(MapiValue::Binary(cursor.read_bytes(len)?.to_vec()))
        }
        Some(MapiPropertyType::MultipleInteger16) => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::new();
            for _ in 0..count {
                values.push(cursor.read_u16()? as i16);
            }
            Ok(MapiValue::MultiI16(values))
        }
        Some(MapiPropertyType::MultipleInteger32) => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::new();
            for _ in 0..count {
                values.push(cursor.read_i32()?);
            }
            Ok(MapiValue::MultiI32(values))
        }
        Some(MapiPropertyType::MultipleInteger64) => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::new();
            for _ in 0..count {
                values.push(cursor.read_i64()?);
            }
            Ok(MapiValue::MultiI64(values))
        }
        Some(MapiPropertyType::MultipleString8) => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::new();
            for _ in 0..count {
                values.push(cursor.read_ascii_z()?);
            }
            Ok(MapiValue::MultiString(values))
        }
        Some(MapiPropertyType::MultipleString) => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::new();
            for _ in 0..count {
                values.push(cursor.read_utf16z()?);
            }
            Ok(MapiValue::MultiString(values))
        }
        Some(MapiPropertyType::MultipleGuid) => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::new();
            for _ in 0..count {
                let guid = cursor
                    .read_bytes(16)?
                    .try_into()
                    .map_err(|_| anyhow!("invalid MAPI multivalue GUID property value"))?;
                values.push(guid);
            }
            Ok(MapiValue::MultiGuid(values))
        }
        Some(MapiPropertyType::MultipleBinary) => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::new();
            for _ in 0..count {
                let len = cursor.read_u16()? as usize;
                values.push(cursor.read_bytes(len)?.to_vec());
            }
            Ok(MapiValue::MultiBinary(values))
        }
        None => {
            let tag = MapiPropertyTag::new(property_tag);
            let known_unsupported_name =
                MapiPropertyType::known_unsupported_name(tag.property_type_code());
            tracing::warn!(
                adapter = "mapi",
                enum_name = "MapiPropertyType",
                raw_value = tag.property_type_code(),
                property_id = tag.property_id(),
                known_unsupported = known_unsupported_name.is_some(),
                known_unsupported_name = known_unsupported_name.unwrap_or(""),
                "unsupported MAPI property type rejected at parser boundary"
            );
            Err(anyhow!(
                "unsupported MAPI property type {:#06X} for property id {:#06X}",
                tag.property_type_code(),
                tag.property_id()
            ))
        }
    }
}

pub(in crate::mapi) fn write_ascii_z(row: &mut Vec<u8>, value: &str) {
    row.extend(
        value
            .bytes()
            .map(|byte| if byte.is_ascii() { byte } else { b'?' }),
    );
    row.push(0);
}

pub(in crate::mapi) fn write_rop_binary(row: &mut Vec<u8>, value: &[u8]) {
    let len = value.len().min(u16::MAX as usize);
    write_u16(row, len as u16);
    row.extend_from_slice(&value[..len]);
}

pub(in crate::mapi) fn write_multi_i16(row: &mut Vec<u8>, values: &[i16]) {
    write_u32(row, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        write_u16(row, *value as u16);
    }
}

pub(in crate::mapi) fn write_multi_i32(row: &mut Vec<u8>, values: &[i32]) {
    write_u32(row, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        row.extend_from_slice(&value.to_le_bytes());
    }
}

pub(in crate::mapi) fn write_multi_i64(row: &mut Vec<u8>, values: &[i64]) {
    write_u32(row, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        row.extend_from_slice(&value.to_le_bytes());
    }
}

pub(in crate::mapi) fn write_multi_string8(row: &mut Vec<u8>, values: &[String]) {
    write_u32(row, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        write_ascii_z(row, value);
    }
}

pub(in crate::mapi) fn write_multi_string(row: &mut Vec<u8>, values: &[String]) {
    write_u32(row, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        write_utf16z(row, value);
    }
}

pub(in crate::mapi) fn write_multi_guid(row: &mut Vec<u8>, values: &[[u8; 16]]) {
    write_u32(row, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        row.extend_from_slice(value);
    }
}

pub(in crate::mapi) fn write_multi_binary(row: &mut Vec<u8>, values: &[Vec<u8>]) {
    write_u32(row, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        write_rop_binary(row, value);
    }
}

pub(in crate::mapi) fn write_named_property(row: &mut Vec<u8>, property: &MapiNamedProperty) {
    match &property.kind {
        MapiNamedPropertyKind::Lid(lid) => {
            row.push(0x00);
            row.extend_from_slice(&property.guid);
            write_u32(row, *lid);
        }
        MapiNamedPropertyKind::Name(name) => {
            row.push(0x01);
            row.extend_from_slice(&property.guid);
            let mut name_bytes = name
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect::<Vec<_>>();
            name_bytes.extend_from_slice(&0u16.to_le_bytes());
            let size = name_bytes.len().min(u8::MAX as usize);
            row.push(size as u8);
            row.extend_from_slice(&name_bytes[..size]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapi_store::{
        MapiCollaborationFolder, MapiCollaborationFolderKind, MapiPublicFolder,
    };
    use lpe_storage::{
        CollaborationCollection, CollaborationRights, PublicFolder, PublicFolderRights,
    };

    fn mailbox(id: &str, parent_id: Option<Uuid>, role: &str, name: &str) -> JmapMailbox {
        JmapMailbox {
            id: Uuid::parse_str(id).unwrap(),
            parent_id,
            role: role.to_string(),
            name: name.to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            is_subscribed: true,
        }
    }

    fn utf16z(value: &str) -> Vec<u8> {
        value
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .chain([0, 0])
            .collect()
    }

    fn valid_swapped_todo_data() -> Vec<u8> {
        let mut value = vec![0; SWAPPED_TODO_DATA_LEN];
        value[0..4].copy_from_slice(&SWAPPED_TODO_DATA_VERSION.to_le_bytes());
        let flags = SWAPPED_TODO_FLAG_TODO_ITEM
            | SWAPPED_TODO_FLAG_FLAG_TO
            | SWAPPED_TODO_FLAG_START_DATE
            | SWAPPED_TODO_FLAG_DUE_DATE
            | SWAPPED_TODO_FLAG_REMINDER
            | SWAPPED_TODO_FLAG_REMINDER_SET;
        value[4..8].copy_from_slice(&flags.to_le_bytes());
        value[8..12].copy_from_slice(&8u32.to_le_bytes());
        for (index, unit) in "Follow up".encode_utf16().enumerate() {
            let offset = 12 + index * 2;
            value[offset..offset + 2].copy_from_slice(&unit.to_le_bytes());
        }
        value[524..528].copy_from_slice(&1_000_000u32.to_le_bytes());
        value[528..532].copy_from_slice(&1_001_440u32.to_le_bytes());
        value[532..536].copy_from_slice(&1_000_030u32.to_le_bytes());
        value[536..540].copy_from_slice(&1u32.to_le_bytes());
        value
    }

    #[test]
    fn pending_html_only_message_derives_plain_body_for_save_and_submit() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "sender@example.test".to_string(),
            display_name: "Sender".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let mailbox = mailbox(
            "11111111-1111-4111-8111-111111111111",
            None,
            "drafts",
            "Drafts",
        );
        let mut properties = HashMap::new();
        properties.insert(
            PID_TAG_SUBJECT_W,
            MapiValue::String("HTML draft".to_string()),
        );
        properties.insert(
            PID_TAG_HTML_BINARY,
            MapiValue::Binary(b"<html><body>Hello<br>World &amp; team</body></html>".to_vec()),
        );
        let recipients = vec![PendingRecipient {
            row_id: 1,
            address: "to@example.test".to_string(),
            display_name: Some("To".to_string()),
            recipient_type: 0x01,
        }];

        let imported =
            jmap_import_from_pending_message(&principal, &mailbox, &properties, &recipients);
        assert_eq!(imported.body_text, "Hello\nWorld & team");
        assert_eq!(
            imported.body_html_sanitized.as_deref(),
            Some("<html><body>Hello<br>World &amp; team</body></html>")
        );
        assert_eq!(imported.size_octets, "HTML draft".len() as i64 + 18);

        let submitted = mapi_submit_from_pending_message(&principal, &properties, &recipients);
        assert_eq!(submitted.body_text, "Hello\nWorld & team");
        assert_eq!(
            submitted.body_html_sanitized.as_deref(),
            Some("<html><body>Hello<br>World &amp; team</body></html>")
        );
        assert_eq!(submitted.size_octets, "HTML draft".len() as i64 + 18);
    }

    #[test]
    fn read_recipients_success_response_includes_row_count() {
        let request = RopRequest {
            rop_id: 0x0F,
            input_handle_index: Some(2),
            output_handle_index: None,
            payload: 0u32.to_le_bytes().to_vec(),
        };
        let object = MapiObject::PendingMessage {
            folder_id: DRAFTS_FOLDER_ID,
            properties: HashMap::new(),
            recipients: vec![
                PendingRecipient {
                    row_id: 0,
                    address: "bob@example.test".to_string(),
                    display_name: Some("Bob".to_string()),
                    recipient_type: 0x01,
                },
                PendingRecipient {
                    row_id: 1,
                    address: "carol@example.test".to_string(),
                    display_name: Some("Carol".to_string()),
                    recipient_type: 0x02,
                },
            ],
        };

        let response = rop_read_recipients_response(
            &request,
            Some(&object),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(&response[..7], &[0x0F, 0x02, 0, 0, 0, 0, 2]);
        assert_eq!(u32::from_le_bytes(response[7..11].try_into().unwrap()), 0);
        assert_eq!(response[11], 0x01);
        assert!(response
            .windows(utf16z("Bob").len())
            .any(|window| window == utf16z("Bob").as_slice()));
        assert!(response
            .windows(utf16z("Carol").len())
            .any(|window| window == utf16z("Carol").as_slice()));
    }

    #[test]
    fn associated_fai_identity_properties_do_not_reuse_source_key_for_change_keys() {
        let shortcut_id = crate::mapi::identity::mapi_store_id(91);
        let shortcut = MapiNavigationShortcutMessage {
            id: shortcut_id,
            folder_id: COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x9191),
            subject: "Inbox".to_string(),
            target_folder_id: Some(INBOX_FOLDER_ID),
            shortcut_type: 1,
            flags: 0,
            section: 0,
            ordinal: 0,
            group_header_id: None,
            group_name: String::new(),
        };
        let source_key =
            navigation_shortcut_property_value(&shortcut, Uuid::nil(), PID_TAG_SOURCE_KEY);
        let change_key =
            navigation_shortcut_property_value(&shortcut, Uuid::nil(), PID_TAG_CHANGE_KEY);
        let predecessor = navigation_shortcut_property_value(
            &shortcut,
            Uuid::nil(),
            PID_TAG_PREDECESSOR_CHANGE_LIST,
        );

        assert_eq!(
            source_key,
            Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
                shortcut_id
            )))
        );
        assert_eq!(
            change_key,
            Some(MapiValue::Binary(
                mapi_mailstore::change_key_for_change_number(
                    mapi_mailstore::change_number_for_store_id(shortcut_id)
                )
            ))
        );
        assert_eq!(
            predecessor,
            Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
                mapi_mailstore::change_number_for_store_id(shortcut_id)
            )))
        );
        assert_ne!(source_key, predecessor);

        let action_id = crate::mapi::identity::mapi_store_id(92);
        let action = MapiConversationActionMessage {
            id: action_id,
            folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x9292),
            action: lpe_storage::ConversationAction {
                id: Uuid::from_u128(0x9292),
                conversation_id: Uuid::from_u128(0xabab),
                subject: "Conv.Action".to_string(),
                move_folder_entry_id: None,
                move_store_entry_id: None,
                move_target_mailbox_id: None,
                categories_json: "[]".to_string(),
                max_delivery_time: None,
                last_applied_time: None,
                version: 1,
                processed: 0,
                created_at: "2026-05-30T00:00:00Z".to_string(),
                updated_at: "2026-05-30T00:00:00Z".to_string(),
            },
        };
        let source_key = conversation_action_property_value(&action, PID_TAG_SOURCE_KEY);
        let change_key = conversation_action_property_value(&action, PID_TAG_CHANGE_KEY);
        let predecessor =
            conversation_action_property_value(&action, PID_TAG_PREDECESSOR_CHANGE_LIST);
        assert_eq!(
            change_key,
            Some(MapiValue::Binary(
                mapi_mailstore::change_key_for_change_number(
                    mapi_mailstore::change_number_for_store_id(action_id)
                )
            ))
        );
        assert_eq!(
            predecessor,
            Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
                mapi_mailstore::change_number_for_store_id(action_id)
            )))
        );
        assert_ne!(source_key, predecessor);
    }

    fn round_trip(property_tag: u32, value: &MapiValue) -> MapiValue {
        let mut encoded = Vec::new();
        write_mapi_value(&mut encoded, property_tag, value);
        parse_mapi_property_value(&mut Cursor::new(&encoded), property_tag).unwrap()
    }

    #[test]
    fn property_tag_splits_id_type_and_named_range() {
        let tag = MapiPropertyTag::new(PID_TAG_SUBJECT_W);

        assert_eq!(tag.property_id(), 0x0037);
        assert_eq!(tag.property_type_code(), 0x001F);
        assert_eq!(tag.property_type(), Some(MapiPropertyType::String));
        assert!(MapiPropertyTag::new(0x8001_001F).property_id() >= FIRST_NAMED_PROPERTY_ID);
    }

    #[test]
    fn mailbox_properties_report_real_subfolder_state() {
        let parent = mailbox("11111111-1111-1111-1111-111111111111", None, "", "Parent");
        let child = mailbox(
            "22222222-2222-2222-2222-222222222222",
            Some(parent.id),
            "",
            "Child",
        );
        crate::mapi::identity::remember_mapi_identity(
            parent.id,
            crate::mapi::identity::mapi_store_id(0x1001),
        );
        crate::mapi::identity::remember_mapi_identity(
            child.id,
            crate::mapi::identity::mapi_store_id(0x1002),
        );
        let mailboxes = vec![parent.clone(), child.clone()];

        assert_eq!(
            mailbox_property_value_with_context(&parent, &mailboxes, PID_TAG_SUBFOLDERS),
            Some(MapiValue::Bool(true))
        );
        assert_eq!(
            mailbox_property_value_with_context(&child, &mailboxes, PID_TAG_SUBFOLDERS),
            Some(MapiValue::Bool(false))
        );
    }

    #[test]
    fn folder_properties_report_deleted_count_total() {
        let mailbox = mailbox(
            "55555555-5555-5555-5555-555555555555",
            None,
            "inbox",
            "Inbox",
        );
        let collection = MapiCollaborationFolder {
            id: CALENDAR_FOLDER_ID,
            kind: MapiCollaborationFolderKind::Calendar,
            collection: CollaborationCollection {
                id: "calendar-default".to_string(),
                kind: "calendar".to_string(),
                owner_account_id: Uuid::nil(),
                owner_email: "alice@example.test".to_string(),
                owner_display_name: "Alice".to_string(),
                display_name: "Calendar".to_string(),
                is_owned: true,
                rights: CollaborationRights {
                    may_read: true,
                    may_write: true,
                    may_delete: true,
                    may_share: true,
                },
            },
            item_count: 0,
        };

        assert_eq!(
            mailbox_property_value_with_context(
                &mailbox,
                std::slice::from_ref(&mailbox),
                PID_TAG_DELETED_COUNT_TOTAL
            ),
            Some(MapiValue::U32(0))
        );
        assert_eq!(
            collaboration_folder_property_value(&collection, PID_TAG_DELETED_COUNT_TOTAL),
            Some(MapiValue::U32(0))
        );
    }

    #[test]
    fn collaboration_folder_projects_default_post_message_class_for_contacts() {
        let collection = MapiCollaborationFolder {
            id: CONTACTS_FOLDER_ID,
            kind: MapiCollaborationFolderKind::Contacts,
            collection: CollaborationCollection {
                id: "contacts-default".to_string(),
                kind: "contacts".to_string(),
                owner_account_id: Uuid::nil(),
                owner_email: "alice@example.test".to_string(),
                owner_display_name: "Alice".to_string(),
                display_name: "Contacts".to_string(),
                is_owned: true,
                rights: CollaborationRights {
                    may_read: true,
                    may_write: true,
                    may_delete: true,
                    may_share: true,
                },
            },
            item_count: 0,
        };

        assert_eq!(
            collaboration_folder_property_value(&collection, PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
            Some(MapiValue::String("IPM.Contact".to_string()))
        );
        assert_eq!(
            collaboration_folder_property_value(
                &collection,
                PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8
            ),
            Some(MapiValue::String("IPM.Contact".to_string()))
        );
    }

    #[test]
    fn public_folder_projects_default_post_message_class_from_folder_class() {
        let folder = MapiPublicFolder {
            id: PUBLIC_FOLDERS_ROOT_FOLDER_ID + 0x10000,
            folder: PublicFolder {
                id: Uuid::from_u128(1),
                tree_id: Uuid::from_u128(2),
                parent_folder_id: None,
                canonical_id: Uuid::from_u128(3),
                display_name: "Public Contacts".to_string(),
                folder_class: "IPF.Contact".to_string(),
                path: "/Public Contacts".to_string(),
                sort_order: 0,
                lifecycle_state: "active".to_string(),
                change_counter: 1,
                rights: PublicFolderRights {
                    may_read: true,
                    may_write: true,
                    may_delete: true,
                    may_share: true,
                },
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-01T00:00:00Z".to_string(),
            },
            item_count: 0,
            child_count: 0,
        };

        assert_eq!(
            public_folder_property_value(&folder, PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
            Some(MapiValue::String("IPM.Contact".to_string()))
        );
        assert_eq!(
            public_folder_property_value(&folder, PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8),
            Some(MapiValue::String("IPM.Contact".to_string()))
        );
    }

    #[test]
    fn note_derived_folder_classes_project_default_post_message_class() {
        assert_eq!(
            default_post_message_class_for_container_class("IPF.Note.OutlookHomepage"),
            Some("IPM.Note")
        );
    }

    #[test]
    fn mailbox_parent_source_key_uses_real_parent_when_context_is_available() {
        let parent = mailbox("33333333-3333-3333-3333-333333333333", None, "", "Parent");
        let child = mailbox(
            "44444444-4444-4444-4444-444444444444",
            Some(parent.id),
            "",
            "Child",
        );
        crate::mapi::identity::remember_mapi_identity(
            parent.id,
            crate::mapi::identity::mapi_store_id(0x1003),
        );
        crate::mapi::identity::remember_mapi_identity(
            child.id,
            crate::mapi::identity::mapi_store_id(0x1004),
        );
        let mailboxes = vec![parent.clone(), child.clone()];

        assert_eq!(
            mailbox_property_value_with_context(&child, &mailboxes, PID_TAG_PARENT_SOURCE_KEY),
            Some(MapiValue::Binary(
                mapi_mailstore::source_key_for_mailbox_folder(&parent)
            ))
        );
        assert_eq!(
            mailbox_property_value_with_context(&parent, &mailboxes, PID_TAG_PARENT_SOURCE_KEY),
            Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
                IPM_SUBTREE_FOLDER_ID
            )))
        );
    }

    #[test]
    fn mailbox_parent_source_key_keeps_root_level_search_specials_under_root() {
        for (role, expected_folder_id) in [
            ("reminders", REMINDERS_FOLDER_ID),
            ("todo_search", TODO_SEARCH_FOLDER_ID),
            ("tracked_mail_processing", TRACKED_MAIL_PROCESSING_FOLDER_ID),
        ] {
            let mailbox = mailbox("55555555-5555-4555-9555-555555555555", None, role, role);

            assert_eq!(mapi_folder_id(&mailbox), expected_folder_id);
            assert_eq!(
                mailbox_property_value_with_context(
                    &mailbox,
                    std::slice::from_ref(&mailbox),
                    PID_TAG_PARENT_SOURCE_KEY
                ),
                Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
                    ROOT_FOLDER_ID
                )))
            );
        }
    }

    #[test]
    fn swapped_todo_data_parser_accepts_documented_layout() {
        let parsed = parse_swapped_todo_data(&valid_swapped_todo_data()).unwrap();
        assert_eq!(parsed.todo_item_flags, Some(8));
        assert_eq!(parsed.flag_request.as_deref(), Some("Follow up"));
        assert_eq!(parsed.start_minutes, Some(1_000_000));
        assert_eq!(parsed.due_minutes, Some(1_001_440));
        assert_eq!(parsed.reminder_minutes, Some(1_000_030));
        assert_eq!(parsed.reminder_set, Some(true));
    }

    #[test]
    fn swapped_todo_data_parser_rejects_placeholder_bytes() {
        assert!(parse_swapped_todo_data(&[1, 2, 3, 4]).is_err());
        let mut unsupported = valid_swapped_todo_data();
        unsupported[0..4].copy_from_slice(&2u32.to_le_bytes());
        assert!(parse_swapped_todo_data(&unsupported).is_err());
        let mut unknown_flags = valid_swapped_todo_data();
        unknown_flags[4..8].copy_from_slice(&0x8000_0000u32.to_le_bytes());
        assert!(parse_swapped_todo_data(&unknown_flags).is_err());
    }

    #[test]
    fn special_folder_identification_properties_project_store_folder_ids() {
        let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        assert_eq!(PID_TAG_VALID_FOLDER_MASK, 0x35DF_0003);
        assert_eq!(PID_TAG_IPM_SUBTREE_ENTRY_ID, 0x35E0_0102);
        assert_eq!(PID_TAG_IPM_OUTBOX_ENTRY_ID, 0x35E2_0102);
        assert_eq!(PID_TAG_IPM_WASTEBASKET_ENTRY_ID, 0x35E3_0102);
        assert_eq!(PID_TAG_IPM_SENTMAIL_ENTRY_ID, 0x35E4_0102);
        assert_eq!(PID_TAG_VIEWS_ENTRY_ID, 0x35E5_0102);
        assert_eq!(PID_TAG_COMMON_VIEWS_ENTRY_ID, 0x35E6_0102);
        assert_eq!(PID_TAG_FINDER_ENTRY_ID, 0x35E7_0102);
        assert_eq!(PID_TAG_IPM_ARCHIVE_ENTRY_ID, 0x35FF_0102);
        assert_eq!(PID_TAG_IPM_APPOINTMENT_ENTRY_ID, 0x36D0_0102);
        assert_eq!(PID_TAG_IPM_CONTACT_ENTRY_ID, 0x36D1_0102);
        assert_eq!(PID_TAG_IPM_JOURNAL_ENTRY_ID, 0x36D2_0102);
        assert_eq!(PID_TAG_IPM_NOTE_ENTRY_ID, 0x36D3_0102);
        assert_eq!(PID_TAG_IPM_TASK_ENTRY_ID, 0x36D4_0102);
        assert_eq!(PID_TAG_REM_ONLINE_ENTRY_ID, 0x36D5_0102);
        assert_eq!(PID_TAG_REM_OFFLINE_ENTRY_ID, 0x36D6_0102);
        assert_eq!(PID_TAG_IPM_DRAFTS_ENTRY_ID, 0x36D7_0102);
        assert_eq!(PID_TAG_FREE_BUSY_ENTRY_IDS, 0x36E4_1102);

        assert_eq!(
            special_folder_identification_property_value(Uuid::nil(), PID_TAG_VALID_FOLDER_MASK),
            Some(MapiValue::U32(0xFF))
        );

        for (property_tag, folder_id) in [
            (PID_TAG_IPM_SUBTREE_ENTRY_ID, IPM_SUBTREE_FOLDER_ID),
            (PID_TAG_IPM_OUTBOX_ENTRY_ID, OUTBOX_FOLDER_ID),
            (PID_TAG_IPM_WASTEBASKET_ENTRY_ID, TRASH_FOLDER_ID),
            (PID_TAG_IPM_SENTMAIL_ENTRY_ID, SENT_FOLDER_ID),
            (PID_TAG_VIEWS_ENTRY_ID, VIEWS_FOLDER_ID),
            (PID_TAG_COMMON_VIEWS_ENTRY_ID, COMMON_VIEWS_FOLDER_ID),
            (PID_TAG_FINDER_ENTRY_ID, SEARCH_FOLDER_ID),
            (PID_TAG_IPM_ARCHIVE_ENTRY_ID, ARCHIVE_FOLDER_ID),
            (PID_TAG_IPM_APPOINTMENT_ENTRY_ID, CALENDAR_FOLDER_ID),
            (PID_TAG_IPM_CONTACT_ENTRY_ID, CONTACTS_FOLDER_ID),
            (PID_TAG_IPM_JOURNAL_ENTRY_ID, JOURNAL_FOLDER_ID),
            (PID_TAG_IPM_NOTE_ENTRY_ID, NOTES_FOLDER_ID),
            (PID_TAG_IPM_TASK_ENTRY_ID, TASKS_FOLDER_ID),
            (PID_TAG_IPM_DRAFTS_ENTRY_ID, DRAFTS_FOLDER_ID),
        ] {
            let entry_id =
                crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, folder_id)
                    .unwrap();
            assert_eq!(
                special_folder_identification_property_value(mailbox_guid, property_tag),
                Some(MapiValue::Binary(entry_id.clone()))
            );
            assert_eq!(entry_id.len(), 46);
            assert_eq!(
                crate::mapi::identity::object_id_from_folder_identifier_bytes(&entry_id),
                Some(folder_id)
            );
        }
        let reminders_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
            mailbox_guid,
            REMINDERS_FOLDER_ID,
        )
        .unwrap();
        assert_eq!(
            special_folder_identification_property_value(mailbox_guid, PID_TAG_REM_ONLINE_ENTRY_ID),
            Some(MapiValue::Binary(reminders_entry_id.clone()))
        );
        assert_eq!(
            special_folder_identification_property_value(
                mailbox_guid,
                PID_TAG_REM_OFFLINE_ENTRY_ID
            ),
            Some(MapiValue::Binary(reminders_entry_id.clone()))
        );
        assert_eq!(
            crate::mapi::identity::object_id_from_folder_identifier_bytes(&reminders_entry_id),
            Some(REMINDERS_FOLDER_ID)
        );
    }

    #[test]
    fn additional_ren_entry_ids_ex_advertises_outlook_store_special_folders() {
        let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let Some(MapiValue::Binary(value)) = special_folder_identification_property_value(
            mailbox_guid,
            PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX,
        ) else {
            panic!("expected AdditionalRenEntryIdsEx binary value");
        };

        let mut offset = 0;
        let mut entries = Vec::new();
        loop {
            let persist_id = u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap());
            let data_size = u16::from_le_bytes(value[offset + 2..offset + 4].try_into().unwrap());
            offset += 4;
            if persist_id == 0 {
                break;
            }
            let block_end = offset + data_size as usize;
            let mut folder_id = None;
            while offset < block_end {
                let element_id = u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap());
                let element_size =
                    u16::from_le_bytes(value[offset + 2..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if element_id == 0 {
                    break;
                }
                let element = &value[offset..offset + element_size];
                if element_id == 0x0001 {
                    folder_id =
                        crate::mapi::identity::object_id_from_folder_identifier_bytes(element);
                }
                offset += element_size;
            }
            offset = block_end;
            entries.push((persist_id, folder_id));
        }

        assert_eq!(
            entries,
            vec![
                (0x8001, Some(RSS_FEEDS_FOLDER_ID)),
                (0x8002, Some(TRACKED_MAIL_PROCESSING_FOLDER_ID)),
                (0x8004, Some(TODO_SEARCH_FOLDER_ID)),
                (0x8006, Some(CONVERSATION_ACTION_SETTINGS_FOLDER_ID)),
                (0x8008, Some(SUGGESTED_CONTACTS_FOLDER_ID)),
                (0x8009, Some(CONTACTS_SEARCH_FOLDER_ID)),
                (0x800A, Some(IM_CONTACT_LIST_FOLDER_ID)),
                (0x800B, Some(QUICK_CONTACTS_FOLDER_ID)),
                (0x800F, Some(ARCHIVE_FOLDER_ID)),
            ]
        );
        assert_eq!(value.len(), 490);
    }

    #[test]
    fn additional_ren_entry_ids_advertises_documented_indexed_special_folders() {
        let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let Some(MapiValue::MultiBinary(values)) = special_folder_identification_property_value(
            mailbox_guid,
            PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
        ) else {
            panic!("expected AdditionalRenEntryIds multi-binary value");
        };

        assert_eq!(
            values
                .iter()
                .map(
                    |entry_id| crate::mapi::identity::object_id_from_folder_identifier_bytes(
                        entry_id
                    )
                )
                .collect::<Vec<_>>(),
            vec![
                Some(CONFLICTS_FOLDER_ID),
                Some(SYNC_ISSUES_FOLDER_ID),
                Some(LOCAL_FAILURES_FOLDER_ID),
                Some(SERVER_FAILURES_FOLDER_ID),
                Some(JUNK_FOLDER_ID),
            ]
        );
        assert!(values.iter().all(|entry_id| entry_id.len() == 46));
    }

    #[test]
    fn free_busy_entry_ids_advertises_freebusy_data_at_documented_index() {
        let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let Some(MapiValue::MultiBinary(values)) =
            special_folder_identification_property_value(mailbox_guid, PID_TAG_FREE_BUSY_ENTRY_IDS)
        else {
            panic!("expected FreeBusyEntryIds multi-binary value");
        };

        assert_eq!(values.len(), 4);
        assert!(values[..3].iter().all(Vec::is_empty));
        assert_eq!(
            crate::mapi::identity::object_id_from_folder_identifier_bytes(&values[3]),
            Some(FREEBUSY_DATA_FOLDER_ID)
        );
        assert_eq!(values[3].len(), 46);
    }

    #[test]
    fn typed_scalar_property_values_round_trip() {
        assert_eq!(
            round_trip(0x3001_001F, &MapiValue::String("Inbox".to_string())),
            MapiValue::String("Inbox".to_string())
        );
        assert_eq!(
            round_trip(0x3001_001E, &MapiValue::String("Inbox".to_string())),
            MapiValue::String("Inbox".to_string())
        );
        assert_eq!(
            round_trip(0x3602_0003, &MapiValue::I32(42)),
            MapiValue::I32(42)
        );
        assert_eq!(
            round_trip(0x360A_000B, &MapiValue::Bool(true)),
            MapiValue::Bool(true)
        );
        assert_eq!(
            round_trip(0x6748_0014, &MapiValue::I64(99)),
            MapiValue::I64(99)
        );
        assert_eq!(
            round_trip(
                PID_LID_PERCENT_COMPLETE_TAG,
                &MapiValue::F64(1.0f64.to_bits())
            ),
            MapiValue::F64(1.0f64.to_bits())
        );
    }

    #[test]
    fn object_id_properties_use_mapi_wire_ids() {
        let mut encoded = Vec::new();
        write_mapi_value(
            &mut encoded,
            PID_TAG_FOLDER_ID,
            &MapiValue::U64(crate::mapi::identity::CALENDAR_FOLDER_ID),
        );

        assert_eq!(
            crate::mapi::identity::object_id_from_wire_id(&encoded),
            Some(crate::mapi::identity::CALENDAR_FOLDER_ID)
        );
    }

    #[test]
    fn binary_property_uses_rop_u16_length_prefix() {
        let mut encoded = Vec::new();
        write_mapi_value(
            &mut encoded,
            PID_TAG_ATTACH_DATA_BINARY,
            &MapiValue::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        );

        assert_eq!(encoded, vec![0x04, 0x00, 0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(
            parse_mapi_property_value(&mut Cursor::new(&encoded), PID_TAG_ATTACH_DATA_BINARY)
                .unwrap(),
            MapiValue::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF])
        );
    }

    #[test]
    fn multivalue_strings_and_binaries_round_trip() {
        let strings = MapiValue::MultiString(vec!["alpha".to_string(), "beta".to_string()]);
        let binaries = MapiValue::MultiBinary(vec![vec![0x01, 0x02], vec![0xAA, 0xBB, 0xCC]]);

        assert_eq!(round_trip(0x8001_101F, &strings), strings);
        assert_eq!(round_trip(0x8002_1102, &binaries), binaries);
    }

    #[test]
    fn large_inline_string_round_trips_through_common_codec() {
        let large = MapiValue::String("A".repeat(4096));

        assert_eq!(round_trip(PID_TAG_BODY_W, &large), large);
    }

    #[test]
    fn mapi_note_and_journal_inputs_preserve_canonical_fields() {
        let mut note_properties = HashMap::new();
        note_properties.insert(PID_TAG_SUBJECT_W, MapiValue::String("Mapped note".into()));
        note_properties.insert(PID_TAG_BODY_W, MapiValue::String("Note body".into()));
        note_properties.insert(PID_LID_NOTE_COLOR_TAG, MapiValue::I32(1));
        let note = note_input_from_mapi(
            Uuid::nil(),
            Some(Uuid::from_u128(1)),
            &default_note_for_mapping(),
            &note_properties,
        );
        assert_eq!(note.id, Some(Uuid::from_u128(1)));
        assert_eq!(note.title, "Mapped note");
        assert_eq!(note.body_text, "Note body");
        assert_eq!(note.color, "green");

        let mut journal_properties = HashMap::new();
        journal_properties.insert(PID_TAG_SUBJECT_W, MapiValue::String("Mapped call".into()));
        journal_properties.insert(PID_TAG_BODY_W, MapiValue::String("Call body".into()));
        journal_properties.insert(
            PID_TAG_MESSAGE_CLASS_W,
            MapiValue::String("IPM.Activity".into()),
        );
        journal_properties.insert(
            PID_LID_LOG_TYPE_W_TAG,
            MapiValue::String("Phone call".into()),
        );
        journal_properties.insert(
            PID_LID_COMPANIES_TAG,
            MapiValue::MultiString(vec!["Contoso".into()]),
        );
        journal_properties.insert(
            PID_LID_CONTACTS_TAG,
            MapiValue::MultiString(vec!["Adam Barr".into()]),
        );
        let journal = journal_entry_input_from_mapi(
            Uuid::nil(),
            Some(Uuid::from_u128(2)),
            &default_journal_entry_for_mapping(),
            &journal_properties,
        );
        assert_eq!(journal.id, Some(Uuid::from_u128(2)));
        assert_eq!(journal.subject, "Mapped call");
        assert_eq!(journal.body_text, "Call body");
        assert_eq!(journal.entry_type, "Phone call");
        assert_eq!(journal.message_class, "IPM.Activity");
        assert_eq!(journal.companies_json, "[\"Contoso\"]");
        assert_eq!(journal.contacts_json, "[\"Adam Barr\"]");
    }

    #[test]
    fn mapi_note_and_journal_named_properties_project_canonical_values() {
        assert_eq!(
            well_known_named_property_id(&MapiNamedProperty {
                guid: PSETID_NOTE_GUID,
                kind: MapiNamedPropertyKind::Lid(PID_LID_NOTE_COLOR),
            }),
            Some(PID_LID_NOTE_COLOR as u16)
        );
        assert_eq!(
            well_known_named_property_id(&MapiNamedProperty {
                guid: PSETID_LOG_GUID,
                kind: MapiNamedPropertyKind::Lid(PID_LID_LOG_TYPE),
            }),
            Some(PID_LID_LOG_TYPE as u16)
        );

        let note = ClientNote {
            color: "pink".to_string(),
            ..default_note_for_mapping()
        };
        assert_eq!(
            note_property_value(&note, 1, NOTES_FOLDER_ID, PID_LID_NOTE_COLOR_TAG),
            Some(MapiValue::I32(2))
        );

        let journal = JournalEntry {
            entry_type: "Phone call".to_string(),
            starts_at: Some("2026-05-19T10:00:00Z".to_string()),
            companies_json: "[\"Contoso\"]".to_string(),
            contacts_json: "[\"Adam Barr\"]".to_string(),
            ..default_journal_entry_for_mapping()
        };
        assert_eq!(
            journal_entry_property_value(&journal, 1, JOURNAL_FOLDER_ID, PID_LID_LOG_TYPE_W_TAG),
            Some(MapiValue::String("Phone call".to_string()))
        );
        assert_eq!(
            journal_entry_property_value(&journal, 1, JOURNAL_FOLDER_ID, PID_LID_COMPANIES_TAG),
            Some(MapiValue::MultiString(vec!["Contoso".to_string()]))
        );
        assert_eq!(
            journal_entry_property_value(
                &journal,
                1,
                JOURNAL_FOLDER_ID,
                PID_LID_CONTACT_LINK_NAME_W_TAG
            ),
            Some(MapiValue::String("Adam Barr".to_string()))
        );
    }

    #[test]
    fn rss_feed_messages_project_rss_message_class_and_named_properties() {
        assert_eq!(
            well_known_named_property_id(&MapiNamedProperty {
                guid: PSETID_POST_RSS_GUID,
                kind: MapiNamedPropertyKind::Lid(PID_LID_POST_RSS_ITEM_GUID),
            }),
            Some(PID_LID_POST_RSS_ITEM_GUID as u16)
        );

        let mailbox_id = Uuid::from_u128(0x3333);
        let email = JmapEmail {
            id: Uuid::from_u128(0x1111),
            thread_id: Uuid::from_u128(0x2222),
            mailbox_id,
            mailbox_role: "rss_feeds".to_string(),
            mailbox_name: "RSS Feeds".to_string(),
            modseq: 7,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![lpe_storage::JmapEmailMailboxState {
                mailbox_id,
                role: "rss_feeds".to_string(),
                name: "RSS Feeds".to_string(),
                modseq: 7,
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
            received_at: "2026-05-20T10:00:00Z".to_string(),
            sent_at: None,
            from_address: "feed@example.test".to_string(),
            from_display: Some("Feed".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: Uuid::nil(),
            to: Vec::new(),
            cc: Vec::new(),
            bcc: vec![lpe_storage::JmapEmailAddress {
                address: "hidden@example.test".to_string(),
                display_name: Some("Hidden".to_string()),
            }],
            subject: "RSS item".to_string(),
            preview: "Preview".to_string(),
            body_text: "<item>RSS item</item>".to_string(),
            body_html_sanitized: Some("<p>RSS item</p>".to_string()),
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
            size_octets: 128,
            internet_message_id: Some("rss-guid".to_string()),
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        };

        assert_eq!(
            email_property_value(&email, PID_TAG_MESSAGE_CLASS_W),
            Some(MapiValue::String("IPM.Post.RSS".to_string()))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_ORIGINAL_MESSAGE_CLASS_W),
            Some(MapiValue::String("IPM.Post.RSS".to_string()))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_ACCESS_LEVEL),
            Some(MapiValue::U32(1))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_SENDER_ADDRESS_TYPE_W),
            Some(MapiValue::String("SMTP".to_string()))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_SENDER_SMTP_ADDRESS_W),
            Some(MapiValue::String("feed@example.test".to_string()))
        );
        assert_eq!(
            email_property_value(&email, PID_LID_POST_RSS_ITEM_GUID_W_TAG),
            Some(MapiValue::String("rss-guid".to_string()))
        );
        assert_eq!(
            email_property_value(&email, PID_LID_POST_RSS_CHANNEL_W_TAG),
            Some(MapiValue::String("RSS Feeds".to_string()))
        );
        assert_eq!(
            email_property_value(&email, PID_LID_POST_RSS_ITEM_XML_W_TAG),
            Some(MapiValue::String("<item>RSS item</item>".to_string()))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_CONVERSATION_TOPIC_W),
            Some(MapiValue::String("RSS item".to_string()))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_CONVERSATION_INDEX),
            Some(MapiValue::Binary(conversation_index_for_uuid(
                email.thread_id
            )))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_MESSAGE_STATUS),
            Some(MapiValue::U32(0))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_SEARCH_KEY),
            Some(MapiValue::Binary(message_search_key(email.id)))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_DISPLAY_BCC_W),
            Some(MapiValue::String("Hidden".to_string()))
        );

        let headers = match email_property_value(&email, PID_TAG_TRANSPORT_MESSAGE_HEADERS_W) {
            Some(MapiValue::String(headers)) => headers,
            other => panic!("unexpected transport headers value: {other:?}"),
        };
        assert!(headers.contains("Message-ID: rss-guid"));
        assert!(headers.contains("From: Feed"));
        assert!(headers.contains("Subject: RSS item"));
        assert!(!headers.contains("Bcc:"));
        assert_eq!(
            email_property_value(&email, PID_TAG_BODY_HTML_W),
            Some(MapiValue::String("<p>RSS item</p>".to_string()))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_HTML_BINARY),
            Some(MapiValue::Binary(b"<p>RSS item</p>".to_vec()))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_RTF_IN_SYNC),
            Some(MapiValue::Bool(false))
        );
        let rtf = match email_property_value(&email, PID_TAG_RTF_COMPRESSED) {
            Some(MapiValue::Binary(value)) => value,
            other => panic!("unexpected RTF body value: {other:?}"),
        };
        assert!(rtf.len() > 16);
        assert_eq!(
            u32::from_le_bytes([rtf[0], rtf[1], rtf[2], rtf[3]]) as usize,
            rtf.len() - 4
        );
        assert_eq!(
            u32::from_le_bytes([rtf[4], rtf[5], rtf[6], rtf[7]]) as usize,
            rtf.len() - 16
        );
        assert_eq!(
            u32::from_le_bytes([rtf[8], rtf[9], rtf[10], rtf[11]]),
            0x414C_454D
        );
        assert_eq!(u32::from_le_bytes([rtf[12], rtf[13], rtf[14], rtf[15]]), 0);
        assert!(String::from_utf8_lossy(&rtf[16..]).contains("RSS item"));
        assert_eq!(
            email_property_value(&email, PID_TAG_NATIVE_BODY),
            Some(MapiValue::U32(3))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_INTERNET_CODEPAGE),
            Some(MapiValue::U32(65001))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_MESSAGE_LOCALE_ID),
            Some(MapiValue::U32(0x0409))
        );
    }

    #[test]
    fn followup_mail_projects_outlook_flag_properties() {
        assert_eq!(
            well_known_named_property_id(&MapiNamedProperty {
                guid: PSETID_COMMON_GUID,
                kind: MapiNamedPropertyKind::Lid(PID_LID_FLAG_REQUEST),
            }),
            Some(PID_LID_FLAG_REQUEST as u16)
        );
        assert_eq!(
            well_known_named_property_id(&MapiNamedProperty {
                guid: PSETID_TASK_GUID,
                kind: MapiNamedPropertyKind::Lid(PID_LID_PERCENT_COMPLETE),
            }),
            Some(PID_LID_PERCENT_COMPLETE as u16)
        );
        assert_eq!(
            well_known_named_property_id(&MapiNamedProperty {
                guid: PSETID_TASK_GUID,
                kind: MapiNamedPropertyKind::Lid(PID_LID_TASK_START_DATE),
            }),
            Some(PID_LID_TASK_START_DATE as u16)
        );
        assert_eq!(
            well_known_named_property_id(&MapiNamedProperty {
                guid: PSETID_TASK_GUID,
                kind: MapiNamedPropertyKind::Lid(PID_LID_TASK_DUE_DATE),
            }),
            Some(PID_LID_TASK_DUE_DATE as u16)
        );

        let mailbox_id = Uuid::from_u128(0x4444);
        let store_id = Uuid::from_u128(0x5555);
        let email = JmapEmail {
            id: Uuid::from_u128(0x1111),
            thread_id: Uuid::from_u128(0x2222),
            mailbox_id,
            mailbox_role: "inbox".to_string(),
            mailbox_name: "Inbox".to_string(),
            modseq: 7,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![lpe_storage::JmapEmailMailboxState {
                mailbox_id,
                role: "inbox".to_string(),
                name: "Inbox".to_string(),
                modseq: 7,
                unread: false,
                flagged: true,
                followup_flag_status: "complete".to_string(),
                followup_icon: 6,
                todo_item_flags: 8,
                followup_request: "Follow up".to_string(),
                followup_start_at: Some("2026-05-20T09:00:00Z".to_string()),
                followup_due_at: Some("2026-05-21T17:00:00Z".to_string()),
                followup_completed_at: Some("2026-05-20T10:30:00Z".to_string()),
                reminder_set: true,
                reminder_at: Some("2026-05-20T09:30:00Z".to_string()),
                reminder_dismissed_at: None,
                swapped_todo_store_id: Some(store_id),
                swapped_todo_data: Some(vec![1, 2, 3, 4]),
                categories: Vec::new(),
                draft: false,
            }],
            received_at: "2026-05-20T10:00:00Z".to_string(),
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
            subject: "Flagged item".to_string(),
            preview: "Flagged item".to_string(),
            body_text: "Flagged item".to_string(),
            body_html_sanitized: None,
            unread: false,
            flagged: true,
            followup_flag_status: "complete".to_string(),
            followup_icon: 6,
            todo_item_flags: 8,
            followup_request: "Follow up".to_string(),
            followup_start_at: Some("2026-05-20T09:00:00Z".to_string()),
            followup_due_at: Some("2026-05-21T17:00:00Z".to_string()),
            followup_completed_at: Some("2026-05-20T10:30:00Z".to_string()),
            reminder_set: true,
            reminder_at: Some("2026-05-20T09:30:00Z".to_string()),
            reminder_dismissed_at: None,
            swapped_todo_store_id: Some(store_id),
            swapped_todo_data: Some(valid_swapped_todo_data()),
            categories: Vec::new(),
            has_attachments: false,
            size_octets: 128,
            internet_message_id: None,
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        };

        assert_eq!(
            email_property_value(&email, PID_TAG_FLAG_STATUS),
            Some(MapiValue::U32(1))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_FOLLOWUP_ICON),
            Some(MapiValue::I32(6))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_TODO_ITEM_FLAGS),
            Some(MapiValue::I32(8))
        );
        assert_eq!(
            email_property_value(&email, PID_LID_FLAG_REQUEST_W_TAG),
            Some(MapiValue::String("Follow up".to_string()))
        );
        assert_eq!(
            email_property_value(&email, PID_LID_PERCENT_COMPLETE_TAG),
            Some(MapiValue::F64(1.0f64.to_bits()))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_FLAG_COMPLETE_TIME),
            Some(MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(
                "2026-05-20T10:30:00Z"
            )))
        );
        assert_eq!(
            email_property_value(&email, PID_LID_TASK_START_DATE_TAG),
            Some(MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(
                "2026-05-20T09:00:00Z"
            )))
        );
        assert_eq!(
            email_property_value(&email, PID_LID_TASK_DUE_DATE_TAG),
            Some(MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(
                "2026-05-21T17:00:00Z"
            )))
        );
        assert_eq!(
            email_property_value(&email, PID_LID_REMINDER_SET_TAG),
            Some(MapiValue::Bool(true))
        );
        assert_eq!(
            email_property_value(&email, PID_LID_REMINDER_TIME_TAG),
            Some(MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(
                "2026-05-20T09:30:00Z"
            )))
        );
        assert_eq!(
            email_property_value(&email, PID_LID_REMINDER_SIGNAL_TIME_TAG),
            Some(MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(
                "2026-05-20T09:30:00Z"
            )))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_SWAPPED_TODO_STORE),
            Some(MapiValue::Binary(store_id.as_bytes().to_vec()))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_SWAPPED_TODO_DATA),
            Some(MapiValue::Binary(valid_swapped_todo_data()))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_BODY_HTML_W),
            Some(MapiValue::String(
                "<html><body>Flagged item</body></html>".to_string()
            ))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_HTML_BINARY),
            Some(MapiValue::Binary(
                b"<html><body>Flagged item</body></html>".to_vec()
            ))
        );
        assert_eq!(
            email_property_value(&email, PID_TAG_NATIVE_BODY),
            Some(MapiValue::U32(1))
        );
    }

    #[test]
    fn reminder_named_properties_project_from_canonical_reminder_links() {
        assert_eq!(
            well_known_named_property_id(&MapiNamedProperty {
                guid: PSETID_COMMON_GUID,
                kind: MapiNamedPropertyKind::Lid(PID_LID_REMINDER_SET),
            }),
            Some(PID_LID_REMINDER_SET as u16)
        );
        let rights = lpe_storage::CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: false,
        };
        let event_id = Uuid::from_u128(0x3333);
        let event = lpe_storage::AccessibleEvent {
            id: event_id,
            uid: "event-uid".to_string(),
            collection_id: "default".to_string(),
            owner_account_id: Uuid::nil(),
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
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "[]".to_string(),
            notes: String::new(),
            body_html: String::new(),
        };
        let reminder = lpe_storage::ClientReminder {
            source_type: "calendar".to_string(),
            source_id: event_id,
            occurrence_start_at: None,
            title: "Standup".to_string(),
            due_at: Some("2026-05-21T09:30:00Z".to_string()),
            reminder_at: "2026-05-21T08:45:00Z".to_string(),
            dismissed_at: None,
            completed_at: None,
            status: "pending".to_string(),
        };
        assert_eq!(
            event_property_value_with_reminder(
                &event,
                1,
                REMINDERS_FOLDER_ID,
                PID_LID_REMINDER_SET_TAG,
                Some(&reminder)
            ),
            Some(MapiValue::Bool(true))
        );
        assert_eq!(
            event_property_value_with_reminder(
                &event,
                1,
                REMINDERS_FOLDER_ID,
                PID_LID_REMINDER_SIGNAL_TIME_TAG,
                Some(&reminder)
            ),
            Some(MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(
                "2026-05-21T08:45:00Z"
            )))
        );
        assert_eq!(
            event_property_value_with_reminder(
                &event,
                1,
                REMINDERS_FOLDER_ID,
                PID_LID_REMINDER_DELTA_TAG,
                Some(&reminder)
            ),
            Some(MapiValue::I32(15))
        );
        assert_eq!(
            event_property_value_with_reminder(
                &event,
                1,
                REMINDERS_FOLDER_ID,
                PID_LID_REMINDER_OVERRIDE_TAG,
                Some(&reminder)
            ),
            Some(MapiValue::Bool(false))
        );

        let task = lpe_storage::ClientTask {
            id: Uuid::from_u128(0x4444),
            owner_account_id: Uuid::nil(),
            owner_email: "alice@example.test".to_string(),
            owner_display_name: "Alice".to_string(),
            is_owned: true,
            rights,
            task_list_id: Uuid::nil(),
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
        let task_reminder = lpe_storage::ClientReminder {
            source_type: "task".to_string(),
            source_id: task.id,
            occurrence_start_at: None,
            title: "Follow up".to_string(),
            due_at: task.due_at.clone(),
            reminder_at: "2026-05-21T11:45:00Z".to_string(),
            dismissed_at: None,
            completed_at: None,
            status: "pending".to_string(),
        };
        assert_eq!(
            task_property_value_with_reminder(
                &task,
                2,
                REMINDERS_FOLDER_ID,
                PID_LID_PERCENT_COMPLETE_TAG,
                Some(&task_reminder)
            ),
            Some(MapiValue::F64(0.0f64.to_bits()))
        );
        assert_eq!(
            task_property_value_with_reminder(
                &task,
                2,
                REMINDERS_FOLDER_ID,
                PID_LID_REMINDER_TIME_TAG,
                Some(&task_reminder)
            ),
            Some(MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(
                "2026-05-21T11:45:00Z"
            )))
        );
        assert_eq!(
            task_property_value_with_reminder(
                &task,
                2,
                REMINDERS_FOLDER_ID,
                PID_LID_REMINDER_DELTA_TAG,
                Some(&task_reminder)
            ),
            Some(MapiValue::I32(15))
        );
        assert_eq!(
            task_property_value_with_reminder(
                &task,
                2,
                REMINDERS_FOLDER_ID,
                PID_LID_REMINDER_FILE_PARAMETER_W_TAG,
                Some(&task_reminder)
            ),
            Some(MapiValue::String(String::new()))
        );
    }

    #[test]
    fn zero_duration_events_project_non_zero_mapi_appointment_window() {
        let event = lpe_storage::AccessibleEvent {
            id: Uuid::from_u128(0x5555),
            uid: "zero-duration".to_string(),
            collection_id: "default".to_string(),
            owner_account_id: Uuid::nil(),
            owner_email: "alice@example.test".to_string(),
            owner_display_name: "Alice".to_string(),
            rights: lpe_storage::CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: false,
            },
            date: "2026-05-21".to_string(),
            time: "09:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 0,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Zero duration".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "[]".to_string(),
            notes: String::new(),
            body_html: String::new(),
        };

        assert!(event_end_filetime(&event) > event_start_filetime(&event));
        assert_eq!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_END_DATE),
            Some(MapiValue::I64(event_end_filetime(&event) as i64))
        );
    }

    #[test]
    fn calendar_projection_uses_canonical_all_day_status_and_participants() {
        let event = lpe_storage::AccessibleEvent {
            id: Uuid::from_u128(0x7777),
            uid: "canonical-calendar".to_string(),
            collection_id: "default".to_string(),
            owner_account_id: Uuid::nil(),
            owner_email: "alice@example.test".to_string(),
            owner_display_name: "Alice".to_string(),
            rights: lpe_storage::CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: false,
            },
            date: "2026-05-21".to_string(),
            time: "09:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 60,
            all_day: true,
            status: "cancelled".to_string(),
            sequence: 3,
            recurrence_rule: "FREQ=WEEKLY;COUNT=2".to_string(),
            recurrence_json: "{\"frequency\":\"weekly\"}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Canonical appointment".to_string(),
            location: "Room A".to_string(),
            organizer_json: "{\"email\":\"alice@example.test\"}".to_string(),
            attendees: "Bob".to_string(),
            attendees_json: serialize_calendar_participants_metadata(
                &CalendarParticipantsMetadata {
                    organizer: Some(lpe_storage::CalendarOrganizerMetadata {
                        email: "alice@example.test".to_string(),
                        common_name: "Alice".to_string(),
                    }),
                    attendees: vec![
                        lpe_storage::CalendarParticipantMetadata {
                            email: "bob@example.test".to_string(),
                            common_name: "Bob".to_string(),
                            role: "REQ-PARTICIPANT".to_string(),
                            partstat: "accepted".to_string(),
                            rsvp: false,
                        },
                        lpe_storage::CalendarParticipantMetadata {
                            email: "cara@example.test".to_string(),
                            common_name: "Cara".to_string(),
                            role: "OPT-PARTICIPANT".to_string(),
                            partstat: "needs-action".to_string(),
                            rsvp: false,
                        },
                    ],
                },
            ),
            notes: "Body".to_string(),
            body_html: "<p>Body</p>".to_string(),
        };

        assert_eq!(
            event_property_value(
                &event,
                1,
                CALENDAR_FOLDER_ID,
                PID_LID_APPOINTMENT_SUB_TYPE_TAG
            ),
            Some(MapiValue::Bool(true))
        );
        assert_eq!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_BUSY_STATUS_TAG),
            Some(MapiValue::I32(0))
        );
        assert_eq!(
            event_property_value(
                &event,
                1,
                CALENDAR_FOLDER_ID,
                PID_LID_APPOINTMENT_STATE_FLAGS_TAG
            ),
            Some(MapiValue::I32(0x0000_0005))
        );
        assert_eq!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_COMMON_START_TAG),
            Some(MapiValue::I64(event_start_filetime(&event) as i64))
        );
        assert_eq!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_COMMON_END_TAG),
            Some(MapiValue::I64(event_end_filetime(&event) as i64))
        );
        assert_eq!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_LOCATION_W),
            Some(MapiValue::String("Room A".to_string()))
        );
        assert_eq!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_SENDER_NAME_W),
            Some(MapiValue::String("Alice".to_string()))
        );
        assert_eq!(
            event_property_value(
                &event,
                1,
                CALENDAR_FOLDER_ID,
                PID_TAG_SENDER_EMAIL_ADDRESS_W
            ),
            Some(MapiValue::String("alice@example.test".to_string()))
        );
        assert_eq!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_DISPLAY_TO_W),
            Some(MapiValue::String("Bob, Cara".to_string()))
        );
        assert_eq!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_DISPLAY_CC_W),
            Some(MapiValue::String("Cara".to_string()))
        );
        assert_eq!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_BODY_HTML_W),
            Some(MapiValue::String("<p>Body</p>".to_string()))
        );
        assert_eq!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_HTML_BINARY),
            Some(MapiValue::Binary(b"<p>Body</p>".to_vec()))
        );
        assert_eq!(
            event_property_value(
                &event,
                1,
                CALENDAR_FOLDER_ID,
                PID_LID_ALL_ATTENDEES_STRING_W_TAG
            ),
            Some(MapiValue::String("Bob; Cara".to_string()))
        );
        assert_eq!(
            event_property_value(
                &event,
                1,
                CALENDAR_FOLDER_ID,
                PID_LID_TO_ATTENDEES_STRING_W_TAG
            ),
            Some(MapiValue::String("Bob".to_string()))
        );
        assert_eq!(
            event_property_value(
                &event,
                1,
                CALENDAR_FOLDER_ID,
                PID_LID_CC_ATTENDEES_STRING_W_TAG
            ),
            Some(MapiValue::String("Cara".to_string()))
        );
        assert_eq!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_LOCATION_W_TAG),
            Some(MapiValue::String("Room A".to_string()))
        );
        assert_eq!(
            event_property_value(
                &event,
                1,
                CALENDAR_FOLDER_ID,
                PID_LID_APPOINTMENT_DURATION_TAG
            ),
            Some(MapiValue::I32(60))
        );
        assert_eq!(
            event_property_value(
                &event,
                1,
                CALENDAR_FOLDER_ID,
                PID_LID_TIME_ZONE_DESCRIPTION_W_TAG
            ),
            Some(MapiValue::String("UTC".to_string()))
        );
        assert!(matches!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_TIME_ZONE_STRUCT_TAG),
            Some(MapiValue::Binary(value)) if value.len() == 48
        ));
        assert!(matches!(
            event_property_value(
                &event,
                1,
                CALENDAR_FOLDER_ID,
                PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG
            ),
            Some(MapiValue::Binary(value)) if value.starts_with(&[0x02, 0x01]) && value.ends_with(&[0; 16])
        ));
        assert_eq!(
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_HAS_ATTACHMENTS),
            Some(MapiValue::Bool(false))
        );
    }

    #[test]
    fn mapi_over_http_calendar_writes_map_supported_mapi_fields_to_canonical_event_fields() {
        let existing = default_event_for_mapping(Uuid::nil(), "default");
        let mut properties = HashMap::new();
        properties.insert(PID_TAG_SUBJECT_W, MapiValue::String("Updated".to_string()));
        properties.insert(PID_LID_APPOINTMENT_SUB_TYPE_TAG, MapiValue::Bool(true));
        properties.insert(PID_LID_BUSY_STATUS_TAG, MapiValue::I32(1));
        properties.insert(
            PID_TAG_SENDER_NAME_W,
            MapiValue::String("Alice Owner".to_string()),
        );
        properties.insert(
            PID_TAG_SENDER_EMAIL_ADDRESS_W,
            MapiValue::String("Alice@Example.Test".to_string()),
        );
        properties.insert(
            PID_TAG_DISPLAY_TO_W,
            MapiValue::String("Bob One".to_string()),
        );
        properties.insert(
            PID_TAG_DISPLAY_CC_W,
            MapiValue::String("Cara Two".to_string()),
        );
        properties.insert(
            PID_TAG_BODY_HTML_W,
            MapiValue::String("<p>Updated</p>".to_string()),
        );
        properties.insert(
            PID_LID_LOCATION_W_TAG,
            MapiValue::String("Room B".to_string()),
        );
        properties.insert(
            PID_LID_TIME_ZONE_DESCRIPTION_W_TAG,
            MapiValue::String("W. Europe Standard Time".to_string()),
        );
        properties.insert(
            PID_TAG_START_DATE,
            MapiValue::I64(date_time_to_filetime("2026-05-22", "10:00") as i64),
        );
        properties.insert(PID_LID_APPOINTMENT_DURATION_TAG, MapiValue::I32(45));

        let input = event_input_from_mapi(
            Uuid::nil(),
            Some(Uuid::from_u128(0x8888)),
            &existing,
            &properties,
        )
        .unwrap();

        assert_eq!(input.title, "Updated");
        assert!(input.all_day);
        assert_eq!(input.body_html, "<p>Updated</p>");
        assert_eq!(input.date, "2026-05-22");
        assert_eq!(input.time, "10:00");
        assert_eq!(input.duration_minutes, 45);
        assert_eq!(input.location, "Room B");
        assert_eq!(input.time_zone, "W. Europe Standard Time");
        assert_eq!(input.status, "tentative");
        assert_eq!(input.recurrence_rule, existing.recurrence_rule);
        assert_eq!(input.attendees, "Bob One, Cara Two");
        assert!(input.organizer_json.contains("alice@example.test"));
        assert!(input.attendees_json.contains("Bob One"));
        assert!(input.attendees_json.contains("OPT-PARTICIPANT"));
    }

    #[test]
    fn mapi_over_http_calendar_binary_payloads_fail_explicitly() {
        let mut properties = HashMap::new();
        properties.insert(0x8200_0102, MapiValue::Binary(vec![1, 2, 3]));

        let error = reject_unsupported_mapi_event_properties(&properties).unwrap_err();

        assert!(error
            .to_string()
            .contains("MAPI binary calendar recurrence or meeting payloads are not supported"));
    }

    #[test]
    fn mapi_over_http_calendar_state_flags_map_bounded_cancel_state() {
        let existing = default_event_for_mapping(Uuid::nil(), "default");
        let mut properties = HashMap::new();
        properties.insert(PID_LID_APPOINTMENT_STATE_FLAGS_TAG, MapiValue::I32(0x5));

        let input = event_input_from_mapi(
            Uuid::nil(),
            Some(Uuid::from_u128(0x8888)),
            &existing,
            &properties,
        )
        .unwrap();

        assert_eq!(input.status, "cancelled");

        properties.insert(PID_LID_APPOINTMENT_STATE_FLAGS_TAG, MapiValue::I32(0x8));
        let error = reject_unsupported_mapi_event_properties(&properties).unwrap_err();
        assert!(error
            .to_string()
            .contains("unsupported MAPI appointment state flags"));
    }

    #[test]
    fn mapi_over_http_calendar_whole_start_end_write_to_canonical_start_duration() {
        let existing = default_event_for_mapping(Uuid::nil(), "default");
        let mut properties = HashMap::new();
        properties.insert(
            PID_LID_APPOINTMENT_START_WHOLE_TAG,
            MapiValue::I64(date_time_to_filetime("2026-06-01", "13:15") as i64),
        );
        properties.insert(
            PID_LID_APPOINTMENT_END_WHOLE_TAG,
            MapiValue::I64(date_time_to_filetime("2026-06-01", "14:45") as i64),
        );

        let input = event_input_from_mapi(
            Uuid::nil(),
            Some(Uuid::from_u128(0x8888)),
            &existing,
            &properties,
        )
        .unwrap();

        assert_eq!(input.date, "2026-06-01");
        assert_eq!(input.time, "13:15");
        assert_eq!(input.duration_minutes, 90);
    }

    #[test]
    fn mapi_over_http_calendar_common_start_end_write_to_canonical_start_duration() {
        let existing = default_event_for_mapping(Uuid::nil(), "default");
        let mut properties = HashMap::new();
        properties.insert(
            PID_LID_COMMON_START_TAG,
            MapiValue::I64(date_time_to_filetime("2026-06-02", "08:00") as i64),
        );
        properties.insert(
            PID_LID_COMMON_END_TAG,
            MapiValue::I64(date_time_to_filetime("2026-06-02", "09:30") as i64),
        );

        let input = event_input_from_mapi(
            Uuid::nil(),
            Some(Uuid::from_u128(0x8888)),
            &existing,
            &properties,
        )
        .unwrap();

        assert_eq!(input.date, "2026-06-02");
        assert_eq!(input.time, "08:00");
        assert_eq!(input.duration_minutes, 90);
    }

    #[test]
    fn mapi_over_http_calendar_meeting_classes_fail_explicitly() {
        for message_class in [
            "IPM.Schedule.Meeting.Resp.Pos",
            "IPM.Schedule.Meeting.Canceled",
            "IPM.Note",
        ] {
            let mut properties = HashMap::new();
            properties.insert(
                PID_TAG_MESSAGE_CLASS_W,
                MapiValue::String(message_class.to_string()),
            );

            let error = reject_unsupported_mapi_event_properties(&properties).unwrap_err();

            assert!(
                error
                    .to_string()
                    .contains("is not mapped to canonical calendar state"),
                "{message_class}"
            );
        }

        let mut properties = HashMap::new();
        properties.insert(
            PID_TAG_MESSAGE_CLASS_W,
            MapiValue::String("IPM.Appointment".to_string()),
        );

        reject_unsupported_mapi_event_properties(&properties).unwrap();

        properties.insert(
            PID_TAG_MESSAGE_CLASS_W,
            MapiValue::String("IPM.Schedule.Meeting.Request".to_string()),
        );

        reject_unsupported_mapi_event_properties(&properties).unwrap();
    }

    #[test]
    fn mapi_over_http_calendar_meeting_response_classes_map_to_partstat() {
        let mut existing = default_event_for_mapping(Uuid::nil(), "default");
        existing.attendees = "Bob".to_string();
        existing.attendees_json = r#"{"attendees":[{"email":"bob@example.test","common_name":"Bob","role":"REQ-PARTICIPANT","partstat":"needs-action","rsvp":true}]}"#.to_string();

        for (message_class, expected_partstat) in [
            ("IPM.Schedule.Meeting.Resp.Pos", "accepted"),
            ("IPM.Schedule.Meeting.Resp.Tent", "tentative"),
            ("IPM.Schedule.Meeting.Resp.Neg", "declined"),
        ] {
            let mut properties = HashMap::new();
            properties.insert(
                PID_TAG_MESSAGE_CLASS_W,
                MapiValue::String(message_class.to_string()),
            );
            properties.insert(
                PID_TAG_SENDER_EMAIL_ADDRESS_W,
                MapiValue::String("bob@example.test".to_string()),
            );
            properties.insert(PID_TAG_SENDER_NAME_W, MapiValue::String("Bob".to_string()));

            let input = meeting_response_event_input_from_mapi(
                Uuid::nil(),
                Some(existing.id),
                &existing,
                &properties,
            )
            .unwrap()
            .expect("meeting response should map");

            assert!(input
                .attendees_json
                .contains(&format!(r#""partstat":"{expected_partstat}""#)));
        }
    }

    #[test]
    fn mapi_over_http_calendar_recurrence_maps_month_end_rule() {
        let existing = default_event_for_mapping(Uuid::nil(), "default");
        let mut month_end = Vec::new();
        append_recur_header(&mut month_end, 0x200C, 0x0004, 1);
        month_end.extend_from_slice(&31u32.to_le_bytes());
        append_recur_tail(
            &mut month_end,
            0x0000_2022,
            3,
            &[],
            &[],
            "2026-05-31",
            "2026-07-31",
        );
        append_appointment_recur_suffix(&mut month_end, 9 * 60, 10 * 60, 0);
        let mut properties = HashMap::new();
        properties.insert(PID_LID_APPOINTMENT_RECUR_TAG, MapiValue::Binary(month_end));

        let input = event_input_from_mapi(
            Uuid::nil(),
            Some(Uuid::from_u128(0x999E)),
            &existing,
            &properties,
        )
        .unwrap();

        assert_eq!(input.recurrence_rule, "FREQ=MONTHLY;COUNT=3;BYMONTHDAY=31");
        assert_eq!(
            input.recurrence_json,
            r#"{"frequency":"monthly","count":3,"byMonthDay":31}"#
        );
    }

    #[test]
    fn mapi_over_http_calendar_recurrence_rejects_unsupported_shapes() {
        let existing = default_event_for_mapping(Uuid::nil(), "default");
        let modified_exception = test_weekly_recur_blob_with_modified_instance(0x0004, "", "");
        let mut properties = HashMap::new();
        properties.insert(
            PID_LID_APPOINTMENT_RECUR_TAG,
            MapiValue::Binary(modified_exception),
        );

        let error = event_input_from_mapi(
            Uuid::nil(),
            Some(Uuid::from_u128(0x999F)),
            &existing,
            &properties,
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("unsupported MAPI calendar recurrence exception override"));
    }

    #[test]
    fn mapi_over_http_calendar_recurrence_binary_maps_to_canonical_daily_rule() {
        let existing = default_event_for_mapping(Uuid::nil(), "default");
        let mut properties = HashMap::new();
        properties.insert(0x8216_0102, MapiValue::Binary(test_daily_recur_blob(2, 3)));

        let input = event_input_from_mapi(
            Uuid::nil(),
            Some(Uuid::from_u128(0x9999)),
            &existing,
            &properties,
        )
        .unwrap();

        assert_eq!(input.recurrence_rule, "FREQ=DAILY;INTERVAL=2;COUNT=3");
        assert_eq!(
            input.recurrence_json,
            r#"{"frequency":"daily","interval":2,"count":3}"#
        );
        assert_eq!(input.recurrence_exceptions_json, "[]");
    }

    #[test]
    fn mapi_over_http_calendar_recurrence_binary_maps_monthly_and_yearly_rules() {
        let existing = default_event_for_mapping(Uuid::nil(), "default");
        let mut monthly_properties = HashMap::new();
        monthly_properties.insert(
            0x8216_0102,
            MapiValue::Binary(test_monthly_recur_blob(2, 12)),
        );

        let monthly = event_input_from_mapi(
            Uuid::nil(),
            Some(Uuid::from_u128(0x999B)),
            &existing,
            &monthly_properties,
        )
        .unwrap();

        assert_eq!(
            monthly.recurrence_rule,
            "FREQ=MONTHLY;INTERVAL=2;COUNT=5;BYMONTHDAY=12"
        );
        assert_eq!(
            monthly.recurrence_json,
            r#"{"frequency":"monthly","interval":2,"count":5,"byMonthDay":12}"#
        );

        let mut monthly_nth_properties = HashMap::new();
        monthly_nth_properties.insert(
            0x8216_0102,
            MapiValue::Binary(test_monthly_nth_recur_blob()),
        );

        let monthly_nth = event_input_from_mapi(
            Uuid::nil(),
            Some(Uuid::from_u128(0x999D)),
            &existing,
            &monthly_nth_properties,
        )
        .unwrap();

        assert_eq!(
            monthly_nth.recurrence_rule,
            "FREQ=MONTHLY;COUNT=3;BYDAY=TU,TH;BYSETPOS=2"
        );
        assert_eq!(
            monthly_nth.recurrence_json,
            r#"{"frequency":"monthly","count":3,"byDay":["TU","TH"],"bySetPosition":2}"#
        );

        let mut yearly_properties = HashMap::new();
        yearly_properties.insert(0x8216_0102, MapiValue::Binary(test_yearly_recur_blob()));

        let yearly = event_input_from_mapi(
            Uuid::nil(),
            Some(Uuid::from_u128(0x999C)),
            &existing,
            &yearly_properties,
        )
        .unwrap();

        assert_eq!(
            yearly.recurrence_rule,
            "FREQ=YEARLY;COUNT=2;BYDAY=FR;BYMONTH=5;BYSETPOS=-1"
        );
        assert_eq!(
            yearly.recurrence_json,
            r#"{"frequency":"yearly","count":2,"byDay":["FR"],"byMonth":5,"bySetPosition":-1}"#
        );
    }

    #[test]
    fn mapi_over_http_calendar_recurrence_binary_maps_deleted_instances_to_overrides() {
        let existing = default_event_for_mapping(Uuid::nil(), "default");
        let mut properties = HashMap::new();
        properties.insert(
            0x8216_0102,
            MapiValue::Binary(test_weekly_recur_blob_with_deleted_instance()),
        );

        let input = event_input_from_mapi(
            Uuid::nil(),
            Some(Uuid::from_u128(0x999A)),
            &existing,
            &properties,
        )
        .unwrap();

        assert_eq!(input.recurrence_rule, "FREQ=WEEKLY;COUNT=4;BYDAY=MO,WE");
        assert_eq!(
            input.recurrence_json,
            r#"{"frequency":"weekly","count":4,"byDay":["MO","WE"]}"#
        );
        assert_eq!(
            input.recurrence_exceptions_json,
            r#"[{"recurrenceId":"2026-05-25","excluded":true}]"#
        );
    }

    #[test]
    fn mapi_over_http_calendar_recurrence_binary_maps_modified_instances_to_overrides() {
        let existing = default_event_for_mapping(Uuid::nil(), "default");
        let mut properties = HashMap::new();
        properties.insert(
            0x8216_0102,
            MapiValue::Binary(test_weekly_recur_blob_with_modified_instance(0, "", "")),
        );

        let input = event_input_from_mapi(
            Uuid::nil(),
            Some(Uuid::from_u128(0x9990)),
            &existing,
            &properties,
        )
        .unwrap();

        assert_eq!(input.recurrence_rule, "FREQ=WEEKLY;COUNT=4;BYDAY=MO,WE");
        assert_eq!(
            input.recurrence_exceptions_json,
            r#"[{"end":"2026-05-25T11:30:00","recurrenceId":"2026-05-25","start":"2026-05-25T11:00:00"}]"#
        );
    }

    #[test]
    fn mapi_over_http_calendar_recurrence_binary_maps_subject_location_exceptions() {
        let existing = default_event_for_mapping(Uuid::nil(), "default");
        let mut properties = HashMap::new();
        properties.insert(
            0x8216_0102,
            MapiValue::Binary(test_weekly_recur_blob_with_modified_instance(
                0x0011,
                "Changed subject",
                "Room B",
            )),
        );

        let input = event_input_from_mapi(
            Uuid::nil(),
            Some(Uuid::from_u128(0x9991)),
            &existing,
            &properties,
        )
        .unwrap();

        assert_eq!(
            input.recurrence_exceptions_json,
            r#"[{"end":"2026-05-25T11:30:00","location":"Room B","recurrenceId":"2026-05-25","start":"2026-05-25T11:00:00","title":"Changed subject"}]"#
        );
    }

    #[test]
    fn mapi_over_http_calendar_recurrence_binary_maps_mixed_deleted_and_modified_instances() {
        let existing = default_event_for_mapping(Uuid::nil(), "default");
        let mut properties = HashMap::new();
        properties.insert(
            0x8216_0102,
            MapiValue::Binary(test_weekly_recur_blob_with_deleted_and_modified_instances()),
        );

        let input = event_input_from_mapi(
            Uuid::nil(),
            Some(Uuid::from_u128(0x9992)),
            &existing,
            &properties,
        )
        .unwrap();

        assert_eq!(
            input.recurrence_exceptions_json,
            r#"[{"recurrenceId":"2026-05-27","excluded":true},{"end":"2026-05-25T11:30:00","location":"Room B","recurrenceId":"2026-05-25","start":"2026-05-25T11:00:00","title":"Changed subject"}]"#
        );
    }

    #[test]
    fn mapi_over_http_calendar_recurrence_projects_back_to_mapi_binary() {
        let mut event = default_event_for_mapping(Uuid::nil(), "default");
        event.date = "2026-05-18".to_string();
        event.time = "09:00".to_string();
        event.duration_minutes = 60;
        event.recurrence_rule = "FREQ=WEEKLY;COUNT=4;BYDAY=MO,WE".to_string();
        event.recurrence_json =
            r#"{"frequency":"weekly","count":4,"byDay":["MO","WE"]}"#.to_string();
        event.recurrence_exceptions_json =
            r#"[{"recurrenceId":"2026-05-25","excluded":true}]"#.to_string();

        let Some(MapiValue::Binary(value)) =
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_APPOINTMENT_RECUR_TAG)
        else {
            panic!("expected recurrence binary projection");
        };
        let recurrence = appointment_recurrence_from_mapi(&value).unwrap();

        assert_eq!(recurrence.recurrence_rule, event.recurrence_rule);
        assert_eq!(recurrence.recurrence_json, event.recurrence_json);
        assert_eq!(
            recurrence.recurrence_exceptions_json,
            event.recurrence_exceptions_json
        );
    }

    #[test]
    fn mapi_over_http_calendar_modified_recurrence_projects_back_to_mapi_binary() {
        let mut event = default_event_for_mapping(Uuid::nil(), "default");
        event.date = "2026-05-18".to_string();
        event.time = "09:00".to_string();
        event.duration_minutes = 60;
        event.recurrence_rule = "FREQ=WEEKLY;COUNT=4;BYDAY=MO,WE".to_string();
        event.recurrence_json =
            r#"{"frequency":"weekly","count":4,"byDay":["MO","WE"]}"#.to_string();
        event.recurrence_exceptions_json =
            r#"[{"recurrenceId":"2026-05-25","start":"2026-05-25T11:00:00","end":"2026-05-25T11:30:00"}]"#.to_string();

        let Some(MapiValue::Binary(value)) =
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_APPOINTMENT_RECUR_TAG)
        else {
            panic!("expected recurrence binary projection");
        };
        let recurrence = appointment_recurrence_from_mapi(&value).unwrap();

        assert_eq!(recurrence.recurrence_rule, event.recurrence_rule);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&recurrence.recurrence_exceptions_json)
                .unwrap(),
            serde_json::from_str::<serde_json::Value>(&event.recurrence_exceptions_json).unwrap()
        );
    }

    #[test]
    fn mapi_over_http_calendar_subject_location_recurrence_projects_back_to_mapi_binary() {
        let mut event = default_event_for_mapping(Uuid::nil(), "default");
        event.date = "2026-05-18".to_string();
        event.time = "09:00".to_string();
        event.duration_minutes = 60;
        event.recurrence_rule = "FREQ=WEEKLY;COUNT=4;BYDAY=MO,WE".to_string();
        event.recurrence_json =
            r#"{"frequency":"weekly","count":4,"byDay":["MO","WE"]}"#.to_string();
        event.recurrence_exceptions_json =
            r#"[{"recurrenceId":"2026-05-25","start":"2026-05-25T11:00:00","end":"2026-05-25T11:30:00","title":"Changed subject","location":"Room B"}]"#.to_string();

        let Some(MapiValue::Binary(value)) =
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_APPOINTMENT_RECUR_TAG)
        else {
            panic!("expected recurrence binary projection");
        };
        let recurrence = appointment_recurrence_from_mapi(&value).unwrap();

        assert_eq!(
            recurrence.recurrence_exceptions_json,
            r#"[{"end":"2026-05-25T11:30:00","location":"Room B","recurrenceId":"2026-05-25","start":"2026-05-25T11:00:00","title":"Changed subject"}]"#
        );
    }

    #[test]
    fn mapi_over_http_calendar_mixed_recurrence_overrides_project_back_to_mapi_binary() {
        let mut event = default_event_for_mapping(Uuid::nil(), "default");
        event.date = "2026-05-18".to_string();
        event.time = "09:00".to_string();
        event.duration_minutes = 60;
        event.recurrence_rule = "FREQ=WEEKLY;COUNT=4;BYDAY=MO,WE".to_string();
        event.recurrence_json =
            r#"{"frequency":"weekly","count":4,"byDay":["MO","WE"]}"#.to_string();
        event.recurrence_exceptions_json =
            r#"[{"recurrenceId":"2026-05-27","excluded":true},{"recurrenceId":"2026-05-25","start":"2026-05-25T11:00:00","end":"2026-05-25T11:30:00","title":"Changed subject","location":"Room B"}]"#.to_string();

        let Some(MapiValue::Binary(value)) =
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_APPOINTMENT_RECUR_TAG)
        else {
            panic!("expected recurrence binary projection");
        };
        let recurrence = appointment_recurrence_from_mapi(&value).unwrap();

        assert_eq!(
            recurrence.recurrence_exceptions_json,
            r#"[{"recurrenceId":"2026-05-27","excluded":true},{"end":"2026-05-25T11:30:00","location":"Room B","recurrenceId":"2026-05-25","start":"2026-05-25T11:00:00","title":"Changed subject"}]"#
        );
    }

    #[test]
    fn mapi_over_http_calendar_month_end_recurrence_projects_back_to_mapi_binary() {
        let mut event = default_event_for_mapping(Uuid::nil(), "default");
        event.date = "2026-05-31".to_string();
        event.time = "09:00".to_string();
        event.duration_minutes = 60;
        event.recurrence_rule = "FREQ=MONTHLY;COUNT=3;BYMONTHDAY=31".to_string();
        event.recurrence_json = r#"{"frequency":"monthly","count":3,"byMonthDay":31}"#.to_string();
        event.recurrence_exceptions_json = "[]".to_string();

        let Some(MapiValue::Binary(value)) =
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_APPOINTMENT_RECUR_TAG)
        else {
            panic!("expected recurrence binary projection");
        };

        assert_eq!(u16::from_le_bytes([value[4], value[5]]), 0x200C);
        assert_eq!(u16::from_le_bytes([value[6], value[7]]), 0x0004);
        let recurrence = appointment_recurrence_from_mapi(&value).unwrap();
        assert_eq!(recurrence.recurrence_rule, event.recurrence_rule);
        assert_eq!(recurrence.recurrence_json, event.recurrence_json);
    }

    #[test]
    fn mapi_over_http_calendar_yearly_recurrence_projects_back_to_mapi_binary_with_month() {
        let mut event = default_event_for_mapping(Uuid::nil(), "default");
        event.date = "2026-01-14".to_string();
        event.time = "09:00".to_string();
        event.duration_minutes = 60;
        event.recurrence_rule = "FREQ=YEARLY;COUNT=2;BYMONTHDAY=14;BYMONTH=7".to_string();
        event.recurrence_json =
            r#"{"frequency":"yearly","count":2,"byMonthDay":14,"byMonth":7}"#.to_string();
        event.recurrence_exceptions_json = "[]".to_string();

        let Some(MapiValue::Binary(value)) =
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_APPOINTMENT_RECUR_TAG)
        else {
            panic!("expected recurrence binary projection");
        };

        let first_date_time = u32::from_le_bytes(value[10..14].try_into().unwrap());
        assert_eq!(
            recurrence_date_string(first_date_time).unwrap(),
            "2026-07-14"
        );
        let recurrence = appointment_recurrence_from_mapi(&value).unwrap();
        assert_eq!(recurrence.recurrence_rule, event.recurrence_rule);
        assert_eq!(recurrence.recurrence_json, event.recurrence_json);
    }

    #[test]
    fn mapi_over_http_calendar_yearly_nth_recurrence_projects_back_to_mapi_binary_with_month() {
        let mut event = default_event_for_mapping(Uuid::nil(), "default");
        event.date = "2026-01-09".to_string();
        event.time = "09:00".to_string();
        event.duration_minutes = 60;
        event.recurrence_rule = "FREQ=YEARLY;COUNT=3;BYDAY=FR;BYMONTH=10;BYSETPOS=2".to_string();
        event.recurrence_json =
            r#"{"frequency":"yearly","count":3,"byDay":["FR"],"byMonth":10,"bySetPosition":2}"#
                .to_string();
        event.recurrence_exceptions_json = "[]".to_string();

        let Some(MapiValue::Binary(value)) =
            event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_APPOINTMENT_RECUR_TAG)
        else {
            panic!("expected recurrence binary projection");
        };

        assert_eq!(u16::from_le_bytes([value[4], value[5]]), 0x200D);
        assert_eq!(u16::from_le_bytes([value[6], value[7]]), 0x0003);
        let first_date_time = u32::from_le_bytes(value[10..14].try_into().unwrap());
        assert_eq!(
            recurrence_date_string(first_date_time).unwrap(),
            "2026-10-09"
        );
        let recurrence = appointment_recurrence_from_mapi(&value).unwrap();
        assert_eq!(recurrence.recurrence_rule, event.recurrence_rule);
        assert_eq!(recurrence.recurrence_json, event.recurrence_json);
    }

    fn test_daily_recur_blob(interval_days: u32, count: u32) -> Vec<u8> {
        let mut value = Vec::new();
        append_recur_header(&mut value, 0x200A, 0x0000, interval_days * 1440);
        append_recur_tail(
            &mut value,
            0x0000_2022,
            count,
            &[],
            &[],
            "2026-05-21",
            "2026-05-25",
        );
        append_appointment_recur_suffix(&mut value, 9 * 60, 10 * 60, 0);
        value
    }

    fn test_weekly_recur_blob_with_deleted_instance() -> Vec<u8> {
        let mut value = Vec::new();
        append_recur_header(&mut value, 0x200B, 0x0001, 1);
        value.extend_from_slice(&0x0000_000Au32.to_le_bytes());
        append_recur_tail(
            &mut value,
            0x0000_2022,
            4,
            &[recurrence_minutes_since_1601("2026-05-25")],
            &[],
            "2026-05-18",
            "2026-06-08",
        );
        append_appointment_recur_suffix(&mut value, 9 * 60, 10 * 60, 0);
        value
    }

    fn test_weekly_recur_blob_with_modified_instance(
        override_flags: u16,
        subject: &str,
        location: &str,
    ) -> Vec<u8> {
        let original = recurrence_minutes_since_1601("2026-05-25");
        let start = original + 11 * 60;
        let end = original + 11 * 60 + 30;
        let mut value = Vec::new();
        append_recur_header(&mut value, 0x200B, 0x0001, 1);
        value.extend_from_slice(&0x0000_000Au32.to_le_bytes());
        append_recur_tail(
            &mut value,
            0x0000_2022,
            4,
            &[original],
            &[original],
            "2026-05-18",
            "2026-06-08",
        );
        value.extend_from_slice(&0x0000_3006u32.to_le_bytes());
        value.extend_from_slice(&0x0000_3009u32.to_le_bytes());
        value.extend_from_slice(&(9u32 * 60).to_le_bytes());
        value.extend_from_slice(&(10u32 * 60).to_le_bytes());
        value.extend_from_slice(&1u16.to_le_bytes());
        value.extend_from_slice(&start.to_le_bytes());
        value.extend_from_slice(&end.to_le_bytes());
        value.extend_from_slice(&original.to_le_bytes());
        value.extend_from_slice(&override_flags.to_le_bytes());
        if override_flags & 0x0001 != 0 {
            value.extend_from_slice(&((subject.len() + 1) as u16).to_le_bytes());
            value.extend_from_slice(&(subject.len() as u16).to_le_bytes());
            value.extend_from_slice(subject.as_bytes());
        }
        if override_flags & 0x0010 != 0 {
            value.extend_from_slice(&((location.len() + 1) as u16).to_le_bytes());
            value.extend_from_slice(&(location.len() as u16).to_le_bytes());
            value.extend_from_slice(location.as_bytes());
        }
        value.extend_from_slice(&0u32.to_le_bytes());
        value.extend_from_slice(&4u32.to_le_bytes());
        value.extend_from_slice(&0u32.to_le_bytes());
        value.extend_from_slice(&0u32.to_le_bytes());
        if override_flags & 0x0011 != 0 {
            value.extend_from_slice(&start.to_le_bytes());
            value.extend_from_slice(&end.to_le_bytes());
            value.extend_from_slice(&original.to_le_bytes());
            if override_flags & 0x0001 != 0 {
                append_recur_wide_string(&mut value, subject);
            }
            if override_flags & 0x0010 != 0 {
                append_recur_wide_string(&mut value, location);
            }
            value.extend_from_slice(&0u32.to_le_bytes());
        }
        value.extend_from_slice(&0u32.to_le_bytes());
        value
    }

    fn test_weekly_recur_blob_with_deleted_and_modified_instances() -> Vec<u8> {
        let deleted_only = recurrence_minutes_since_1601("2026-05-27");
        let modified = recurrence_minutes_since_1601("2026-05-25");
        let start = modified + 11 * 60;
        let end = modified + 11 * 60 + 30;
        let subject = "Changed subject";
        let location = "Room B";
        let override_flags = 0x0011u16;
        let mut value = Vec::new();
        append_recur_header(&mut value, 0x200B, 0x0001, 1);
        value.extend_from_slice(&0x0000_000Au32.to_le_bytes());
        append_recur_tail(
            &mut value,
            0x0000_2022,
            4,
            &[deleted_only, modified],
            &[modified],
            "2026-05-18",
            "2026-06-08",
        );
        value.extend_from_slice(&0x0000_3006u32.to_le_bytes());
        value.extend_from_slice(&0x0000_3009u32.to_le_bytes());
        value.extend_from_slice(&(9u32 * 60).to_le_bytes());
        value.extend_from_slice(&(10u32 * 60).to_le_bytes());
        value.extend_from_slice(&1u16.to_le_bytes());
        value.extend_from_slice(&start.to_le_bytes());
        value.extend_from_slice(&end.to_le_bytes());
        value.extend_from_slice(&modified.to_le_bytes());
        value.extend_from_slice(&override_flags.to_le_bytes());
        append_recur_ansi_string(&mut value, subject);
        append_recur_ansi_string(&mut value, location);
        value.extend_from_slice(&0u32.to_le_bytes());
        value.extend_from_slice(&4u32.to_le_bytes());
        value.extend_from_slice(&0u32.to_le_bytes());
        value.extend_from_slice(&0u32.to_le_bytes());
        value.extend_from_slice(&start.to_le_bytes());
        value.extend_from_slice(&end.to_le_bytes());
        value.extend_from_slice(&modified.to_le_bytes());
        append_recur_wide_string(&mut value, subject);
        append_recur_wide_string(&mut value, location);
        value.extend_from_slice(&0u32.to_le_bytes());
        value.extend_from_slice(&0u32.to_le_bytes());
        value
    }

    fn test_monthly_recur_blob(interval_months: u32, day: u32) -> Vec<u8> {
        let mut value = Vec::new();
        append_recur_header(&mut value, 0x200C, 0x0002, interval_months);
        value.extend_from_slice(&day.to_le_bytes());
        append_recur_tail(
            &mut value,
            0x0000_2022,
            5,
            &[],
            &[],
            "2026-05-12",
            "2027-01-12",
        );
        append_appointment_recur_suffix(&mut value, 8 * 60, 9 * 60, 0);
        value
    }

    fn test_yearly_recur_blob() -> Vec<u8> {
        let mut value = Vec::new();
        append_recur_header(&mut value, 0x200D, 0x0003, 12);
        value.extend_from_slice(&0x0000_0020u32.to_le_bytes());
        value.extend_from_slice(&5u32.to_le_bytes());
        append_recur_tail(
            &mut value,
            0x0000_2022,
            2,
            &[],
            &[],
            "2026-05-29",
            "2027-05-28",
        );
        append_appointment_recur_suffix(&mut value, 13 * 60, 14 * 60, 0);
        value
    }

    fn test_monthly_nth_recur_blob() -> Vec<u8> {
        let mut value = Vec::new();
        append_recur_header(&mut value, 0x200C, 0x0003, 1);
        value.extend_from_slice(&0x0000_0014u32.to_le_bytes());
        value.extend_from_slice(&2u32.to_le_bytes());
        append_recur_tail(
            &mut value,
            0x0000_2022,
            3,
            &[],
            &[],
            "2026-05-12",
            "2026-07-14",
        );
        append_appointment_recur_suffix(&mut value, 10 * 60, 11 * 60, 0);
        value
    }

    fn append_recur_header(value: &mut Vec<u8>, frequency: u16, pattern_type: u16, period: u32) {
        value.extend_from_slice(&0x3004u16.to_le_bytes());
        value.extend_from_slice(&0x3004u16.to_le_bytes());
        value.extend_from_slice(&frequency.to_le_bytes());
        value.extend_from_slice(&pattern_type.to_le_bytes());
        value.extend_from_slice(&0x0000u16.to_le_bytes());
        value.extend_from_slice(&recurrence_minutes_since_1601("2026-05-01").to_le_bytes());
        value.extend_from_slice(&period.to_le_bytes());
        value.extend_from_slice(&0u32.to_le_bytes());
    }

    fn append_recur_tail(
        value: &mut Vec<u8>,
        end_type: u32,
        count: u32,
        deleted: &[u32],
        modified: &[u32],
        start_date: &str,
        end_date: &str,
    ) {
        value.extend_from_slice(&end_type.to_le_bytes());
        value.extend_from_slice(&count.to_le_bytes());
        value.extend_from_slice(&0u32.to_le_bytes());
        value.extend_from_slice(&(deleted.len() as u32).to_le_bytes());
        for date in deleted {
            value.extend_from_slice(&date.to_le_bytes());
        }
        value.extend_from_slice(&(modified.len() as u32).to_le_bytes());
        for date in modified {
            value.extend_from_slice(&date.to_le_bytes());
        }
        value.extend_from_slice(&recurrence_minutes_since_1601(start_date).to_le_bytes());
        value.extend_from_slice(&recurrence_minutes_since_1601(end_date).to_le_bytes());
    }

    fn append_appointment_recur_suffix(value: &mut Vec<u8>, start: u32, end: u32, exceptions: u16) {
        value.extend_from_slice(&0x0000_3006u32.to_le_bytes());
        value.extend_from_slice(&0x0000_3009u32.to_le_bytes());
        value.extend_from_slice(&start.to_le_bytes());
        value.extend_from_slice(&end.to_le_bytes());
        value.extend_from_slice(&exceptions.to_le_bytes());
        value.extend_from_slice(&0u32.to_le_bytes());
        value.extend_from_slice(&0u32.to_le_bytes());
    }

    #[test]
    fn unsupported_property_types_fail_explicitly() {
        let result = parse_mapi_property_value(&mut Cursor::new(&[]), 0x0037_000D);

        assert!(result.is_err());
    }

    #[test]
    fn logon_omits_custom_server_icon_payloads() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@l-p-e.ch".to_string(),
            display_name: "test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };

        for tag in [PID_TAG_SERVER_CONNECTED_ICON, PID_TAG_SERVER_ACCOUNT_ICON] {
            assert_eq!(logon_property_value(&principal, tag), None);
        }
    }

    #[test]
    fn logon_projects_private_mailbox_store_flag() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@l-p-e.ch".to_string(),
            display_name: "test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };

        assert_eq!(
            logon_property_value(&principal, PID_TAG_PRIVATE),
            Some(MapiValue::Bool(true))
        );
    }

    #[test]
    fn navigation_shortcut_group_header_and_link_properties_round_trip_group_identity() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let group_id = Uuid::from_bytes([0x33; 16]);
        let header = MapiNavigationShortcutMessage {
            id: crate::mapi::identity::mapi_store_id(900),
            folder_id: COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x1111),
            subject: "Projects".to_string(),
            target_folder_id: None,
            shortcut_type: 4,
            flags: 0,
            section: 3,
            ordinal: 0x80,
            group_header_id: Some(group_id),
            group_name: "Projects".to_string(),
        };
        let link = MapiNavigationShortcutMessage {
            id: crate::mapi::identity::mapi_store_id(901),
            folder_id: COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x2222),
            subject: "Project Inbox".to_string(),
            target_folder_id: Some(INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            section: 3,
            ordinal: 0x81,
            group_header_id: Some(group_id),
            group_name: "Projects".to_string(),
        };

        assert_eq!(
            navigation_shortcut_property_value(&header, account_id, PID_TAG_WLINK_GROUP_HEADER_ID),
            Some(MapiValue::Guid([0x33; 16]))
        );
        assert_eq!(
            navigation_shortcut_property_value(&link, account_id, PID_TAG_WLINK_GROUP_CLSID),
            Some(MapiValue::Guid([0x33; 16]))
        );
        assert_eq!(
            navigation_shortcut_property_value(&link, account_id, PID_TAG_WLINK_GROUP_NAME_W),
            Some(MapiValue::String("Projects".to_string()))
        );
        assert_eq!(
            navigation_shortcut_property_value(&header, account_id, PID_TAG_WLINK_SAVE_STAMP),
            Some(MapiValue::U32(0x3333_3333))
        );
        assert_eq!(
            navigation_shortcut_property_value(&link, account_id, PID_TAG_WLINK_SAVE_STAMP),
            Some(MapiValue::U32(0x3333_3333))
        );
    }

    #[test]
    fn navigation_shortcut_projects_associated_table_identity_columns() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let shortcut = MapiNavigationShortcutMessage {
            id: crate::mapi::identity::mapi_store_id(901),
            folder_id: COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x2222),
            subject: "Project Inbox".to_string(),
            target_folder_id: Some(INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            section: 3,
            ordinal: 0x81,
            group_header_id: None,
            group_name: "Projects".to_string(),
        };

        assert_eq!(
            navigation_shortcut_property_value(&shortcut, account_id, PID_TAG_FOLDER_ID),
            Some(MapiValue::U64(COMMON_VIEWS_FOLDER_ID))
        );
        assert_eq!(
            navigation_shortcut_property_value(&shortcut, account_id, PID_TAG_INST_ID),
            Some(MapiValue::U64(shortcut.id))
        );
        assert_eq!(
            navigation_shortcut_property_value(&shortcut, account_id, PID_TAG_INSTANCE_NUM),
            Some(MapiValue::U32(0))
        );
        let expected_entry_id = crate::mapi::identity::message_entry_id_from_object_ids(
            account_id,
            COMMON_VIEWS_FOLDER_ID,
            shortcut.id,
        )
        .unwrap();
        assert_eq!(
            navigation_shortcut_property_value(&shortcut, account_id, PID_TAG_ENTRY_ID),
            Some(MapiValue::Binary(expected_entry_id))
        );
        assert_eq!(
            navigation_shortcut_property_value(&shortcut, account_id, PID_TAG_INSTANCE_KEY),
            Some(MapiValue::Binary(
                crate::mapi::identity::instance_key_for_object_id(shortcut.id)
            ))
        );
        assert_eq!(
            navigation_shortcut_property_value(&shortcut, account_id, PID_TAG_RECORD_KEY),
            Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
                shortcut.id
            )))
        );
    }

    #[test]
    fn common_view_named_view_projects_descriptor_properties_for_outlook() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let view = MapiCommonViewNamedViewMessage {
            id: crate::mapi::identity::mapi_store_id(0x7fff_ffff_fff7),
            folder_id: COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x11111111111111111111111111111111),
            name: "Messages".to_string(),
            view_flags: 0,
            view_type: 8,
        };

        let Some(MapiValue::Binary(descriptor)) = common_view_named_view_property_value(
            &view,
            account_id,
            PID_TAG_VIEW_DESCRIPTOR_BINARY,
        ) else {
            panic!("expected PidTagViewDescriptorBinary");
        };
        assert_eq!(
            common_view_named_view_property_value(
                &view,
                account_id,
                OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835,
            ),
            Some(MapiValue::Binary(descriptor.clone()))
        );
        assert_eq!(descriptor.len(), 96);
        assert_eq!(&descriptor[8..12], &8u32.to_le_bytes());
        assert_eq!(&descriptor[20..24], &1u32.to_le_bytes());
        assert_eq!(&descriptor[24..28], &u32::MAX.to_le_bytes());
        assert_eq!(&descriptor[60..62], &1u16.to_le_bytes());
        assert_eq!(&descriptor[62..64], &4u16.to_le_bytes());
        assert_eq!(&descriptor[64..68], &7u32.to_le_bytes());
        assert_eq!(&descriptor[72..76], &0x28u32.to_le_bytes());
        assert_eq!(&descriptor[88..92], &0u32.to_le_bytes());
        assert_eq!(&descriptor[92..96], &4u32.to_le_bytes());
        assert_eq!(
            common_view_named_view_property_value(
                &view,
                account_id,
                OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B,
            ),
            Some(MapiValue::Binary(Vec::new()))
        );
    }

    #[test]
    fn mapi_mailbox_display_name_normalizes_canonical_inbox() {
        let inbox = mailbox(
            "11111111-1111-1111-1111-111111111111",
            None,
            "inbox",
            "INBOX",
        );
        let custom = mailbox(
            "22222222-2222-2222-2222-222222222222",
            None,
            "",
            "INBOX Reports",
        );

        assert_eq!(mapi_mailbox_display_name(&inbox), "Inbox");
        assert_eq!(
            mailbox_property_value_with_context(&inbox, &[], PID_TAG_DISPLAY_NAME_W),
            Some(MapiValue::String("Inbox".to_string()))
        );
        assert_eq!(mapi_mailbox_display_name(&custom), "INBOX Reports");
    }

    #[test]
    fn sharing_local_folder_id_named_property_maps_to_outlook_id() {
        assert_eq!(
            well_known_named_property_id(&MapiNamedProperty {
                guid: PSETID_SHARING_GUID,
                kind: MapiNamedPropertyKind::Name(
                    "SharingCalendarGroupEntryAssociatedLocalFolderId".to_string(),
                ),
            }),
            Some(0x8010)
        );
    }

    #[test]
    fn navigation_shortcut_projects_sharing_local_folder_id() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let shortcut = MapiNavigationShortcutMessage {
            id: crate::mapi::identity::mapi_store_id(901),
            folder_id: COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x2222),
            subject: "Project Inbox".to_string(),
            target_folder_id: Some(INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            section: 3,
            ordinal: 0x81,
            group_header_id: None,
            group_name: "Projects".to_string(),
        };
        let expected =
            crate::mapi::identity::folder_entry_id_from_object_id(account_id, INBOX_FOLDER_ID)
                .unwrap();

        assert_eq!(
            navigation_shortcut_property_value(
                &shortcut,
                account_id,
                PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG,
            ),
            Some(MapiValue::Binary(expected))
        );
    }

    #[test]
    fn navigation_shortcut_projects_address_book_store_entry_id() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let shortcut = MapiNavigationShortcutMessage {
            id: crate::mapi::identity::mapi_store_id(901),
            folder_id: COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x2222),
            subject: "Inbox".to_string(),
            target_folder_id: Some(INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            section: 1,
            ordinal: 0x10,
            group_header_id: None,
            group_name: "Mail".to_string(),
        };

        assert_eq!(
            navigation_shortcut_property_value(
                &shortcut,
                account_id,
                PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,
            ),
            Some(MapiValue::Binary(mapi_mailstore::private_store_entry_id(
                account_id
            )))
        );
    }

    #[test]
    fn navigation_shortcut_wlink_guid_fields_follow_requested_property_type() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let group_id = Uuid::from_bytes([0x33; 16]);
        let header = MapiNavigationShortcutMessage {
            id: crate::mapi::identity::mapi_store_id(900),
            folder_id: COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x1111),
            subject: "Projects".to_string(),
            target_folder_id: None,
            shortcut_type: 4,
            flags: 0,
            section: 3,
            ordinal: 0x80,
            group_header_id: Some(group_id),
            group_name: "Projects".to_string(),
        };
        let link = MapiNavigationShortcutMessage {
            id: crate::mapi::identity::mapi_store_id(901),
            folder_id: COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x2222),
            subject: "Project Inbox".to_string(),
            target_folder_id: Some(INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            section: 3,
            ordinal: 0x81,
            group_header_id: Some(group_id),
            group_name: "Projects".to_string(),
        };
        let calendar_link = MapiNavigationShortcutMessage {
            id: crate::mapi::identity::mapi_store_id(902),
            folder_id: COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x3333),
            subject: "Project Calendar".to_string(),
            target_folder_id: Some(CALENDAR_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            section: 3,
            ordinal: 0x82,
            group_header_id: Some(group_id),
            group_name: "Projects".to_string(),
        };

        assert_eq!(
            navigation_shortcut_property_value(&header, account_id, 0x6842_0102),
            Some(MapiValue::Binary([0x33; 16].to_vec()))
        );
        assert_eq!(
            navigation_shortcut_property_value(&link, account_id, 0x6850_0102),
            Some(MapiValue::Binary([0x33; 16].to_vec()))
        );
        assert_eq!(
            navigation_shortcut_property_value(&link, account_id, 0x684F_0102),
            Some(MapiValue::Binary(wlink_folder_type_guid(&link).to_vec()))
        );
        assert_eq!(
            navigation_shortcut_property_value(&link, account_id, PID_TAG_WLINK_FOLDER_TYPE),
            Some(MapiValue::Guid(wlink_folder_type_guid(&link)))
        );
        assert_eq!(
            navigation_shortcut_property_value(&link, account_id, 0x684F_0102),
            Some(MapiValue::Binary(
                [
                    0x0C, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x46,
                ]
                .to_vec()
            ))
        );
        assert_eq!(
            navigation_shortcut_property_value(&calendar_link, account_id, 0x684F_0102),
            Some(MapiValue::Binary(
                [
                    0x02, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x46,
                ]
                .to_vec()
            ))
        );
    }

    #[test]
    fn logon_projects_outlook_bootstrap_identity_metadata() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@l-p-e.ch".to_string(),
            display_name: "Test User".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };

        assert_eq!(
            logon_property_value(&principal, PID_TAG_OUTLOOK_STORE_STATE),
            Some(MapiValue::U32(0))
        );
        assert_eq!(
            logon_property_value(&principal, PID_TAG_RESOURCE_FLAGS),
            Some(MapiValue::U32(0))
        );
        assert_eq!(
            logon_property_value(&principal, PID_TAG_MAILBOX_OWNER_NAME_W),
            Some(MapiValue::String("Test User".to_string()))
        );
        assert_eq!(
            logon_property_value(&principal, PID_TAG_ASSOCIATED_SHARING_PROVIDER),
            Some(MapiValue::Guid(OUTLOOK_SHARING_PROVIDER_GUID))
        );
        assert_eq!(
            logon_property_value(&principal, PID_TAG_USER_GUID),
            Some(MapiValue::Binary(principal.account_id.as_bytes().to_vec()))
        );
        let Some(MapiValue::Binary(owner_entry_id)) =
            logon_property_value(&principal, PID_TAG_MAILBOX_OWNER_ENTRY_ID)
        else {
            panic!("expected mailbox owner EntryID");
        };
        assert_eq!(&owner_entry_id[..4], &[0, 0, 0, 0]);
        assert_eq!(
            &owner_entry_id[4..20],
            &NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID
        );
        assert!(owner_entry_id.ends_with(&[0]));
        assert_eq!(
            logon_property_value(&principal, PID_TAG_USER_ENTRY_ID),
            Some(MapiValue::Binary(owner_entry_id))
        );
        let Some(MapiValue::Binary(public_folder_entry_id)) =
            logon_property_value(&principal, PID_TAG_IPM_PUBLIC_FOLDERS_ENTRY_ID)
        else {
            panic!("expected public folders EntryID");
        };
        assert_eq!(
            crate::mapi::identity::object_id_from_folder_entry_id(&public_folder_entry_id),
            Some(PUBLIC_FOLDERS_ROOT_FOLDER_ID)
        );
    }

    #[test]
    fn logon_projects_max_submit_message_size() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@l-p-e.ch".to_string(),
            display_name: "test".to_string(),
            quota_mb: Some(4096),
            quota_used_octets: Some(12_345),
        };

        assert_eq!(
            logon_property_value(&principal, PID_TAG_MAX_SUBMIT_MESSAGE_SIZE),
            Some(MapiValue::U32(35 * 1024))
        );
        assert_eq!(
            logon_property_value(&principal, PID_TAG_MESSAGE_SIZE_EXTENDED),
            Some(MapiValue::I64(12_345))
        );
        assert_eq!(
            logon_property_value(&principal, PID_TAG_PROHIBIT_RECEIVE_QUOTA),
            Some(MapiValue::U32(4096 * 1024))
        );
        assert_eq!(
            logon_property_value(&principal, PID_TAG_PROHIBIT_SEND_QUOTA),
            Some(MapiValue::U32(4096 * 1024))
        );
        assert_eq!(
            logon_property_value(&principal, PID_TAG_STORAGE_QUOTA_LIMIT),
            Some(MapiValue::U32(4096 * 1024))
        );
        assert_eq!(
            logon_property_value(&principal, PID_TAG_PST_PATH_W),
            Some(MapiValue::String(String::new()))
        );
    }
}
