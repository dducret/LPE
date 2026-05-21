use super::rop::*;
use super::session::*;
use super::sync::*;
use super::tables::*;
use super::wire::MapiPropertyType;
use super::*;
use crate::mapi_store::{MapiMessage, MapiSearchFolderDefinitionMessage};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(in crate::mapi) struct MapiNamedProperty {
    pub(in crate::mapi) guid: [u8; 16],
    pub(in crate::mapi) kind: MapiNamedPropertyKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(in crate::mapi) enum MapiNamedPropertyKind {
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

pub(in crate::mapi) fn canonical_property_storage_tag(property_tag: u32) -> u32 {
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
pub(in crate::mapi) const PID_TAG_VALID_FOLDER_MASK: u32 = 0x35DF_0003;
pub(in crate::mapi) const PID_TAG_IPM_APPOINTMENT_ENTRY_ID: u32 = 0x36D0_0102;
pub(in crate::mapi) const PID_TAG_IPM_CONTACT_ENTRY_ID: u32 = 0x36D1_0102;
pub(in crate::mapi) const PID_TAG_IPM_JOURNAL_ENTRY_ID: u32 = 0x36D2_0102;
pub(in crate::mapi) const PID_TAG_IPM_NOTE_ENTRY_ID: u32 = 0x36D3_0102;
pub(in crate::mapi) const PID_TAG_IPM_TASK_ENTRY_ID: u32 = 0x36D4_0102;
pub(in crate::mapi) const PID_TAG_REM_ONLINE_ENTRY_ID: u32 = 0x36D5_0102;
pub(in crate::mapi) const PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX: u32 = 0x36D9_0102;
pub(in crate::mapi) const PID_TAG_HIER_REV: u32 = 0x4082_0040;
pub(in crate::mapi) const PID_TAG_FOLDER_ID: u32 = 0x6748_0014;
pub(in crate::mapi) const PID_TAG_PARENT_FOLDER_ID: u32 = 0x6749_0014;
pub(in crate::mapi) const PID_TAG_MESSAGE_CLASS_W: u32 = 0x001A_001F;
pub(in crate::mapi) const PID_TAG_SUBJECT_W: u32 = 0x0037_001F;
pub(in crate::mapi) const PID_TAG_SENDER_NAME_W: u32 = 0x0C1A_001F;
pub(in crate::mapi) const PID_TAG_SENDER_EMAIL_ADDRESS_W: u32 = 0x0C1F_001F;
pub(in crate::mapi) const PID_TAG_RECIPIENT_TYPE: u32 = 0x0C15_0003;
pub(in crate::mapi) const PID_TAG_DISPLAY_TO_W: u32 = 0x0E04_001F;
pub(in crate::mapi) const PID_TAG_MESSAGE_DELIVERY_TIME: u32 = 0x0E06_0040;
pub(in crate::mapi) const PID_TAG_MESSAGE_FLAGS: u32 = 0x0E07_0003;
pub(in crate::mapi) const PID_TAG_MESSAGE_SIZE: u32 = 0x0E08_0003;
pub(in crate::mapi) const PID_TAG_HAS_ATTACHMENTS: u32 = 0x0E1B_000B;
pub(in crate::mapi) const PID_TAG_NORMALIZED_SUBJECT_W: u32 = 0x0E1D_001F;
pub(in crate::mapi) const PID_TAG_ACCESS: u32 = 0x0FF4_0003;
pub(in crate::mapi) const PID_TAG_INSTANCE_KEY: u32 = 0x0FF6_0102;
pub(in crate::mapi) const PID_TAG_ENTRY_ID: u32 = 0x0FFF_0102;
pub(in crate::mapi) const PID_TAG_BODY_STRING8: u32 = 0x1000_001E;
pub(in crate::mapi) const PID_TAG_BODY_W: u32 = 0x1000_001F;
pub(in crate::mapi) const PID_TAG_BODY_HTML_W: u32 = 0x1013_001F;

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
pub(in crate::mapi) const PID_TAG_LAST_MODIFICATION_TIME: u32 = 0x3008_0040;
pub(in crate::mapi) const PID_TAG_HIERARCHY_CHANGE_NUMBER: u32 = 0x663E_0003;
pub(in crate::mapi) const PID_TAG_SOURCE_KEY: u32 = 0x65E0_0102;
pub(in crate::mapi) const PID_TAG_PARENT_SOURCE_KEY: u32 = 0x65E1_0102;
pub(in crate::mapi) const PID_TAG_CHANGE_KEY: u32 = 0x65E2_0102;
pub(in crate::mapi) const PID_TAG_PREDECESSOR_CHANGE_LIST: u32 = 0x65E3_0102;
pub(in crate::mapi) const PID_TAG_LOCAL_COMMIT_TIME: u32 = 0x6709_0040;
pub(in crate::mapi) const PID_TAG_LOCAL_COMMIT_TIME_MAX: u32 = 0x670A_0040;
pub(in crate::mapi) const PID_TAG_SERIALIZED_REPLID_GUID_MAP: u32 = 0x6638_0102;
pub(in crate::mapi) const PID_TAG_MAILBOX_OWNER_ENTRY_ID: u32 = 0x661B_0102;
pub(in crate::mapi) const PID_TAG_MAILBOX_OWNER_NAME_W: u32 = 0x661C_001F;
pub(in crate::mapi) const PID_TAG_SERVER_TYPE_DISPLAY_NAME_W: u32 = 0x341D_001F;
pub(in crate::mapi) const PID_TAG_SERVER_CONNECTED_ICON: u32 = 0x341E_0102;
pub(in crate::mapi) const PID_TAG_SERVER_ACCOUNT_ICON: u32 = 0x341F_0102;
pub(in crate::mapi) const PID_TAG_OUTLOOK_STORE_STATE: u32 = 0x346F_0003;
pub(in crate::mapi) const PID_TAG_PRIVATE: u32 = 0x0E5C_000B;
pub(in crate::mapi) const PID_TAG_USER_GUID: u32 = 0x6707_0102;
pub(in crate::mapi) const PID_TAG_OST_OSTID: u32 = 0x7C04_0102;
pub(in crate::mapi) const PID_TAG_MID: u32 = 0x674A_0014;
pub(in crate::mapi) const PID_TAG_CHANGE_NUMBER: u32 = 0x67A4_0014;
pub(in crate::mapi) const PID_TAG_ASSOCIATED: u32 = 0x67AA_000B;
pub(in crate::mapi) const PID_TAG_SEARCH_FOLDER_STORAGE_TYPE: u32 = 0x6842_0003;
pub(in crate::mapi) const PID_TAG_SEARCH_FOLDER_EFP_FLAGS: u32 = 0x6844_0003;
pub(in crate::mapi) const PID_TAG_SEARCH_FOLDER_DEFINITION: u32 = 0x6845_0102;
pub(in crate::mapi) const PID_TAG_SEARCH_FOLDER_TAG: u32 = 0x6847_0003;
pub(in crate::mapi) const PID_TAG_ATTACH_DATA_BINARY: u32 = 0x3701_0102;
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
pub(in crate::mapi) const FIRST_NAMED_PROPERTY_ID: u16 = 0x8001;
pub(in crate::mapi) const MAX_NAMED_PROPERTY_ID: u16 = 0xFFFE;
pub(in crate::mapi) const PS_MAPI_GUID: [u8; 16] = [
    0x28, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];
pub(in crate::mapi) const PS_INTERNET_HEADERS_GUID: [u8; 16] = [
    0x86, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];
pub(in crate::mapi) const PSETID_COMMON_GUID: [u8; 16] = [
    0x08, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
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
pub(in crate::mapi) const PSETID_POST_RSS_GUID: [u8; 16] = [
    0x41, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];

pub(in crate::mapi) const PID_LID_COMMON_START: u32 = 0x0000_8516;
pub(in crate::mapi) const PID_LID_COMMON_END: u32 = 0x0000_8517;
pub(in crate::mapi) const PID_LID_REMINDER_TIME: u32 = 0x0000_8502;
pub(in crate::mapi) const PID_LID_REMINDER_SET: u32 = 0x0000_8503;
pub(in crate::mapi) const PID_LID_FLAG_REQUEST: u32 = 0x0000_8530;
pub(in crate::mapi) const PID_LID_REMINDER_SIGNAL_TIME: u32 = 0x0000_8560;
pub(in crate::mapi) const PID_LID_TASK_START_DATE: u32 = 0x0000_8104;
pub(in crate::mapi) const PID_LID_TASK_DUE_DATE: u32 = 0x0000_8105;
pub(in crate::mapi) const PID_LID_COMPANIES: u32 = 0x0000_8539;
pub(in crate::mapi) const PID_LID_CONTACTS: u32 = 0x0000_853A;
pub(in crate::mapi) const PID_LID_CONTACT_LINK_NAME: u32 = 0x0000_8586;
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
pub(in crate::mapi) const PID_LID_REMINDER_TIME_TAG: u32 = 0x8502_0040;
pub(in crate::mapi) const PID_LID_REMINDER_SET_TAG: u32 = 0x8503_000B;
pub(in crate::mapi) const PID_LID_FLAG_REQUEST_W_TAG: u32 = 0x8530_001F;
pub(in crate::mapi) const PID_LID_REMINDER_SIGNAL_TIME_TAG: u32 = 0x8560_0040;
pub(in crate::mapi) const PID_LID_TASK_START_DATE_TAG: u32 = 0x8104_0040;
pub(in crate::mapi) const PID_LID_TASK_DUE_DATE_TAG: u32 = 0x8105_0040;
pub(in crate::mapi) const PID_LID_COMPANIES_TAG: u32 = 0x8539_101F;
pub(in crate::mapi) const PID_LID_CONTACTS_TAG: u32 = 0x853A_101F;
pub(in crate::mapi) const PID_LID_CONTACT_LINK_NAME_W_TAG: u32 = 0x8586_001F;
pub(in crate::mapi) const PID_LID_CONTACT_LINK_NAME_STRING8_TAG: u32 = 0x8586_001E;
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

fn well_known_named_properties() -> Vec<(u16, MapiNamedProperty)> {
    [
        (PID_LID_COMMON_START, PSETID_COMMON_GUID),
        (PID_LID_COMMON_END, PSETID_COMMON_GUID),
        (PID_LID_REMINDER_TIME, PSETID_COMMON_GUID),
        (PID_LID_REMINDER_SET, PSETID_COMMON_GUID),
        (PID_LID_FLAG_REQUEST, PSETID_COMMON_GUID),
        (PID_LID_REMINDER_SIGNAL_TIME, PSETID_COMMON_GUID),
        (PID_LID_TASK_START_DATE, PSETID_TASK_GUID),
        (PID_LID_TASK_DUE_DATE, PSETID_TASK_GUID),
        (PID_LID_COMPANIES, PSETID_COMMON_GUID),
        (PID_LID_CONTACTS, PSETID_COMMON_GUID),
        (PID_LID_CONTACT_LINK_NAME, PSETID_COMMON_GUID),
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
    .map(|(lid, guid)| {
        (
            lid as u16,
            MapiNamedProperty {
                guid,
                kind: MapiNamedPropertyKind::Lid(lid),
            },
        )
    })
    .collect()
}

const NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID: [u8; 16] = [
    0xDC, 0xA7, 0x40, 0xC8, 0xC0, 0x42, 0x10, 0x1A, 0xB4, 0xB9, 0x08, 0x00, 0x2B, 0x2F, 0xE1, 0x82,
];

const FOLDER_IPM_SUBTREE_VALID: u32 = 0x0000_0001;
const FOLDER_IPM_INBOX_VALID: u32 = 0x0000_0002;
const FOLDER_IPM_OUTBOX_VALID: u32 = 0x0000_0004;
const FOLDER_IPM_WASTEBASKET_VALID: u32 = 0x0000_0008;
const FOLDER_IPM_SENTMAIL_VALID: u32 = 0x0000_0010;
const FOLDER_VIEWS_VALID: u32 = 0x0000_0020;
const FOLDER_COMMON_VIEWS_VALID: u32 = 0x0000_0040;

pub(in crate::mapi) fn reminders_experiment_enabled() -> bool {
    std::env::var("LPE_MAPI_EXPERIMENT_REMINDERS_ENABLED")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
        .unwrap_or(false)
}

pub(in crate::mapi) fn logon_property_value(
    principal: &AccountPrincipal,
    property_tag: u32,
) -> Option<MapiValue> {
    match property_tag {
        PID_TAG_SERIALIZED_REPLID_GUID_MAP => Some(MapiValue::Binary(serialized_replid_guid_map())),
        PID_TAG_VALID_FOLDER_MASK => Some(MapiValue::U32(valid_folder_mask())),
        PID_TAG_MAILBOX_OWNER_ENTRY_ID => {
            Some(MapiValue::Binary(mailbox_owner_entry_id(principal)))
        }
        PID_TAG_MAILBOX_OWNER_NAME_W => Some(MapiValue::String(principal.display_name.clone())),
        PID_TAG_SERVER_TYPE_DISPLAY_NAME_W => Some(MapiValue::String("LPE".to_string())),
        PID_TAG_SERVER_CONNECTED_ICON | PID_TAG_SERVER_ACCOUNT_ICON => {
            Some(MapiValue::Binary(server_status_icon()))
        }
        PID_TAG_OUTLOOK_STORE_STATE => Some(MapiValue::U32(0)),
        PID_TAG_PRIVATE => Some(MapiValue::Bool(false)),
        PID_TAG_USER_GUID => Some(MapiValue::Binary(principal.account_id.as_bytes().to_vec())),
        _ => special_folder_identification_property_value(principal.account_id, property_tag),
    }
}

fn server_status_icon() -> Vec<u8> {
    const WIDTH: u8 = 16;
    const HEIGHT: u8 = 16;
    const PIXEL_BYTES: u32 = WIDTH as u32 * HEIGHT as u32 * 4;
    const MASK_BYTES: u32 = HEIGHT as u32 * 4;
    const IMAGE_BYTES: u32 = 40 + PIXEL_BYTES + MASK_BYTES;
    const IMAGE_OFFSET: u32 = 22;

    let mut value = Vec::with_capacity((IMAGE_OFFSET + IMAGE_BYTES) as usize);
    value.extend_from_slice(&0u16.to_le_bytes());
    value.extend_from_slice(&1u16.to_le_bytes());
    value.extend_from_slice(&1u16.to_le_bytes());
    value.extend_from_slice(&[WIDTH, HEIGHT, 0, 0]);
    value.extend_from_slice(&1u16.to_le_bytes());
    value.extend_from_slice(&32u16.to_le_bytes());
    value.extend_from_slice(&IMAGE_BYTES.to_le_bytes());
    value.extend_from_slice(&IMAGE_OFFSET.to_le_bytes());

    value.extend_from_slice(&40u32.to_le_bytes());
    value.extend_from_slice(&(WIDTH as i32).to_le_bytes());
    value.extend_from_slice(&((HEIGHT as i32) * 2).to_le_bytes());
    value.extend_from_slice(&1u16.to_le_bytes());
    value.extend_from_slice(&32u16.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&PIXEL_BYTES.to_le_bytes());
    value.extend_from_slice(&0i32.to_le_bytes());
    value.extend_from_slice(&0i32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());

    for y in 0..HEIGHT {
        for x in 0..WIDTH {
            let border = x == 0 || y == 0 || x == WIDTH - 1 || y == HEIGHT - 1;
            let diagonal = x == y || x + y == WIDTH - 1;
            let (blue, green, red) = if border || diagonal {
                (0xFF, 0xFF, 0xFF)
            } else {
                (0x76, 0x99, 0x22)
            };
            value.extend_from_slice(&[blue, green, red, 0xFF]);
        }
    }
    value.extend(std::iter::repeat(0).take(MASK_BYTES as usize));
    value
}

pub(in crate::mapi) fn special_folder_identification_property_value(
    _mailbox_guid: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    special_folder_identification_property_value_with_reminders(
        _mailbox_guid,
        property_tag,
        reminders_experiment_enabled(),
    )
}

fn special_folder_identification_property_value_with_reminders(
    _mailbox_guid: Uuid,
    property_tag: u32,
    reminders_enabled: bool,
) -> Option<MapiValue> {
    match canonical_property_storage_tag(property_tag) {
        PID_TAG_VALID_FOLDER_MASK => Some(MapiValue::U32(valid_folder_mask())),
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID => Some(special_folder_long_term_id(CALENDAR_FOLDER_ID)),
        PID_TAG_IPM_CONTACT_ENTRY_ID => Some(special_folder_long_term_id(CONTACTS_FOLDER_ID)),
        PID_TAG_IPM_JOURNAL_ENTRY_ID => Some(special_folder_long_term_id(JOURNAL_FOLDER_ID)),
        PID_TAG_IPM_NOTE_ENTRY_ID => Some(special_folder_long_term_id(NOTES_FOLDER_ID)),
        PID_TAG_IPM_TASK_ENTRY_ID => Some(special_folder_long_term_id(TASKS_FOLDER_ID)),
        PID_TAG_REM_ONLINE_ENTRY_ID if reminders_enabled => {
            Some(special_folder_long_term_id(REMINDERS_FOLDER_ID))
        }
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX => Some(MapiValue::Binary(
            additional_ren_entry_ids_ex(_mailbox_guid),
        )),
        _ => None,
    }
}

pub(in crate::mapi) fn is_default_folder_identification_property_tag(property_tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(property_tag),
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID
            | PID_TAG_IPM_CONTACT_ENTRY_ID
            | PID_TAG_IPM_JOURNAL_ENTRY_ID
            | PID_TAG_IPM_NOTE_ENTRY_ID
            | PID_TAG_IPM_TASK_ENTRY_ID
            | PID_TAG_REM_ONLINE_ENTRY_ID
            | PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX
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
}

fn special_folder_long_term_id(folder_id: u64) -> MapiValue {
    MapiValue::Binary(
        crate::mapi::identity::long_term_id_from_object_id(folder_id)
            .unwrap()
            .to_vec(),
    )
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
    ];
    let mut value = Vec::new();
    for (persist_id, folder_id) in entries {
        let entry_id = special_folder_entry_id(mailbox_guid, folder_id);
        let data_size = 16usize.saturating_add(entry_id.len());
        value.extend_from_slice(&persist_id.to_le_bytes());
        value.extend_from_slice(&(data_size.min(u16::MAX as usize) as u16).to_le_bytes());
        value.extend_from_slice(&0x0002u16.to_le_bytes());
        value.extend_from_slice(&4u16.to_le_bytes());
        value.extend_from_slice(&0u32.to_le_bytes());
        value.extend_from_slice(&0x0001u16.to_le_bytes());
        value.extend_from_slice(&(entry_id.len().min(u16::MAX as usize) as u16).to_le_bytes());
        value.extend_from_slice(&entry_id);
        value.extend_from_slice(&0u16.to_le_bytes());
        value.extend_from_slice(&0u16.to_le_bytes());
    }
    value.extend_from_slice(&0u16.to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
    value
}

fn special_folder_entry_id(mailbox_guid: Uuid, folder_id: u64) -> Vec<u8> {
    let long_term_id = crate::mapi::identity::long_term_id_from_object_id(folder_id)
        .expect("special folders use valid MAPI folder IDs");
    let mut value = Vec::with_capacity(46);
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&mailbox_guid.to_bytes_le());
    value.extend_from_slice(&1u16.to_le_bytes());
    value.extend_from_slice(&long_term_id[..16]);
    value.extend_from_slice(&long_term_id[16..22]);
    value.extend_from_slice(&0u16.to_le_bytes());
    value
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

    match object {
        Some(MapiObject::Message {
            folder_id,
            message_id,
        }) => {
            let Some(email) =
                message_for_id(*folder_id, *message_id, mailboxes, emails).or_else(|| {
                    search_folder_message_for_id(snapshot, *folder_id, *message_id)
                        .map(|message| &message.email)
                })
            else {
                return rop_error_response(0x0F, input_handle_index, 0x8004_010F);
            };
            let recipients = message_recipients(email);
            if start >= recipients.len() {
                return rop_error_response(0x0F, input_handle_index, 0x8004_010F);
            }
            for (offset, recipient) in recipients.into_iter().enumerate().skip(start) {
                write_u32(&mut response, offset as u32);
                response.push(recipient.recipient_type);
                response.extend_from_slice(&0x0FFFu16.to_le_bytes());
                response.extend_from_slice(&0u16.to_le_bytes());
                let row = serialize_recipient_row(recipient.address);
                response.extend_from_slice(&(row.len() as u16).to_le_bytes());
                response.extend_from_slice(&row);
            }
        }
        Some(MapiObject::PendingMessage { recipients, .. }) => {
            if start >= recipients.len() {
                return rop_error_response(0x0F, input_handle_index, 0x8004_010F);
            }
            for recipient in recipients.iter().skip(start) {
                write_u32(&mut response, recipient.row_id);
                response.push(recipient.recipient_type);
                response.extend_from_slice(&0x0FFFu16.to_le_bytes());
                response.extend_from_slice(&0u16.to_le_bytes());
                let row = serialize_pending_recipient_row(recipient);
                response.extend_from_slice(&(row.len() as u16).to_le_bytes());
                response.extend_from_slice(&row);
            }
        }
        _ => return rop_error_response(0x0F, input_handle_index, 0x0000_04B9),
    }
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
        _ => None,
    }
}

pub(in crate::mapi) fn restriction_matches_mailbox(
    restriction: Option<&MapiRestriction>,
    mailbox: &JmapMailbox,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        mailbox_property_value(mailbox, property_tag)
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

pub(in crate::mapi) fn mailbox_property_value(
    mailbox: &JmapMailbox,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    if mailbox.role == "inbox" {
        if let Some(value) = special_folder_identification_property_value(Uuid::nil(), property_tag)
        {
            return Some(value);
        }
    }
    match property_tag {
        PID_TAG_DISPLAY_NAME_W => Some(MapiValue::String(mailbox.name.clone())),
        PID_TAG_CONTENT_COUNT => Some(MapiValue::U32(mailbox.total_emails)),
        PID_TAG_CONTENT_UNREAD_COUNT => Some(MapiValue::U32(mailbox.unread_emails)),
        PID_TAG_SUBFOLDERS => Some(MapiValue::Bool(false)),
        PID_TAG_FOLDER_TYPE => Some(MapiValue::U32(if mailbox.role == "__mapi_search" {
            FOLDER_SEARCH
        } else {
            FOLDER_GENERIC
        })),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_FOLDER_ACCESS)),
        PID_TAG_CONTAINER_CLASS_W => Some(MapiValue::String(folder_message_class(mailbox).into())),
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(mapi_folder_id(mailbox))),
        PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME
        | PID_TAG_LOCAL_COMMIT_TIME_MAX
        | PID_TAG_HIER_REV => Some(MapiValue::U64(mapi_mailstore::filetime_from_change_number(
            mapi_mailstore::canonical_folder_change_number(mailbox),
        ))),
        PID_TAG_HIERARCHY_CHANGE_NUMBER => Some(MapiValue::U32(
            mapi_mailstore::canonical_folder_change_number(mailbox).min(u64::from(u32::MAX)) as u32,
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_mailbox_folder(mailbox),
        )),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(IPM_SUBTREE_FOLDER_ID),
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
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(folder.id)),
        PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME
        | PID_TAG_LOCAL_COMMIT_TIME_MAX
        | PID_TAG_HIER_REV => Some(MapiValue::U64(mapi_mailstore::filetime_from_change_number(
            change_number,
        ))),
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
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
            Some(MapiValue::String(email.subject.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(
            if email.mailbox_role == "rss_feeds" {
                "IPM.Post.RSS"
            } else {
                "IPM.Note"
            }
            .to_string(),
        )),
        PID_TAG_MESSAGE_DELIVERY_TIME
        | PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_rfc3339_utc(&email.received_at),
        )),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(message_flags(email))),
        PID_TAG_FLAG_STATUS => Some(MapiValue::U32(mapi_mailstore::canonical_flag_status(email))),
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
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I64(email.size_octets)),
        PID_TAG_SENDER_NAME_W => Some(MapiValue::String(
            email
                .from_display
                .clone()
                .unwrap_or_else(|| email.from_address.clone()),
        )),
        PID_TAG_SENDER_EMAIL_ADDRESS_W => Some(MapiValue::String(email.from_address.clone())),
        PID_TAG_DISPLAY_TO_W => Some(MapiValue::String(display_to(email))),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(email.has_attachments)),
        PID_TAG_BODY_W => Some(MapiValue::String(email.body_text.clone())),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => {
            let object_id = mapi_message_id(email);
            Some(MapiValue::Binary(
                crate::mapi::identity::instance_key_for_object_id(object_id),
            ))
        }
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
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
        _ => None,
    }
}

pub(in crate::mapi) fn search_folder_definition_property_value(
    message: &MapiSearchFolderDefinitionMessage,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    let definition = &message.definition;
    match property_tag {
        PID_TAG_MID => Some(MapiValue::U64(message.id)),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(message.id),
        )),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(definition.display_name.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(
            "IPM.Microsoft.WunderBar.SFInfo".to_string(),
        )),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I32(
            search_folder_definition_blob(definition)
                .len()
                .min(i32::MAX as usize) as i32,
        )),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_ASSOCIATED => Some(MapiValue::Bool(true)),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
        PID_TAG_SOURCE_KEY | PID_TAG_CHANGE_KEY | PID_TAG_PREDECESSOR_CHANGE_LIST => Some(
            MapiValue::Binary(mapi_mailstore::source_key_for_store_id(message.id)),
        ),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(message.folder_id),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(message.id & 0x00FF_FFFF_FFFF_FFFF)),
        PID_TAG_SEARCH_FOLDER_STORAGE_TYPE => Some(MapiValue::I32(0x0000_0040)),
        PID_TAG_SEARCH_FOLDER_EFP_FLAGS => Some(MapiValue::I32(0)),
        PID_TAG_SEARCH_FOLDER_TAG => {
            Some(MapiValue::I32(search_folder_tag(definition.role.as_str())))
        }
        PID_TAG_SEARCH_FOLDER_DEFINITION => {
            Some(MapiValue::Binary(search_folder_definition_blob(definition)))
        }
        _ => None,
    }
}

fn search_folder_tag(role: &str) -> i32 {
    match role {
        "reminders" => 1,
        "todo_search" => 2,
        "contacts_search" => 3,
        "tracked_mail_processing" => 4,
        _ => 0,
    }
}

fn search_folder_definition_blob(definition: &lpe_storage::SearchFolderDefinition) -> Vec<u8> {
    let mut buffer = Vec::new();
    buffer.extend_from_slice(&0x0410_0000u32.to_be_bytes());
    buffer.extend_from_slice(&0x0000_0040u32.to_be_bytes());
    buffer.extend_from_slice(&0u32.to_be_bytes());
    buffer.push(0);
    buffer.extend_from_slice(&0u32.to_be_bytes());
    buffer.extend_from_slice(&1u32.to_be_bytes());
    buffer.push(0);
    let folder_list = definition.scope_json.to_string().into_bytes();
    buffer.extend_from_slice(&(folder_list.len().min(u32::MAX as usize) as u32).to_be_bytes());
    buffer.extend_from_slice(&folder_list);
    buffer.extend_from_slice(&0u32.to_be_bytes());
    buffer.extend_from_slice(&0u32.to_be_bytes());
    buffer
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
        PID_TAG_START_DATE | PID_TAG_MESSAGE_DELIVERY_TIME | PID_TAG_LAST_MODIFICATION_TIME => {
            Some(MapiValue::I64(event_start_filetime(event) as i64))
        }
        PID_TAG_END_DATE => Some(MapiValue::I64(event_end_filetime(event) as i64)),
        PID_TAG_LOCATION_W => Some(MapiValue::String(event.location.clone())),
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Appointment".to_string())),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I64(event_size(event))),
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
        PID_LID_REMINDER_TIME_TAG | PID_LID_REMINDER_SIGNAL_TIME_TAG => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_rfc3339_utc(&reminder.reminder_at),
        )),
        _ => None,
    }
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
        PID_LID_NOTE_HEIGHT_TAG => Some(MapiValue::I32(200)),
        PID_LID_NOTE_WIDTH_TAG => Some(MapiValue::I32(166)),
        PID_LID_NOTE_X_TAG | PID_LID_NOTE_Y_TAG => Some(MapiValue::I32(80)),
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

    pub(in crate::mapi) fn as_bool(&self) -> Option<bool> {
        match self {
            MapiValue::Bool(value) => Some(*value),
            MapiValue::I16(value) => Some(*value != 0),
            MapiValue::I32(value) => Some(*value != 0),
            MapiValue::I64(value) => Some(*value != 0),
            MapiValue::U32(value) => Some(*value != 0),
            MapiValue::U64(value) => Some(*value != 0),
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
            MapiValue::I64(_) | MapiValue::U64(_) => 8,
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
        PID_TAG_BODY_STRING8 | PID_TAG_BODY_W | PID_TAG_BODY_HTML_W | PID_TAG_HTML_BINARY => {
            message_body_stream_data(
                session,
                input_handle,
                property_tag,
                open_mode,
                mailboxes,
                emails,
            )
        }
        _ => None,
    }
}

pub(in crate::mapi) fn message_body_stream_data(
    session: &MapiSession,
    input_handle: u32,
    property_tag: u32,
    open_mode: u8,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Option<(Vec<u8>, Option<StreamWriteTarget>)> {
    let (body_text, body_html) = match session.handles.get(&input_handle)? {
        MapiObject::Message {
            folder_id,
            message_id,
        } if open_mode == 0 => {
            let email = message_for_id(*folder_id, *message_id, mailboxes, emails)?;
            (email.body_text.clone(), email.body_html_sanitized.clone())
        }
        MapiObject::PendingMessage { properties, .. } => match open_mode {
            0 | 1 => (
                pending_text_property(properties, &[PID_TAG_BODY_W]),
                optional_pending_text_property(properties, &[PID_TAG_BODY_HTML_W])
                    .or_else(|| pending_html_binary_property(properties)),
            ),
            2 => (String::new(), Some(String::new())),
            _ => return None,
        },
        _ => return None,
    };

    let stream = match (property_tag, open_mode) {
        (_, 2) => Vec::new(),
        (PID_TAG_BODY_STRING8, _) => string8z_bytes(&body_text),
        (PID_TAG_BODY_W, _) => utf16z_bytes(&body_text),
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
    let body = pending_text_property(properties, &[PID_TAG_BODY_W]);
    subject
        .len()
        .saturating_add(body.len())
        .min(i64::MAX as usize) as i64
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
        recurrence_rule: String::new(),
        title: String::new(),
        location: String::new(),
        attendees: String::new(),
        attendees_json: serialize_calendar_participants_metadata(
            &CalendarParticipantsMetadata::default(),
        ),
        notes: String::new(),
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
        recurrence_rule: String::new(),
        title: String::new(),
        location: String::new(),
        attendees: String::new(),
        attendees_json: serialize_calendar_participants_metadata(
            &CalendarParticipantsMetadata::default(),
        ),
        notes: String::new(),
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
        sort_order: existing.sort_order,
    }
}

pub(in crate::mapi) fn event_input_from_mapi(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: &AccessibleEvent,
    properties: &HashMap<u32, MapiValue>,
) -> Result<UpsertClientEventInput> {
    reject_unsupported_mapi_event_properties(properties)?;
    let start = properties
        .get(&PID_TAG_START_DATE)
        .and_then(MapiValue::as_i64)
        .and_then(filetime_to_date_time)
        .unwrap_or_else(|| (existing.date.clone(), existing.time.clone()));
    let end = properties
        .get(&PID_TAG_END_DATE)
        .and_then(MapiValue::as_i64)
        .and_then(filetime_to_date_time);
    let duration_minutes = match (
        properties
            .get(&PID_TAG_START_DATE)
            .and_then(MapiValue::as_i64),
        properties
            .get(&PID_TAG_END_DATE)
            .and_then(MapiValue::as_i64),
    ) {
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
        time_zone: existing.time_zone.clone(),
        duration_minutes: end
            .map(|_| duration_minutes)
            .unwrap_or(existing.duration_minutes),
        recurrence_rule: existing.recurrence_rule.clone(),
        title: optional_pending_text_property(
            properties,
            &[
                PID_TAG_SUBJECT_W,
                PID_TAG_NORMALIZED_SUBJECT_W,
                PID_TAG_DISPLAY_NAME_W,
            ],
        )
        .unwrap_or_else(|| existing.title.clone()),
        location: optional_pending_text_property(properties, &[PID_TAG_LOCATION_W])
            .unwrap_or_else(|| existing.location.clone()),
        attendees: existing.attendees.clone(),
        attendees_json: if existing.attendees_json.trim().is_empty() {
            serialize_calendar_participants_metadata(&CalendarParticipantsMetadata::default())
        } else {
            existing.attendees_json.clone()
        },
        notes: optional_pending_text_property(properties, &[PID_TAG_BODY_W])
            .unwrap_or_else(|| existing.notes.clone()),
    })
}

pub(in crate::mapi) fn reject_unsupported_mapi_event_properties(
    properties: &HashMap<u32, MapiValue>,
) -> Result<()> {
    for (tag, value) in properties {
        let supported = matches!(
            *tag,
            PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_DISPLAY_NAME_W
                | PID_TAG_BODY_W
                | PID_TAG_START_DATE
                | PID_TAG_END_DATE
                | PID_TAG_LOCATION_W
                | PID_TAG_MESSAGE_CLASS_W
        );
        if !supported {
            return Err(anyhow!(
                "MAPI calendar property {tag:#010X} is outside the canonical calendar subset"
            ));
        }
        if matches!(value, MapiValue::Binary(_)) {
            return Err(anyhow!(
                "MAPI binary calendar recurrence or meeting payloads are not supported"
            ));
        }
    }
    Ok(())
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
    let body_text = pending_text_property(properties, &[PID_TAG_BODY_W]);
    let from_address =
        optional_pending_text_property(properties, &[PID_TAG_SENDER_EMAIL_ADDRESS_W])
            .unwrap_or_else(|| principal.email.clone());
    let from_display = optional_pending_text_property(properties, &[PID_TAG_SENDER_NAME_W])
        .or_else(|| Some(principal.display_name.clone()));
    let internet_message_id =
        optional_pending_text_property(properties, &[PID_TAG_INTERNET_MESSAGE_ID_W]);
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
    let body_text = pending_text_property(properties, &[PID_TAG_BODY_W]);
    let from_address =
        optional_pending_text_property(properties, &[PID_TAG_SENDER_EMAIL_ADDRESS_W])
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
                update.swapped_todo_data = Some(bytes);
            }
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
        && update.swapped_todo_store_id.is_none()
        && update.swapped_todo_data.is_none()
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

fn filetime_to_rfc3339_utc(filetime: i64) -> Option<String> {
    filetime_to_date_time(filetime).map(|(date, time)| format!("{date}T{time}:00Z"))
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
    let event = snapshot
        .event_for_id(folder_id, event_id)
        .ok_or_else(|| anyhow!("canonical MAPI calendar event was not found"))?;
    let properties = values.into_iter().collect::<HashMap<_, _>>();
    let input = event_input_from_mapi(
        principal.account_id,
        Some(event.canonical_id),
        &event.event,
        &properties,
    )?;
    store
        .update_accessible_event(principal.account_id, event.canonical_id, input)
        .await?;
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
    let properties = values.into_iter().collect::<HashMap<_, _>>();
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
        Some(MapiObject::PendingContact { properties, .. })
        | Some(MapiObject::PendingEvent { properties, .. })
        | Some(MapiObject::PendingTask { properties, .. })
        | Some(MapiObject::PendingNote { properties, .. })
        | Some(MapiObject::PendingJournalEntry { properties, .. }) => {
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
                !(*folder_id == IPM_SUBTREE_FOLDER_ID && *tag == PID_TAG_OST_OSTID)
                    && !(*folder_id == ROOT_FOLDER_ID
                        && is_default_folder_identification_property_tag(*tag))
            }));
            Ok(())
        }
        Some(MapiObject::Logon) => Ok(()),
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
        Some(MapiObject::PendingContact { properties, .. })
        | Some(MapiObject::PendingEvent { properties, .. })
        | Some(MapiObject::PendingTask { properties, .. })
        | Some(MapiObject::PendingNote { properties, .. })
        | Some(MapiObject::PendingJournalEntry { properties, .. }) => {
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
        Some(MapiPropertyType::Error) => {
            write_u32(row, value.clone().into_u32().unwrap_or(0x8004_0102))
        }
        Some(MapiPropertyType::Boolean) => row.push(value.as_bool().unwrap_or_default() as u8),
        Some(MapiPropertyType::Integer64) | Some(MapiPropertyType::Time) => {
            write_u64(row, value.as_i64().unwrap_or_default().max(0) as u64)
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
        Some(MapiPropertyType::Binary) => match value {
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
        Some(MapiPropertyType::Binary) => {
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
            tracing::warn!(
                adapter = "mapi",
                enum_name = "MapiPropertyType",
                raw_value = tag.property_type_code(),
                property_id = tag.property_id(),
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
    fn special_folder_identification_properties_project_store_folder_ids() {
        assert_eq!(PID_TAG_VALID_FOLDER_MASK, 0x35DF_0003);
        assert_eq!(PID_TAG_IPM_APPOINTMENT_ENTRY_ID, 0x36D0_0102);
        assert_eq!(PID_TAG_IPM_CONTACT_ENTRY_ID, 0x36D1_0102);
        assert_eq!(PID_TAG_IPM_JOURNAL_ENTRY_ID, 0x36D2_0102);
        assert_eq!(PID_TAG_IPM_NOTE_ENTRY_ID, 0x36D3_0102);
        assert_eq!(PID_TAG_IPM_TASK_ENTRY_ID, 0x36D4_0102);
        assert_eq!(PID_TAG_REM_ONLINE_ENTRY_ID, 0x36D5_0102);

        assert_eq!(
            special_folder_identification_property_value(Uuid::nil(), PID_TAG_VALID_FOLDER_MASK),
            Some(MapiValue::U32(0x7F))
        );

        for (property_tag, folder_id) in [
            (PID_TAG_IPM_APPOINTMENT_ENTRY_ID, CALENDAR_FOLDER_ID),
            (PID_TAG_IPM_CONTACT_ENTRY_ID, CONTACTS_FOLDER_ID),
            (PID_TAG_IPM_JOURNAL_ENTRY_ID, JOURNAL_FOLDER_ID),
            (PID_TAG_IPM_NOTE_ENTRY_ID, NOTES_FOLDER_ID),
            (PID_TAG_IPM_TASK_ENTRY_ID, TASKS_FOLDER_ID),
        ] {
            let entry_id = crate::mapi::identity::long_term_id_from_object_id(folder_id)
                .unwrap()
                .to_vec();
            assert_eq!(
                special_folder_identification_property_value(Uuid::nil(), property_tag),
                Some(MapiValue::Binary(entry_id.clone()))
            );
            assert_eq!(entry_id.len(), 24);
            assert_eq!(
                crate::mapi::identity::object_id_from_long_term_id(&entry_id),
                Some(folder_id)
            );
        }
        assert_eq!(
            special_folder_identification_property_value(Uuid::nil(), PID_TAG_REM_ONLINE_ENTRY_ID),
            None
        );
        assert_eq!(
            special_folder_identification_property_value_with_reminders(
                Uuid::nil(),
                PID_TAG_REM_ONLINE_ENTRY_ID,
                false
            ),
            None
        );
        let reminder_entry_id =
            crate::mapi::identity::long_term_id_from_object_id(REMINDERS_FOLDER_ID)
                .unwrap()
                .to_vec();
        assert_eq!(
            special_folder_identification_property_value_with_reminders(
                Uuid::nil(),
                PID_TAG_REM_ONLINE_ENTRY_ID,
                true
            ),
            Some(MapiValue::Binary(reminder_entry_id))
        );
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
            bcc: Vec::new(),
            subject: "RSS item".to_string(),
            preview: "Preview".to_string(),
            body_text: "<item>RSS item</item>".to_string(),
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
            swapped_todo_data: Some(vec![1, 2, 3, 4]),
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
            Some(MapiValue::Binary(vec![1, 2, 3, 4]))
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
            recurrence_rule: String::new(),
            title: "Standup".to_string(),
            location: String::new(),
            attendees: String::new(),
            attendees_json: "[]".to_string(),
            notes: String::new(),
        };
        let reminder = lpe_storage::ClientReminder {
            source_type: "calendar".to_string(),
            source_id: event_id,
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
            sort_order: 0,
            updated_at: "2026-05-20T09:00:00Z".to_string(),
        };
        let task_reminder = lpe_storage::ClientReminder {
            source_type: "task".to_string(),
            source_id: task.id,
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
                PID_LID_REMINDER_TIME_TAG,
                Some(&task_reminder)
            ),
            Some(MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(
                "2026-05-21T11:45:00Z"
            )))
        );
    }

    #[test]
    fn unsupported_property_types_fail_explicitly() {
        let result = parse_mapi_property_value(&mut Cursor::new(&[]), 0x0037_000D);

        assert!(result.is_err());
    }

    #[test]
    fn logon_projects_server_icon_properties_as_ico_payloads() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@l-p-e.ch".to_string(),
            display_name: "test".to_string(),
        };

        for tag in [PID_TAG_SERVER_CONNECTED_ICON, PID_TAG_SERVER_ACCOUNT_ICON] {
            let Some(MapiValue::Binary(value)) = logon_property_value(&principal, tag) else {
                panic!("expected binary icon payload");
            };

            assert_eq!(&value[0..4], &[0, 0, 1, 0]);
            assert_eq!(u16::from_le_bytes(value[4..6].try_into().unwrap()), 1);
            assert_eq!(value[6], 16);
            assert_eq!(value[7], 16);
            assert_eq!(u16::from_le_bytes(value[12..14].try_into().unwrap()), 32);
            assert_eq!(
                value.len(),
                u32::from_le_bytes(value[14..18].try_into().unwrap()) as usize
                    + u32::from_le_bytes(value[18..22].try_into().unwrap()) as usize
            );
        }
    }
}
