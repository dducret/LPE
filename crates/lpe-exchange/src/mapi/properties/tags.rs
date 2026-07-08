use super::*;

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
        MapiPropertyType::from_code(self.property_type_code()).or_else(|| {
            let base_type = self.property_type_code() & !0x2000;
            match MapiPropertyType::from_code(base_type) {
                Some(
                    property_type @ (MapiPropertyType::MultipleInteger16
                    | MapiPropertyType::MultipleInteger32
                    | MapiPropertyType::MultipleInteger64
                    | MapiPropertyType::MultipleString8
                    | MapiPropertyType::MultipleString
                    | MapiPropertyType::MultipleTime
                    | MapiPropertyType::MultipleGuid
                    | MapiPropertyType::MultipleBinary),
                ) => Some(property_type),
                _ => None,
            }
        })
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
pub(in crate::mapi) const PID_TAG_SEARCH_FOLDER_TEMPLATE_ID: u32 = 0x6841_0003;
pub(in crate::mapi) const PID_TAG_SEARCH_FOLDER_ID: u32 = 0x6842_0102;
pub(in crate::mapi) const PID_TAG_SEARCH_FOLDER_LAST_USED: u32 = 0x6834_0003;
pub(in crate::mapi) const PID_TAG_SEARCH_FOLDER_EXPIRATION: u32 = 0x683A_0003;
pub(in crate::mapi) const PID_TAG_SEARCH_FOLDER_TAG: u32 = 0x6847_0003;
pub(in crate::mapi) const PID_TAG_SEARCH_FOLDER_EFP_FLAGS: u32 = 0x6848_0003;
pub(in crate::mapi) const PID_TAG_SEARCH_FOLDER_STORAGE_TYPE: u32 = 0x6846_0003;
pub(in crate::mapi) const PID_TAG_SEARCH_FOLDER_DEFINITION: u32 = 0x6845_0102;
pub(in crate::mapi) const PID_TAG_FOLDER_FORM_FLAGS: u32 = 0x36DE_0003;
pub(in crate::mapi) const PID_TAG_FOLDER_WEBVIEWINFO: u32 = 0x36DF_0102;
pub(in crate::mapi) const PID_TAG_FOLDER_XVIEWINFO_E: u32 = 0x36E0_0102;
pub(in crate::mapi) const PID_TAG_FOLDER_VIEWS_ONLY: u32 = 0x36E1_0003;
pub(in crate::mapi) const PID_TAG_FOLDER_FORM_STORAGE: u32 = 0x36EB_0102;
pub(in crate::mapi) const OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C: u32 = 0x120C_0102;
pub(in crate::mapi) const PID_TAG_ARCHIVE_TAG: u32 = 0x3018_0102;
pub(in crate::mapi) const PID_TAG_POLICY_TAG: u32 = 0x3019_0102;
pub(in crate::mapi) const PID_TAG_RETENTION_PERIOD: u32 = 0x301A_0003;
pub(in crate::mapi) const PID_TAG_RETENTION_DATE: u32 = 0x301C_0040;
pub(in crate::mapi) const PID_TAG_RETENTION_FLAGS: u32 = 0x301D_0003;
pub(in crate::mapi) const PID_TAG_ARCHIVE_PERIOD: u32 = 0x301E_0003;
pub(in crate::mapi) const PID_TAG_ARCHIVE_DATE: u32 = 0x301F_0040;
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
pub(in crate::mapi) const PID_TAG_SENT_REPRESENTING_NAME_W: u32 = 0x0042_001F;
pub(in crate::mapi) const PID_TAG_SENT_REPRESENTING_ENTRY_ID: u32 = 0x0041_0102;
pub(in crate::mapi) const PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W: u32 = 0x0064_001F;
pub(in crate::mapi) const PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W: u32 = 0x0065_001F;
pub(in crate::mapi) const PID_TAG_SENT_REPRESENTING_SEARCH_KEY: u32 = 0x003B_0102;
pub(in crate::mapi) const PID_TAG_ALTERNATE_RECIPIENT_ALLOWED: u32 = 0x0002_000B;
pub(in crate::mapi) const PID_TAG_AUTO_FORWARDED: u32 = 0x0005_000B;
pub(in crate::mapi) const PID_TAG_DEFERRED_DELIVERY_TIME: u32 = 0x000F_0040;
pub(in crate::mapi) const PID_TAG_EXPIRY_TIME: u32 = 0x0015_0040;
pub(in crate::mapi) const PID_TAG_ORIGINATOR_DELIVERY_REPORT_REQUESTED: u32 = 0x0023_000B;
pub(in crate::mapi) const PID_TAG_PARENT_KEY: u32 = 0x0025_0102;
pub(in crate::mapi) const PID_TAG_READ_RECEIPT_REQUESTED: u32 = 0x0029_000B;
pub(in crate::mapi) const PID_TAG_RECIPIENT_REASSIGNMENT_PROHIBITED: u32 = 0x002B_000B;
pub(in crate::mapi) const PID_TAG_REPLY_TIME: u32 = 0x0030_0040;
pub(in crate::mapi) const PID_TAG_REPORT_TAG: u32 = 0x0031_0102;
pub(in crate::mapi) const PID_TAG_REPORT_TIME: u32 = 0x0032_0040;
pub(in crate::mapi) const PID_TAG_ORIGINAL_AUTHOR_ENTRY_ID: u32 = 0x004C_0102;
pub(in crate::mapi) const PID_TAG_ORIGINAL_AUTHOR_NAME_W: u32 = 0x004D_001F;
pub(in crate::mapi) const PID_TAG_ORIGINAL_SUBMIT_TIME: u32 = 0x004E_0040;
pub(in crate::mapi) const PID_TAG_REPLY_RECIPIENT_ENTRIES: u32 = 0x004F_0102;
pub(in crate::mapi) const PID_TAG_REPLY_RECIPIENT_NAMES_W: u32 = 0x0050_001F;
pub(in crate::mapi) const PID_TAG_RESPONSE_REQUESTED: u32 = 0x0063_000B;
pub(in crate::mapi) const PID_TAG_ORIGINAL_DISPLAY_BCC_W: u32 = 0x0072_001F;
pub(in crate::mapi) const PID_TAG_ORIGINAL_DISPLAY_CC_W: u32 = 0x0073_001F;
pub(in crate::mapi) const PID_TAG_ORIGINAL_DISPLAY_TO_W: u32 = 0x0074_001F;
pub(in crate::mapi) const PID_TAG_ORIGINAL_SUBJECT_W: u32 = 0x0049_001F;
pub(in crate::mapi) const PID_TAG_ORIGINAL_SENDER_NAME_W: u32 = 0x005A_001F;
pub(in crate::mapi) const PID_TAG_REPORT_DISPOSITION_W: u32 = 0x0080_001F;
pub(in crate::mapi) const PID_TAG_OWNER_APPOINTMENT_ID: u32 = 0x0062_0003;
pub(in crate::mapi) const PID_TAG_REPLY_REQUESTED: u32 = 0x0C17_000B;
pub(in crate::mapi) const PID_TAG_ORIGINAL_SENSITIVITY: u32 = 0x002E_0003;
pub(in crate::mapi) const PID_TAG_SENDER_ENTRY_ID: u32 = 0x0C19_0102;
pub(in crate::mapi) const PID_TAG_SENDER_SEARCH_KEY: u32 = 0x0C1D_0102;
pub(in crate::mapi) const PID_TAG_RECEIVED_BY_ADDRESS_TYPE_W: u32 = 0x0075_001F;
pub(in crate::mapi) const PID_TAG_RECEIVED_BY_EMAIL_ADDRESS_W: u32 = 0x0076_001F;
pub(in crate::mapi) const PID_TAG_RECEIVED_BY_ENTRY_ID_ALT: u32 = 0x003F_0102;
pub(in crate::mapi) const PID_TAG_RECEIVED_BY_NAME_W: u32 = 0x0040_001F;
pub(in crate::mapi) const PID_TAG_RECEIVED_BY_SEARCH_KEY: u32 = 0x0051_0102;
pub(in crate::mapi) const PID_TAG_RECEIVED_REPRESENTING_ADDRESS_TYPE_W: u32 = 0x0077_001F;
pub(in crate::mapi) const PID_TAG_RECEIVED_REPRESENTING_EMAIL_ADDRESS_W: u32 = 0x0078_001F;
pub(in crate::mapi) const PID_TAG_RECEIVED_REPRESENTING_ENTRY_ID: u32 = 0x0043_0102;
pub(in crate::mapi) const PID_TAG_RECEIVED_REPRESENTING_NAME_W: u32 = 0x0044_001F;
pub(in crate::mapi) const PID_TAG_RECEIVED_REPRESENTING_SEARCH_KEY: u32 = 0x0052_0102;
pub(in crate::mapi) const PID_TAG_RECIPIENT_TYPE: u32 = 0x0C15_0003;
pub(in crate::mapi) const PID_TAG_MESSAGE_RECIPIENTS: u32 = 0x0E12_000D;
pub(in crate::mapi) const PID_TAG_MESSAGE_ATTACHMENTS: u32 = 0x0E13_000D;
pub(in crate::mapi) const PID_TAG_CLIENT_SUBMIT_TIME: u32 = 0x0039_0040;
pub(in crate::mapi) const PID_TAG_IMPORTANCE: u32 = 0x0017_0003;
pub(in crate::mapi) const PID_TAG_PRIORITY: u32 = 0x0026_0003;
pub(in crate::mapi) const PID_TAG_SENSITIVITY: u32 = 0x0036_0003;
pub(in crate::mapi) const PID_TAG_ORIGINAL_MESSAGE_CLASS_W: u32 = 0x004B_001F;
pub(in crate::mapi) const PID_TAG_SUBJECT_PREFIX_W: u32 = 0x003D_001F;
pub(in crate::mapi) const PID_TAG_DISPLAY_BCC_W: u32 = 0x0E02_001F;
pub(in crate::mapi) const PID_TAG_DISPLAY_CC_W: u32 = 0x0E03_001F;
pub(in crate::mapi) const PID_TAG_DISPLAY_TO_W: u32 = 0x0E04_001F;
pub(in crate::mapi) const PID_TAG_MESSAGE_DELIVERY_TIME: u32 = 0x0E06_0040;
pub(in crate::mapi) const PID_TAG_MESSAGE_FLAGS: u32 = 0x0E07_0003;
pub(in crate::mapi) const PID_TAG_MESSAGE_SIZE: u32 = 0x0E08_0003;
pub(in crate::mapi) const PID_TAG_MESSAGE_SIZE_EXTENDED: u32 = 0x0E08_0014;
pub(in crate::mapi) const PID_TAG_DELETE_AFTER_SUBMIT: u32 = 0x0E01_000B;
pub(in crate::mapi) const PID_TAG_PARENT_ENTRY_ID: u32 = 0x0E09_0102;
pub(in crate::mapi) const OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B: u32 = 0x0E0B_0102;
pub(in crate::mapi) const PID_TAG_EXTENDED_RULE_MESSAGE_ACTIONS: u32 = 0x0E99_0102;
pub(in crate::mapi) const PID_TAG_EXTENDED_RULE_MESSAGE_CONDITION: u32 = 0x0E9A_0102;
pub(in crate::mapi) const PID_TAG_MESSAGE_STATUS: u32 = 0x0E17_0003;
pub(in crate::mapi) const PID_TAG_HAS_ATTACHMENTS: u32 = 0x0E1B_000B;
pub(in crate::mapi) const PID_TAG_NORMALIZED_SUBJECT_W: u32 = 0x0E1D_001F;
pub(in crate::mapi) const PID_TAG_RTF_IN_SYNC: u32 = 0x0E1F_000B;
pub(in crate::mapi) const PID_TAG_TRUST_SENDER: u32 = 0x0E79_0003;
pub(in crate::mapi) const PID_TAG_ASSOCIATED_SHARING_PROVIDER: u32 = 0x0EA0_0048;
pub(in crate::mapi) const PID_TAG_READ: u32 = 0x0E69_000B;
pub(in crate::mapi) const PID_TAG_CONVERSATION_TOPIC_W: u32 = 0x0070_001F;
pub(in crate::mapi) const PID_TAG_CONVERSATION_INDEX: u32 = 0x0071_0102;
pub(in crate::mapi) const PID_TAG_CONVERSATION_ID: u32 = 0x3013_0102;
pub(in crate::mapi) const PID_TAG_CONVERSATION_INDEX_TRACKING: u32 = 0x3016_000B;
pub(in crate::mapi) const PID_TAG_TRANSPORT_MESSAGE_HEADERS_W: u32 = 0x007D_001F;
pub(in crate::mapi) const PID_TAG_ACCESS: u32 = 0x0FF4_0003;
pub(in crate::mapi) const PID_TAG_ACCESS_LEVEL: u32 = 0x0FF7_0003;
pub(in crate::mapi) const PID_TAG_ROW_TYPE: u32 = 0x0FF5_0003;
pub(in crate::mapi) const PID_TAG_INSTANCE_KEY: u32 = 0x0FF6_0102;
pub(in crate::mapi) const PID_TAG_RECORD_KEY: u32 = 0x0FF9_0102;
pub(in crate::mapi) const PID_TAG_ENTRY_ID: u32 = 0x0FFF_0102;
pub(in crate::mapi) const PID_TAG_DEPTH: u32 = 0x3005_0003;
pub(in crate::mapi) const PID_TAG_SEARCH_KEY: u32 = 0x300B_0102;
pub(in crate::mapi) const PID_TAG_START_DATE_ETC: u32 = 0x301B_0102;
pub(in crate::mapi) const PID_TAG_CREATOR_NAME_W: u32 = 0x3FF8_001F;
pub(in crate::mapi) const PID_TAG_CREATOR_ENTRY_ID: u32 = 0x3FF9_0102;
pub(in crate::mapi) const PID_TAG_LAST_MODIFIER_NAME_W: u32 = 0x3FFA_001F;
pub(in crate::mapi) const PID_TAG_LAST_MODIFIER_ENTRY_ID: u32 = 0x3FFB_0102;
pub(in crate::mapi) const PID_TAG_OBJECT_TYPE: u32 = 0x0FFE_0003;
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
pub(in crate::mapi) const MSGFLAG_FAI: u32 = 0x0000_0040;
pub(in crate::mapi) const FOLLOWUP_COMPLETE: u32 = 0x0000_0001;
pub(in crate::mapi) const FOLLOWUP_FLAGGED: u32 = 0x0000_0002;
pub(in crate::mapi) const PID_TAG_HTML_BINARY: u32 = 0x1013_0102;
pub(in crate::mapi) const PID_TAG_INTERNET_MESSAGE_ID_W: u32 = 0x1035_001F;
pub(in crate::mapi) const PID_TAG_INTERNET_REFERENCES_W: u32 = 0x1039_001F;
pub(in crate::mapi) const PID_TAG_IN_REPLY_TO_ID_W: u32 = 0x1042_001F;
pub(in crate::mapi) const PID_TAG_FLAG_STATUS: u32 = 0x1090_0003;
pub(in crate::mapi) const PID_TAG_FLAG_COMPLETE_TIME: u32 = 0x1091_0040;
pub(in crate::mapi) const PID_TAG_ICON_INDEX: u32 = 0x1080_0003;
pub(in crate::mapi) const PID_TAG_LAST_VERB_EXECUTED: u32 = 0x1081_0003;
pub(in crate::mapi) const PID_TAG_LAST_VERB_EXECUTION_TIME: u32 = 0x1082_0040;
pub(in crate::mapi) const PID_TAG_FOLLOWUP_ICON: u32 = 0x1095_0003;
pub(in crate::mapi) const PID_TAG_TODO_ITEM_FLAGS: u32 = 0x0E2B_0003;
pub(in crate::mapi) const PID_TAG_SWAPPED_TODO_STORE: u32 = 0x0E2C_0102;
pub(in crate::mapi) const PID_TAG_SWAPPED_TODO_DATA: u32 = 0x0E2D_0102;
pub(in crate::mapi) const PID_TAG_SENDER_SMTP_ADDRESS_W: u32 = 0x5D01_001F;
pub(in crate::mapi) const PID_TAG_INTERNET_CODEPAGE: u32 = 0x3FDE_0003;
pub(in crate::mapi) const PID_TAG_PRIMARY_SEND_ACCOUNT_W: u32 = 0x0E28_001F;
pub(in crate::mapi) const PID_TAG_NEXT_SEND_ACCOUNT_W: u32 = 0x0E29_001F;
pub(in crate::mapi) const PID_TAG_INTERNET_MAIL_OVERRIDE_FORMAT: u32 = 0x5902_0003;
pub(in crate::mapi) const PID_TAG_MESSAGE_EDITOR_FORMAT: u32 = 0x5909_0003;
pub(in crate::mapi) const PID_TAG_PROCESSED: u32 = 0x7D01_000B;
pub(in crate::mapi) const PID_TAG_BLOCK_STATUS: u32 = 0x1096_0003;
pub(in crate::mapi) const PID_TAG_MESSAGE_LOCALE_ID: u32 = 0x3FF1_0003;
pub(in crate::mapi) const PID_TAG_CREATION_TIME: u32 = 0x3007_0040;
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
pub(in crate::mapi) const PID_TAG_HAS_NAMED_PROPERTIES: u32 = 0x664A_000B;
pub(in crate::mapi) const PID_TAG_LOCALE_ID: u32 = 0x66A1_0003;
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
pub(in crate::mapi) const PID_TAG_DEFERRED_SEND_TIME: u32 = 0x3FEF_0040;
pub(in crate::mapi) const PID_TAG_EXTENDED_RULE_SIZE_LIMIT: u32 = 0x0E9B_0003;
pub(in crate::mapi) const PID_TAG_PST_PATH_W: u32 = 0x6700_001F;
pub(in crate::mapi) const PID_TAG_OST_OSTID: u32 = 0x7C04_0102;
pub(in crate::mapi) const PID_TAG_SENT_MAIL_SVR_EID: u32 = 0x6740_00FB;
pub(in crate::mapi) const PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W: u32 = 0x5D02_001F;
pub(in crate::mapi) const PID_TAG_MID: u32 = 0x674A_0014;

pub(super) const OUTLOOK_STORE_ICON_ICO: &[u8] = &[
    0x00, 0x00, 0x01, 0x00, 0x01, 0x00, // ICO header: one icon image.
    0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x20, 0x00, 0x30, 0x00, 0x00, 0x00, 0x16, 0x00, 0x00,
    0x00, // Directory entry: 1x1, 32-bit, 48-byte DIB at offset 22.
    0x28, 0x00, 0x00, 0x00, // BITMAPINFOHEADER.
    0x01, 0x00, 0x00, 0x00, // Width.
    0x02, 0x00, 0x00, 0x00, // Height includes XOR and AND masks for ICO DIBs.
    0x01, 0x00, 0x20, 0x00, // Planes and 32-bit color depth.
    0x00, 0x00, 0x00, 0x00, // BI_RGB.
    0x04, 0x00, 0x00, 0x00, // One BGRA pixel.
    0x00, 0x00, 0x00, 0x00, // Horizontal resolution.
    0x00, 0x00, 0x00, 0x00, // Vertical resolution.
    0x00, 0x00, 0x00, 0x00, // Palette colors.
    0x00, 0x00, 0x00, 0x00, // Important colors.
    0x2A, 0x8C, 0xD6, 0xFF, // Opaque pixel.
    0x00, 0x00, 0x00, 0x00, // Empty AND mask.
];
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
pub(in crate::mapi) const PID_TAG_WLINK_CALENDAR_COLOR: u32 = 0x6853_0003;
pub(in crate::mapi) const PID_TAG_WLINK_ADDRESS_BOOK_EID: u32 = 0x6854_0102;
pub(in crate::mapi) const PID_TAG_WLINK_CLIENT_ID: u32 = 0x6890_0102;
pub(in crate::mapi) const PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID: u32 = 0x6891_0102;
pub(in crate::mapi) const PID_TAG_WLINK_RO_GROUP_TYPE: u32 = 0x6892_0003;
pub(in crate::mapi) const PID_TAG_ATTACH_DATA_BINARY: u32 = 0x3701_0102;
pub(in crate::mapi) const PID_TAG_VIEW_DESCRIPTOR_CLSID: u32 = 0x6833_0048;
pub(in crate::mapi) const PID_TAG_VIEW_DESCRIPTOR_FLAGS: u32 = 0x6834_0003;
pub(in crate::mapi) const OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835: u32 = 0x6835_0102;
pub(in crate::mapi) const OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C: u32 = 0x683C_0102;
pub(in crate::mapi) const OUTLOOK_RULE_ORGANIZER_BINARY_6802: u32 = 0x6802_0102;
pub(in crate::mapi) const PID_TAG_VIEW_DESCRIPTOR_VERSION: u32 = 0x683A_0003;
pub(in crate::mapi) const PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE: u32 = 0x683E_0102;
pub(in crate::mapi) const PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE: u32 = 0x6841_0003;
pub(in crate::mapi) const PID_TAG_VIEW_DESCRIPTOR_BINARY: u32 = 0x7001_0102;
pub(in crate::mapi) const PID_TAG_VIEW_DESCRIPTOR_STRINGS_W: u32 = 0x7002_001F;
pub(in crate::mapi) const PID_TAG_VIEW_DESCRIPTOR_NAME_W: u32 = 0x7006_001F;
pub(in crate::mapi) const PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL: u32 = 0x7007_0003;
pub(in crate::mapi) const PID_NAME_CONTENT_CLASS_W_TAG: u32 = 0x801F_001F;
pub(in crate::mapi) const PID_NAME_CONTENT_TYPE_W_TAG: u32 = 0x836B_001F;
pub(in crate::mapi) const PID_TAG_ATTACH_SIZE: u32 = 0x0E20_0003;
pub(in crate::mapi) const PID_TAG_ATTACH_NUM: u32 = 0x0E21_0003;
pub(in crate::mapi) const PID_TAG_ATTACH_EXTENSION_W: u32 = 0x3703_001F;
pub(in crate::mapi) const PID_TAG_ATTACH_FILENAME_W: u32 = 0x3704_001F;
pub(in crate::mapi) const PID_TAG_ATTACH_METHOD: u32 = 0x3705_0003;
pub(in crate::mapi) const ATTACH_BY_VALUE: u32 = 1;
pub(in crate::mapi) const ATTACH_EMBEDDED_MESSAGE: u32 = 5;
pub(in crate::mapi) const PID_TAG_ATTACH_LONG_FILENAME_W: u32 = 0x3707_001F;
pub(in crate::mapi) const PID_TAG_ATTACH_RENDERING: u32 = 0x3709_0102;
pub(in crate::mapi) const PID_TAG_RENDERING_POSITION: u32 = 0x370B_0003;
pub(in crate::mapi) const PID_TAG_ATTACH_MIME_TAG_W: u32 = 0x370E_001F;
pub(in crate::mapi) const PID_TAG_ATTACH_CONTENT_BASE_W: u32 = 0x3711_001F;
pub(in crate::mapi) const PID_TAG_ATTACH_CONTENT_ID_W: u32 = 0x3712_001F;
pub(in crate::mapi) const PID_TAG_ATTACH_FLAGS: u32 = 0x3714_0003;
pub(in crate::mapi) const PID_TAG_ATTACHMENT_LINK_ID: u32 = 0x7FFA_0003;
pub(in crate::mapi) const PID_TAG_ATTACHMENT_FLAGS: u32 = 0x7FFD_0003;
pub(in crate::mapi) const PID_TAG_ATTACHMENT_HIDDEN: u32 = 0x7FFE_000B;
pub(in crate::mapi) const PID_TAG_EMAIL_ADDRESS_W: u32 = 0x3003_001F;
pub(in crate::mapi) const PID_TAG_DISPLAY_TYPE: u32 = 0x3900_0003;
pub(in crate::mapi) const PID_TAG_DISPLAY_TYPE_EX: u32 = 0x3905_0003;
pub(in crate::mapi) const PID_TAG_ADDRESS_BOOK_DISPLAY_NAME_PRINTABLE_W: u32 = 0x39FF_001F;
pub(in crate::mapi) const PID_TAG_SMTP_ADDRESS_W: u32 = 0x39FE_001F;
pub(in crate::mapi) const PID_TAG_SEND_INTERNET_ENCODING: u32 = 0x3A71_0003;
pub(in crate::mapi) const PID_TAG_RECIPIENT_DISPLAY_NAME_W: u32 = 0x5FF6_001F;
pub(in crate::mapi) const PID_TAG_RECIPIENT_ENTRY_ID: u32 = 0x5FF7_0102;
pub(in crate::mapi) const OUTLOOK_RECIPIENT_5FDE: u32 = 0x5FDE_0003;
pub(in crate::mapi) const PID_TAG_RECIPIENT_ORDER: u32 = 0x5FDF_0003;
pub(in crate::mapi) const PID_TAG_RECIPIENT_FLAGS: u32 = 0x5FFD_0003;
pub(in crate::mapi) const PID_TAG_RECIPIENT_TRACK_STATUS: u32 = 0x5FFF_0003;
pub(in crate::mapi) const PID_TAG_GENERATION_W: u32 = 0x3A05_001F;
pub(in crate::mapi) const PID_TAG_GIVEN_NAME_W: u32 = 0x3A06_001F;
pub(in crate::mapi) const PID_TAG_BUSINESS_TELEPHONE_NUMBER_W: u32 = 0x3A08_001F;
pub(in crate::mapi) const PID_TAG_HOME_TELEPHONE_NUMBER_W: u32 = 0x3A09_001F;
pub(in crate::mapi) const PID_TAG_SURNAME_W: u32 = 0x3A11_001F;
pub(in crate::mapi) const PID_TAG_COMPANY_NAME_W: u32 = 0x3A16_001F;
pub(in crate::mapi) const PID_TAG_TITLE_W: u32 = 0x3A17_001F;
pub(in crate::mapi) const PID_TAG_DEPARTMENT_NAME_W: u32 = 0x3A18_001F;
pub(in crate::mapi) const PID_TAG_PRIMARY_TELEPHONE_NUMBER_W: u32 = 0x3A1A_001F;
pub(in crate::mapi) const PID_TAG_BUSINESS2_TELEPHONE_NUMBERS_W: u32 = 0x3A1B_101F;
pub(in crate::mapi) const PID_TAG_MOBILE_TELEPHONE_NUMBER_W: u32 = 0x3A1C_001F;
pub(in crate::mapi) const PID_TAG_MIDDLE_NAME_W: u32 = 0x3A44_001F;
pub(in crate::mapi) const PID_TAG_DISPLAY_NAME_PREFIX_W: u32 = 0x3A45_001F;
pub(in crate::mapi) const PID_TAG_NICKNAME_W: u32 = 0x3A4F_001F;
pub(in crate::mapi) const PID_TAG_PERSONAL_HOME_PAGE_W: u32 = 0x3A50_001F;
pub(in crate::mapi) const PID_TAG_BUSINESS_HOME_PAGE_W: u32 = 0x3A51_001F;
pub(in crate::mapi) const PID_TAG_START_DATE: u32 = 0x0060_0040;
pub(in crate::mapi) const PID_TAG_END_DATE: u32 = 0x0061_0040;
pub(in crate::mapi) const PID_TAG_LOCATION_W: u32 = 0x3FFB_001F;
pub(crate) const FIRST_NAMED_PROPERTY_ID: u16 = 0x8001;
pub(crate) const DYNAMIC_NAMED_PROPERTY_ID_START: u16 = 0x9000;
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
pub(in crate::mapi) const OUTLOOK_VIEW_8F07_GUID: [u8; 16] = [
    0x14, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];
pub(in crate::mapi) const PSETID_ADDRESS_GUID: [u8; 16] = [
    0x04, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
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
pub(in crate::mapi) const OUTLOOK_STALE_SHARING_LOCAL_FOLDER_ID_TAG: u32 = 0x8FFF_0102;
pub(in crate::mapi) const PID_LID_COMMON_START: u32 = 0x0000_8516;
pub(in crate::mapi) const PID_LID_COMMON_END: u32 = 0x0000_8517;
// PidLidSideEffects: [MS-OXPROPS] 2.299, behavior flags in [MS-OXCMSG] 2.2.1.16.
pub(in crate::mapi) const PID_LID_SIDE_EFFECTS: u32 = 0x0000_8510;
// PidLidSideEffects: MS-OXCMSG 2.2.1.16 open-on-delete/copy/move/context-menu bits.
pub(in crate::mapi) const CALENDAR_EVENT_SIDE_EFFECTS: i32 = 0x0000_0161;
pub(in crate::mapi) const PID_LID_OUTLOOK_COMMON_8514: u32 = 0x0000_8514;
pub(in crate::mapi) const PID_LID_OUTLOOK_COMMON_8578: u32 = 0x0000_8578;
pub(in crate::mapi) const PID_LID_OUTLOOK_COMMON_85B1: u32 = 0x0000_85B1;
pub(in crate::mapi) const PID_LID_OUTLOOK_COMMON_85EF: u32 = 0x0000_85EF;
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
pub(in crate::mapi) const PID_LID_APPOINTMENT_COLOR: u32 = 0x0000_8214;
pub(in crate::mapi) const PID_LID_APPOINTMENT_SUB_TYPE: u32 = 0x0000_8215;
pub(in crate::mapi) const PID_LID_APPOINTMENT_RECUR: u32 = 0x0000_8216;
pub(in crate::mapi) const PID_LID_APPOINTMENT_STATE_FLAGS: u32 = 0x0000_8217;
pub(in crate::mapi) const PID_LID_RECURRING: u32 = 0x0000_8223;
pub(in crate::mapi) const PID_LID_ALL_ATTENDEES_STRING: u32 = 0x0000_8238;
pub(in crate::mapi) const PID_LID_TO_ATTENDEES_STRING: u32 = 0x0000_823B;
pub(in crate::mapi) const PID_LID_CC_ATTENDEES_STRING: u32 = 0x0000_823C;
pub(in crate::mapi) const PID_LID_TIME_ZONE_STRUCT: u32 = 0x0000_8233;
pub(in crate::mapi) const PID_LID_TIME_ZONE_DESCRIPTION: u32 = 0x0000_8234;
pub(in crate::mapi) const PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY: u32 = 0x0000_825E;
pub(in crate::mapi) const PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY: u32 = 0x0000_825F;
pub(in crate::mapi) const PID_LID_OUTLOOK_APPOINTMENT_8F07: u32 = 0x0000_8F07;
pub(in crate::mapi) const PID_LID_COMPANIES: u32 = 0x0000_8539;
pub(in crate::mapi) const PID_LID_CONTACTS: u32 = 0x0000_853A;
pub(in crate::mapi) const PID_LID_CONTACT_LINK_SEARCH_KEY: u32 = 0x0000_8584;
pub(in crate::mapi) const PID_LID_CONTACT_LINK_ENTRY: u32 = 0x0000_8585;
pub(in crate::mapi) const PID_LID_CONTACT_LINK_NAME: u32 = 0x0000_8586;
pub(in crate::mapi) const PID_LID_EMAIL1_ADDRESS_TYPE: u32 = 0x0000_8082;
pub(in crate::mapi) const PID_LID_EMAIL1_DISPLAY_NAME: u32 = 0x0000_8080;
pub(in crate::mapi) const PID_LID_EMAIL1_EMAIL_ADDRESS: u32 = 0x0000_8083;
pub(in crate::mapi) const PID_LID_EMAIL1_ORIGINAL_DISPLAY_NAME: u32 = 0x0000_8084;
pub(in crate::mapi) const PID_LID_EMAIL1_ORIGINAL_ENTRY_ID: u32 = 0x0000_8085;
pub(in crate::mapi) const PID_LID_EMAIL2_ADDRESS_TYPE: u32 = 0x0000_8092;
pub(in crate::mapi) const PID_LID_EMAIL2_DISPLAY_NAME: u32 = 0x0000_8090;
pub(in crate::mapi) const PID_LID_EMAIL2_EMAIL_ADDRESS: u32 = 0x0000_8093;
pub(in crate::mapi) const PID_LID_EMAIL2_ORIGINAL_DISPLAY_NAME: u32 = 0x0000_8094;
pub(in crate::mapi) const PID_LID_EMAIL2_ORIGINAL_ENTRY_ID: u32 = 0x0000_8095;
pub(in crate::mapi) const PID_LID_EMAIL3_ADDRESS_TYPE: u32 = 0x0000_80A2;
pub(in crate::mapi) const PID_LID_EMAIL3_DISPLAY_NAME: u32 = 0x0000_80A0;
pub(in crate::mapi) const PID_LID_EMAIL3_EMAIL_ADDRESS: u32 = 0x0000_80A3;
pub(in crate::mapi) const PID_LID_EMAIL3_ORIGINAL_DISPLAY_NAME: u32 = 0x0000_80A4;
pub(in crate::mapi) const PID_LID_EMAIL3_ORIGINAL_ENTRY_ID: u32 = 0x0000_80A5;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_SOURCE_80E0: u32 = 0x0000_80E0;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_SOURCE_80E2: u32 = 0x0000_80E2;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_SOURCE_80E3: u32 = 0x0000_80E3;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_SOURCE_80E5: u32 = 0x0000_80E5;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_SOURCE_80E6: u32 = 0x0000_80E6;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_SOURCE_80E8: u32 = 0x0000_80E8;
pub(in crate::mapi) const PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80E1: u32 = 0x0000_80E1;
pub(in crate::mapi) const PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EA: u32 = 0x0000_80EA;
pub(in crate::mapi) const PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EC: u32 = 0x0000_80EC;
pub(in crate::mapi) const PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80ED: u32 = 0x0000_80ED;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_ADDRESS_TYPE: u32 = 0x0000_80B5;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_DISPLAY_NAME: u32 = 0x0000_80B6;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_EMAIL_ADDRESS: u32 = 0x0000_80B7;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_ADDRESS_TYPE: u32 = 0x0000_80D5;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_DISPLAY_NAME: u32 = 0x0000_80D6;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_EMAIL_ADDRESS: u32 = 0x0000_80D7;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS3_ADDRESS_TYPE: u32 = 0x0000_805F;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS3_EMAIL_ADDRESS: u32 = 0x0000_8060;
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
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_PROVIDER_GUID: u32 = 0x0000_8A01;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_REMOTE_NAME: u32 = 0x0000_8A07;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_REMOTE_UID: u32 = 0x0000_8A08;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_LOCAL_TYPE: u32 = 0x0000_8A1C;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_CAPABILITIES: u32 = 0x0000_8A67;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8AA6: u32 = 0x0000_8AA6;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8A70: u32 = 0x0000_8A70;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8A71: u32 = 0x0000_8A71;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8A72: u32 = 0x0000_8A72;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8A73: u32 = 0x0000_8A73;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8A74: u32 = 0x0000_8A74;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8A75: u32 = 0x0000_8A75;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8A76: u32 = 0x0000_8A76;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8A77: u32 = 0x0000_8A77;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8A78: u32 = 0x0000_8A78;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8A7E: u32 = 0x0000_8A7E;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8A80: u32 = 0x0000_8A80;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8A88: u32 = 0x0000_8A88;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8A8B: u32 = 0x0000_8A8B;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8A8D: u32 = 0x0000_8A8D;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8A8E: u32 = 0x0000_8A8E;
pub(in crate::mapi) const PID_NAME_SHARING_SEND_AS_STATE_TAG: u32 = 0x81ED_0003;

pub(in crate::mapi) const PID_LID_COMMON_START_TAG: u32 = 0x8516_0040;
pub(in crate::mapi) const PID_LID_COMMON_END_TAG: u32 = 0x8517_0040;
pub(in crate::mapi) const PID_LID_GLOBAL_OBJECT_ID_TAG: u32 = 0x8001_0102;
pub(in crate::mapi) const PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG: u32 = 0x8002_0102;
pub(in crate::mapi) const PID_LID_APPOINTMENT_START_WHOLE_TAG: u32 = 0x820D_0040;
pub(in crate::mapi) const PID_LID_APPOINTMENT_END_WHOLE_TAG: u32 = 0x820E_0040;
pub(in crate::mapi) const PID_LID_BUSY_STATUS_TAG: u32 = 0x8205_0003;
pub(in crate::mapi) const PID_LID_LOCATION_W_TAG: u32 = 0x8208_001F;
pub(in crate::mapi) const PID_LID_APPOINTMENT_DURATION_TAG: u32 = 0x8213_0003;
pub(in crate::mapi) const PID_LID_APPOINTMENT_COLOR_TAG: u32 = 0x8214_0003;
pub(in crate::mapi) const PID_LID_APPOINTMENT_SUB_TYPE_TAG: u32 = 0x8215_000B;
pub(in crate::mapi) const PID_LID_APPOINTMENT_RECUR_TAG: u32 = 0x8216_0102;
pub(in crate::mapi) const PID_LID_APPOINTMENT_STATE_FLAGS_TAG: u32 = 0x8217_0003;
pub(in crate::mapi) const PID_LID_RECURRING_TAG: u32 = 0x8223_000B;
pub(in crate::mapi) const PID_LID_OUTLOOK_APPOINTMENT_8F07_TAG: u32 = 0x8F07_000B;
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
pub(in crate::mapi) const PID_LID_SIDE_EFFECTS_TAG: u32 = 0x8510_0003;
pub(in crate::mapi) const PID_LID_OUTLOOK_COMMON_8514_TAG: u32 = 0x8514_000B;
pub(in crate::mapi) const PID_LID_OUTLOOK_COMMON_8578_TAG: u32 = 0x8578_0003;
pub(in crate::mapi) const PID_LID_OUTLOOK_COMMON_85EF_TAG: u32 = 0x85EF_000B;
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
pub(in crate::mapi) const PID_LID_CONTACT_LINK_SEARCH_KEY_TAG: u32 = 0x8584_0102;
pub(in crate::mapi) const PID_LID_CONTACT_LINK_ENTRY_TAG: u32 = 0x8585_0102;
pub(in crate::mapi) const PID_LID_CONTACT_LINK_NAME_W_TAG: u32 = 0x8586_001F;
pub(in crate::mapi) const PID_LID_CONTACT_LINK_NAME_STRING8_TAG: u32 = 0x8586_001E;
pub(in crate::mapi) const PID_LID_EMAIL1_ADDRESS_TYPE_W_TAG: u32 = 0x8082_001F;
pub(in crate::mapi) const PID_LID_EMAIL1_DISPLAY_NAME_W_TAG: u32 = 0x8080_001F;
pub(in crate::mapi) const PID_LID_EMAIL1_EMAIL_ADDRESS_W_TAG: u32 = 0x8083_001F;
pub(in crate::mapi) const PID_LID_EMAIL1_ORIGINAL_DISPLAY_NAME_W_TAG: u32 = 0x8084_001F;
pub(in crate::mapi) const PID_LID_EMAIL2_ADDRESS_TYPE_W_TAG: u32 = 0x8092_001F;
pub(in crate::mapi) const PID_LID_EMAIL2_DISPLAY_NAME_W_TAG: u32 = 0x8090_001F;
pub(in crate::mapi) const PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG: u32 = 0x8093_001F;
pub(in crate::mapi) const PID_LID_EMAIL2_ORIGINAL_DISPLAY_NAME_W_TAG: u32 = 0x8094_001F;
pub(in crate::mapi) const PID_LID_EMAIL3_ADDRESS_TYPE_W_TAG: u32 = 0x80A2_001F;
pub(in crate::mapi) const PID_LID_EMAIL3_DISPLAY_NAME_W_TAG: u32 = 0x80A0_001F;
pub(in crate::mapi) const PID_LID_EMAIL3_EMAIL_ADDRESS_W_TAG: u32 = 0x80A3_001F;
pub(in crate::mapi) const PID_LID_EMAIL3_ORIGINAL_DISPLAY_NAME_W_TAG: u32 = 0x80A4_001F;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_SOURCE_80E0_TAG: u32 = 0x80E0_000B;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_SOURCE_80E2_TAG: u32 = 0x80E2_0102;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_SOURCE_80E3_TAG: u32 = 0x80E3_101F;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_SOURCE_80E5_TAG: u32 = 0x80E5_1102;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_SOURCE_80E6_TAG: u32 = 0x80E6_0003;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_SOURCE_80E8_TAG: u32 = 0x80E8_0048;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_ADDRESS_TYPE_W_TAG: u32 =
    0x80B5_001F;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_DISPLAY_NAME_W_TAG: u32 =
    0x80B6_001F;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_EMAIL_ADDRESS_W_TAG: u32 =
    0x80B7_001F;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_ADDRESS_TYPE_W_TAG: u32 =
    0x80D5_001F;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_DISPLAY_NAME_W_TAG: u32 =
    0x80D6_001F;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_EMAIL_ADDRESS_W_TAG: u32 =
    0x80D7_001F;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS3_ADDRESS_TYPE_W_TAG: u32 =
    0x805F_001F;
pub(in crate::mapi) const PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS3_EMAIL_ADDRESS_W_TAG: u32 =
    0x8060_001F;
pub(in crate::mapi) const PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG: u32 = 0x85C6_0102;
pub(in crate::mapi) const PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG: u32 = 0x85C7_0102;
pub(in crate::mapi) const PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG: u32 = 0x85C8_0040;
pub(in crate::mapi) const PID_LID_CONVERSATION_PROCESSED_TAG: u32 = 0x85C9_0003;
pub(in crate::mapi) const PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG: u32 = 0x85CA_0040;
pub(in crate::mapi) const PID_LID_CONVERSATION_ACTION_VERSION_TAG: u32 = 0x85CB_0003;
pub(in crate::mapi) const PID_NAME_KEYWORDS_TAG: u32 = 0x9000_101F;
pub(in crate::mapi) const PID_NAME_OSC_CONTACT_SOURCES_TAG: u32 = 0x8450_101F;
pub(in crate::mapi) const OUTLOOK_COMPACT_VIEW_AUXILIARY_FLAGS_TAG: u32 = 0x1213_0003;
pub(in crate::mapi) const OUTLOOK_MESSAGES_VIEW_BINARY_0F03_TAG: u32 = 0x0F03_0102;
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
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_PROVIDER_GUID_TAG: u32 = 0x8A01_0048;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_REMOTE_NAME_TAG: u32 = 0x8A07_001F;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_REMOTE_UID_TAG: u32 = 0x8A08_001F;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_LOCAL_TYPE_TAG: u32 = 0x8A1C_0048;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_CAPABILITIES_TAG: u32 = 0x8A67_0003;
pub(in crate::mapi) const PID_LID_OUTLOOK_SHARING_8AA6_TAG: u32 = 0x8AA6_0003;
