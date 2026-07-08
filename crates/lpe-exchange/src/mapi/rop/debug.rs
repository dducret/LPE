use super::super::properties::*;
use super::{
    associated_config_property_value, associated_config_property_value_with_mailbox_guid,
    canonical_property_storage_tag, collaboration_folder_property_value,
    common_view_named_view_property_value, flagged_property_error_code, folder_row_for_id,
    is_advertised_special_folder, logon_property_value,
    mailbox_property_value_with_context_for_account, mapi_properties_from_json, message_for_id,
    modeled_zero_or_default_property, native_body_format, outlook_folder_view_definition,
    parse_mapi_property_value, property_is_unsupported_for_object, public_folder_property_value,
    search_folder_message_for_id, serialize_logon_row, serialize_object_property,
    special_folder_identification_property_value, special_folder_property_value,
    unsupported_specific_property_tags, utf16le_bytes, view_descriptor_all_property_tags,
    view_descriptor_binary, view_descriptor_strings, write_property_default, AccountPrincipal,
    Cursor, JmapEmail, JmapMailbox, MapiMailStoreSnapshot, MapiObject, MapiValue, RopRequest,
    CONTACTS_SEARCH_FOLDER_ID, FOLDER_GENERIC, FOLDER_ROOT, FOLDER_SEARCH, INBOX_FOLDER_ID,
    NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B,
    OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835, OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C,
    PID_TAG_BODY_HTML_W, PID_TAG_BODY_STRING8, PID_TAG_BODY_W, PID_TAG_COMMON_VIEWS_ENTRY_ID,
    PID_TAG_DEFAULT_VIEW_ENTRY_ID, PID_TAG_FINDER_ENTRY_ID, PID_TAG_FOLDER_TYPE,
    PID_TAG_HTML_BINARY, PID_TAG_IPM_APPOINTMENT_ENTRY_ID, PID_TAG_IPM_ARCHIVE_ENTRY_ID,
    PID_TAG_IPM_CONTACT_ENTRY_ID, PID_TAG_IPM_DRAFTS_ENTRY_ID, PID_TAG_IPM_JOURNAL_ENTRY_ID,
    PID_TAG_IPM_NOTE_ENTRY_ID, PID_TAG_IPM_OUTBOX_ENTRY_ID, PID_TAG_IPM_PUBLIC_FOLDERS_ENTRY_ID,
    PID_TAG_IPM_SENTMAIL_ENTRY_ID, PID_TAG_IPM_SUBTREE_ENTRY_ID, PID_TAG_IPM_TASK_ENTRY_ID,
    PID_TAG_IPM_WASTEBASKET_ENTRY_ID, PID_TAG_MAILBOX_OWNER_ENTRY_ID, PID_TAG_MAILBOX_OWNER_NAME_W,
    PID_TAG_MAX_SUBMIT_MESSAGE_SIZE, PID_TAG_MESSAGE_SIZE_EXTENDED, PID_TAG_NATIVE_BODY,
    PID_TAG_OUTLOOK_STORE_STATE, PID_TAG_PRIVATE, PID_TAG_PROHIBIT_RECEIVE_QUOTA,
    PID_TAG_PROHIBIT_SEND_QUOTA, PID_TAG_REM_OFFLINE_ENTRY_ID, PID_TAG_REM_ONLINE_ENTRY_ID,
    PID_TAG_RESOURCE_FLAGS, PID_TAG_ROAMING_DATATYPES, PID_TAG_ROAMING_DICTIONARY,
    PID_TAG_ROAMING_XML_STREAM, PID_TAG_RTF_COMPRESSED, PID_TAG_RTF_IN_SYNC,
    PID_TAG_SERVER_ACCOUNT_ICON, PID_TAG_SERVER_CONNECTED_ICON, PID_TAG_SERVER_TYPE_DISPLAY_NAME_W,
    PID_TAG_STORAGE_QUOTA_LIMIT, PID_TAG_USER_ENTRY_ID, PID_TAG_USER_GUID, PID_TAG_VIEWS_ENTRY_ID,
    PID_TAG_VIEW_DESCRIPTOR_BINARY, PID_TAG_VIEW_DESCRIPTOR_NAME_W,
    PID_TAG_VIEW_DESCRIPTOR_STRINGS_W, PID_TAG_VIEW_DESCRIPTOR_VERSION,
    PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL, PUBLIC_FOLDERS_ROOT_FOLDER_ID, REMINDERS_FOLDER_ID,
    ROOT_FOLDER_ID, SEARCH_FOLDER_ID, TODO_SEARCH_FOLDER_ID, TRACKED_MAIL_PROCESSING_FOLDER_ID,
};
use lpe_domain::crypto::sha256_hex_prefix;

mod folders;
mod shapes;

pub(in crate::mapi) use folders::*;
pub(in crate::mapi) use shapes::*;

pub(in crate::mapi) fn property_row_kind_for_debug(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
) -> &'static str {
    if !unsupported_specific_property_tags(object, principal, mailboxes, emails, snapshot, columns)
        .is_empty()
    {
        "flagged"
    } else {
        "standard"
    }
}

pub(in crate::mapi) fn format_returned_property_tags_for_debug(
    columns: &[u32],
    unsupported_tags: &[u32],
) -> String {
    let returned = columns
        .iter()
        .copied()
        .filter(|tag| !unsupported_tags.contains(tag))
        .collect::<Vec<_>>();
    format_property_tags_for_debug(&returned)
}

pub(in crate::mapi) fn format_property_tags_for_debug(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| format!("{tag:#010x}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(in crate::mapi) fn format_property_names_for_debug(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| property_tag_debug_name(*tag))
        .collect::<Vec<_>>()
        .join(",")
}

pub(in crate::mapi) fn property_tag_debug_name(tag: u32) -> &'static str {
    match tag {
        PID_TAG_DISPLAY_NAME_W => "PidTagDisplayName",
        PID_TAG_ENTRY_ID => "PidTagEntryId",
        PID_TAG_RECORD_KEY => "PidTagRecordKey",
        PID_TAG_SEARCH_KEY => "PidTagSearchKey",
        PID_TAG_CREATOR_NAME_W => "PidTagCreatorName",
        PID_TAG_CREATOR_ENTRY_ID => "PidTagCreatorEntryId",
        PID_TAG_LAST_MODIFIER_NAME_W => "PidTagLastModifierName",
        PID_TAG_LAST_MODIFIER_ENTRY_ID => "PidTagLastModifierEntryId",
        PID_TAG_CREATION_TIME => "PidTagCreationTime",
        PID_TAG_SOURCE_KEY => "PidTagSourceKey",
        PID_TAG_PARENT_SOURCE_KEY => "PidTagParentSourceKey",
        PID_TAG_PARENT_ENTRY_ID => "PidTagParentEntryId",
        PID_TAG_FOLDER_ID => "PidTagFolderId",
        PID_TAG_PARENT_FOLDER_ID => "PidTagParentFolderId",
        PID_TAG_INSTANCE_KEY => "PidTagInstanceKey",
        PID_TAG_FOLDER_TYPE => "PidTagFolderType",
        OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C => "OutlookUndocumentedFolderBinary120C",
        PID_TAG_MESSAGE_CLASS_W | PID_TAG_MESSAGE_CLASS_STRING8 => "PidTagMessageClass",
        PID_TAG_ORIGINAL_MESSAGE_CLASS_W => "PidTagOriginalMessageClass",
        PID_TAG_IMPORTANCE => "PidTagImportance",
        PID_TAG_SENT_REPRESENTING_NAME_W => "PidTagSentRepresentingName",
        PID_TAG_SENT_REPRESENTING_ENTRY_ID => "PidTagSentRepresentingEntryId",
        PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W => "PidTagSentRepresentingAddressType",
        PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W => "PidTagSentRepresentingEmailAddress",
        PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W => "PidTagSentRepresentingSmtpAddress",
        PID_TAG_VIEW_DESCRIPTOR_CLSID => "PidTagViewDescriptorCLSID",
        PID_TAG_VIEW_DESCRIPTOR_FLAGS => "PidTagViewDescriptorFlags",
        OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835 => "OutlookCommonViewDescriptorBinary6835",
        PID_TAG_VIEW_DESCRIPTOR_VERSION => "PidTagViewDescriptorVersion",
        OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C => "OutlookCommonViewDescriptorStrings683C",
        PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE => "PidTagViewDescriptorFolderType",
        PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE => "PidTagViewDescriptorViewMode",
        PID_TAG_VIEW_DESCRIPTOR_BINARY => "PidTagViewDescriptorBinary",
        PID_TAG_VIEW_DESCRIPTOR_STRINGS_W => "PidTagViewDescriptorStrings",
        PID_TAG_VIEW_DESCRIPTOR_NAME_W => "PidTagViewDescriptorName",
        PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL => "PidTagViewDescriptorVersionCanonical",
        PID_TAG_WLINK_GROUP_HEADER_ID => "PidTagWlinkGroupHeaderId",
        PID_TAG_WLINK_SAVE_STAMP => "PidTagWlinkSaveStamp",
        PID_TAG_WLINK_TYPE => "PidTagWlinkType",
        PID_TAG_WLINK_FLAGS => "PidTagWlinkFlags",
        PID_TAG_WLINK_ORDINAL => "PidTagWlinkOrdinal",
        PID_TAG_WLINK_ENTRY_ID => "PidTagWlinkEntryId",
        PID_TAG_WLINK_RECORD_KEY => "PidTagWlinkRecordKey",
        PID_TAG_WLINK_STORE_ENTRY_ID => "PidTagWlinkStoreEntryId",
        PID_TAG_WLINK_FOLDER_TYPE => "PidTagWlinkFolderType",
        PID_TAG_WLINK_GROUP_CLSID => "PidTagWlinkGroupClsid",
        PID_TAG_WLINK_GROUP_NAME_W => "PidTagWlinkGroupName",
        PID_TAG_WLINK_SECTION => "PidTagWlinkSection",
        PID_TAG_WLINK_CALENDAR_COLOR => "PidTagWlinkCalendarColor",
        PID_TAG_WLINK_ADDRESS_BOOK_EID => "PidTagWlinkAddressBookEid",
        PID_TAG_WLINK_CLIENT_ID => "PidTagWlinkClientId",
        PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID => "PidTagWlinkAddressBookStoreEid",
        PID_TAG_WLINK_RO_GROUP_TYPE => "PidTagWlinkRoGroupType",
        OUTLOOK_STALE_SHARING_LOCAL_FOLDER_ID_TAG => {
            "OutlookStaleSharingCalendarGroupEntryAssociatedLocalFolderId"
        }
        OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B => "OutlookAssociatedConfigBinary0E0B",
        PID_NAME_CONTENT_CLASS_W_TAG => "PidNameContentClass",
        PID_NAME_CONTENT_TYPE_W_TAG => "PidNameContentType",
        PID_TAG_MESSAGE_STATUS => "PidTagMessageStatus",
        PID_TAG_CONTENT_COUNT => "PidTagContentCount",
        PID_TAG_ASSOCIATED_CONTENT_COUNT => "PidTagAssociatedContentCount",
        PID_TAG_CONTAINER_CLASS_W => "PidTagContainerClass",
        PID_TAG_CONTENT_UNREAD_COUNT => "PidTagContentUnreadCount",
        PID_TAG_SUBFOLDERS => "PidTagSubfolders",
        PID_TAG_IPM_SUBTREE_ENTRY_ID => "PidTagIpmSubtreeEntryId",
        PID_TAG_IPM_OUTBOX_ENTRY_ID => "PidTagIpmOutboxEntryId",
        PID_TAG_IPM_WASTEBASKET_ENTRY_ID => "PidTagIpmWastebasketEntryId",
        PID_TAG_IPM_SENTMAIL_ENTRY_ID => "PidTagIpmSentMailEntryId",
        PID_TAG_VIEWS_ENTRY_ID => "PidTagViewsEntryId",
        PID_TAG_COMMON_VIEWS_ENTRY_ID => "PidTagCommonViewsEntryId",
        PID_TAG_FINDER_ENTRY_ID => "PidTagFinderEntryId",
        PID_TAG_IPM_ARCHIVE_ENTRY_ID => "PidTagIpmArchiveEntryId",
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID => "PidTagIpmAppointmentEntryId",
        PID_TAG_IPM_CONTACT_ENTRY_ID => "PidTagIpmContactEntryId",
        PID_TAG_IPM_JOURNAL_ENTRY_ID => "PidTagIpmJournalEntryId",
        PID_TAG_IPM_NOTE_ENTRY_ID => "PidTagIpmNoteEntryId",
        PID_TAG_IPM_TASK_ENTRY_ID => "PidTagIpmTaskEntryId",
        PID_TAG_REM_ONLINE_ENTRY_ID => "PidTagRemOnlineEntryId",
        PID_TAG_REM_OFFLINE_ENTRY_ID => "PidTagRemOfflineEntryId",
        PID_TAG_IPM_DRAFTS_ENTRY_ID => "PidTagIpmDraftsEntryId",
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS => "PidTagAdditionalRenEntryIds",
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX => "PidTagAdditionalRenEntryIdsEx",
        PID_TAG_FREE_BUSY_ENTRY_IDS => "PidTagFreeBusyEntryIds",
        PID_TAG_EMAIL_ADDRESS_W => "PidTagEmailAddress",
        PID_TAG_SMTP_ADDRESS_W => "PidTagSmtpAddress",
        PID_TAG_OBJECT_TYPE => "PidTagObjectType",
        PID_TAG_DISPLAY_TYPE => "PidTagDisplayType",
        PID_TAG_DISPLAY_TYPE_EX => "PidTagDisplayTypeEx",
        PID_TAG_ADDRESS_BOOK_DISPLAY_NAME_PRINTABLE_W => "PidTagAddressBookDisplayNamePrintable",
        PID_TAG_SEND_INTERNET_ENCODING => "PidTagSendInternetEncoding",
        PID_TAG_RECIPIENT_DISPLAY_NAME_W => "PidTagRecipientDisplayName",
        PID_TAG_RECIPIENT_ENTRY_ID => "PidTagRecipientEntryId",
        PID_TAG_RECIPIENT_FLAGS => "PidTagRecipientFlags",
        PID_TAG_RECIPIENT_ORDER => "PidTagRecipientOrder",
        PID_TAG_RECIPIENT_TRACK_STATUS => "PidTagRecipientTrackStatus",
        OUTLOOK_RECIPIENT_5FDE => "OutlookRecipient5FDE",
        PID_TAG_SENDER_ADDRESS_TYPE_W => "PidTagSenderAddressType",
        PID_TAG_SENDER_NAME_W => "PidTagSenderName",
        PID_TAG_SENDER_EMAIL_ADDRESS_W => "PidTagSenderEmailAddress",
        PID_TAG_SENDER_SMTP_ADDRESS_W => "PidTagSenderSmtpAddress",
        PID_TAG_CLIENT_SUBMIT_TIME => "PidTagClientSubmitTime",
        PID_TAG_MESSAGE_DELIVERY_TIME => "PidTagMessageDeliveryTime",
        PID_TAG_DISPLAY_BCC_W => "PidTagDisplayBcc",
        PID_TAG_DISPLAY_CC_W => "PidTagDisplayCc",
        PID_TAG_DISPLAY_TO_W => "PidTagDisplayTo",
        PID_TAG_SUBJECT_W => "PidTagSubject",
        PID_TAG_SUBJECT_PREFIX_W => "PidTagSubjectPrefix",
        PID_TAG_NORMALIZED_SUBJECT_W => "PidTagNormalizedSubject",
        PID_TAG_TRANSPORT_MESSAGE_HEADERS_W => "PidTagTransportMessageHeaders",
        PID_TAG_BODY_STRING8 | PID_TAG_BODY_W => "PidTagBody",
        PID_TAG_RTF_COMPRESSED => "PidTagRtfCompressed",
        PID_TAG_BODY_HTML_W => "PidTagBodyHtml",
        PID_TAG_HTML_BINARY => "PidTagHtml",
        PID_TAG_RTF_IN_SYNC => "PidTagRtfInSync",
        PID_TAG_NATIVE_BODY => "PidTagNativeBody",
        PID_TAG_HAS_ATTACHMENTS => "PidTagHasAttachments",
        PID_TAG_TRUST_SENDER => "PidTagTrustSender",
        PID_TAG_HAS_NAMED_PROPERTIES => "PidTagHasNamedProperties",
        PID_TAG_MESSAGE_FLAGS => "PidTagMessageFlags",
        PID_TAG_MESSAGE_SIZE => "PidTagMessageSize",
        PID_TAG_READ => "PidTagRead",
        PID_TAG_INTERNET_CODEPAGE => "PidTagInternetCodepage",
        PID_TAG_MESSAGE_LOCALE_ID => "PidTagMessageLocaleId",
        PID_TAG_LOCALE_ID => "PidTagLocaleId",
        PID_TAG_INTERNET_MESSAGE_ID_W => "PidTagInternetMessageId",
        PID_TAG_EXTENDED_RULE_MESSAGE_ACTIONS => "PidTagExtendedRuleMessageActions",
        PID_TAG_FLAG_STATUS => "PidTagFlagStatus",
        PID_TAG_SWAPPED_TODO_STORE => "PidTagSwappedToDoStore",
        PID_TAG_LAST_MODIFICATION_TIME => "PidTagLastModificationTime",
        PID_TAG_SERIALIZED_REPLID_GUID_MAP => "PidTagSerializedReplidGuidMap",
        PID_TAG_RESOURCE_FLAGS => "PidTagResourceFlags",
        PID_TAG_USER_ENTRY_ID => "PidTagUserEntryId",
        PID_TAG_MAILBOX_OWNER_ENTRY_ID => "PidTagMailboxOwnerEntryId",
        PID_TAG_MAILBOX_OWNER_NAME_W => "PidTagMailboxOwnerName",
        PID_TAG_ASSOCIATED_SHARING_PROVIDER => "PidTagAssociatedSharingProvider",
        PID_TAG_IPM_PUBLIC_FOLDERS_ENTRY_ID => "PidTagIpmPublicFoldersEntryId",
        PID_TAG_SERVER_TYPE_DISPLAY_NAME_W => "PidTagServerTypeDisplayName",
        PID_TAG_SERVER_CONNECTED_ICON => "PidTagServerConnectedIcon",
        PID_TAG_SERVER_ACCOUNT_ICON => "PidTagServerAccountIcon",
        PID_TAG_OUTLOOK_STORE_STATE => "OutlookStoreState",
        PID_TAG_PRIVATE => "PidTagPrivate",
        PID_TAG_USER_GUID => "PidTagUserGuid",
        PID_TAG_MESSAGE_SIZE_EXTENDED => "PidTagMessageSizeExtended",
        PID_TAG_PROHIBIT_RECEIVE_QUOTA => "PidTagProhibitReceiveQuota",
        PID_TAG_MAX_SUBMIT_MESSAGE_SIZE => "PidTagMaxSubmitMessageSize",
        PID_TAG_PROHIBIT_SEND_QUOTA => "PidTagProhibitSendQuota",
        PID_TAG_STORAGE_QUOTA_LIMIT => "PidTagStorageQuotaLimit",
        PID_TAG_EXTENDED_RULE_SIZE_LIMIT => "PidTagExtendedRuleSizeLimit",
        PID_TAG_PST_PATH_W => "PidTagPstPath",
        PID_TAG_ATTACH_NUM => "PidTagAttachNumber",
        PID_TAG_ATTACH_EXTENSION_W => "PidTagAttachExtension",
        PID_TAG_ATTACH_FILENAME_W => "PidTagAttachFilename",
        PID_TAG_ATTACH_METHOD => "PidTagAttachMethod",
        PID_TAG_ATTACH_LONG_FILENAME_W => "PidTagAttachLongFilename",
        PID_TAG_ATTACH_RENDERING => "PidTagAttachRendering",
        PID_TAG_RENDERING_POSITION => "PidTagRenderingPosition",
        PID_TAG_ATTACH_MIME_TAG_W => "PidTagAttachMimeTag",
        PID_TAG_ATTACH_CONTENT_ID_W => "PidTagAttachContentId",
        PID_TAG_ATTACH_FLAGS => "PidTagAttachFlags",
        PID_TAG_ATTACHMENT_LINK_ID => "PidTagAttachmentLinkId",
        PID_TAG_ATTACHMENT_FLAGS => "PidTagAttachmentFlags",
        PID_TAG_ATTACHMENT_HIDDEN => "PidTagAttachmentHidden",
        PID_TAG_LOCAL_COMMIT_TIME_MAX => "PidTagLocalCommitTimeMax",
        PID_TAG_DELETED_COUNT_TOTAL => "PidTagDeletedCountTotal",
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => {
            "PidTagDefaultPostMessageClass"
        }
        PID_TAG_DEFAULT_FORM_NAME_W => "PidTagDefaultFormName",
        PID_TAG_DEFAULT_VIEW_ENTRY_ID => "PidTagDefaultViewEntryId",
        PID_TAG_FOLDER_FORM_FLAGS => "PidTagFolderFormFlags",
        PID_TAG_FOLDER_WEBVIEWINFO => "PidTagFolderWebViewInfo",
        PID_TAG_FOLDER_XVIEWINFO_E => "PidTagFolderXViewInfoE",
        PID_TAG_FOLDER_VIEWS_ONLY => "PidTagFolderViewsOnly",
        PID_TAG_FOLDER_FORM_STORAGE => "PidTagFolderFormStorage",
        PID_TAG_EXTENDED_FOLDER_FLAGS => "PidTagExtendedFolderFlags",
        PID_TAG_SEARCH_FOLDER_ID => "PidTagSearchFolderId",
        PID_TAG_SEARCH_FOLDER_STORAGE_TYPE => "PidTagSearchFolderStorageType",
        PID_TAG_SEARCH_FOLDER_EFP_FLAGS => "PidTagSearchFolderEfpFlags",
        PID_TAG_SEARCH_FOLDER_DEFINITION => "PidTagSearchFolderDefinition",
        PID_TAG_ARCHIVE_TAG => "PidTagArchiveTag",
        PID_TAG_POLICY_TAG => "PidTagPolicyTag",
        PID_TAG_RETENTION_PERIOD => "PidTagRetentionPeriod",
        PID_TAG_RETENTION_FLAGS => "PidTagRetentionFlags",
        PID_TAG_ARCHIVE_PERIOD => "PidTagArchivePeriod",
        PID_TAG_RIGHTS => "PidTagRights",
        PID_TAG_FOLDER_VIEWLIST_FLAGS => "PidTagFolderViewListFlags",
        PID_TAG_SENT_MAIL_SVR_EID => "PidTagSentMailSvrEID",
        tag if is_acl_member_name_property_tag(tag) => "PidTagMemberName",
        PID_LID_PERCENT_COMPLETE_TAG => "PidLidPercentComplete",
        PID_LID_LOCATION_W_TAG => "PidLidLocation",
        PID_LID_APPOINTMENT_DURATION_TAG => "PidLidAppointmentDuration",
        PID_LID_APPOINTMENT_START_WHOLE_TAG => "PidLidAppointmentStartWhole",
        PID_LID_APPOINTMENT_END_WHOLE_TAG => "PidLidAppointmentEndWhole",
        PID_LID_BUSY_STATUS_TAG => "PidLidBusyStatus",
        PID_LID_APPOINTMENT_SUB_TYPE_TAG => "PidLidAppointmentSubType",
        PID_LID_APPOINTMENT_STATE_FLAGS_TAG => "PidLidAppointmentStateFlags",
        PID_LID_TIME_ZONE_STRUCT_TAG => "PidLidTimeZoneStruct",
        PID_LID_TIME_ZONE_DESCRIPTION_W_TAG => "PidLidTimeZoneDescription",
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG => {
            "PidLidAppointmentTimeZoneDefinitionStartDisplay"
        }
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG => {
            "PidLidAppointmentTimeZoneDefinitionEndDisplay"
        }
        PID_TAG_CHANGE_KEY => "PidTagChangeKey",
        PID_TAG_ACCESS => "PidTagAccess",
        PID_TAG_ACCESS_LEVEL => "PidTagAccessLevel",
        PID_TAG_CONVERSATION_TOPIC_W => "PidTagConversationTopic",
        PID_TAG_CONVERSATION_INDEX => "PidTagConversationIndex",
        PID_TAG_ROAMING_DATATYPES => "PidTagRoamingDatatypes",
        PID_TAG_ROAMING_DICTIONARY => "PidTagRoamingDictionary",
        PID_TAG_ROAMING_XML_STREAM => "PidTagRoamingXmlStream",
        0x7C09_0102 => "PidTagRoamingBinary",
        0x685D_0003 => "OutlookConfigurationStamp",
        tag if debug_property_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_CLSID) => {
            "PidTagViewDescriptorCLSID"
        }
        tag if debug_property_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_FLAGS) => {
            "PidTagViewDescriptorFlags"
        }
        tag if debug_property_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_VERSION) => {
            "PidTagViewDescriptorVersion"
        }
        tag if debug_property_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL) => {
            "PidTagViewDescriptorVersionCanonical"
        }
        tag if debug_property_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_NAME_W) => {
            "PidTagViewDescriptorName"
        }
        tag if debug_property_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_STRINGS_W) => {
            "PidTagViewDescriptorStrings"
        }
        tag if debug_property_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE) => {
            "PidTagViewDescriptorFolderType"
        }
        tag if debug_property_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE) => {
            "PidTagViewDescriptorViewMode"
        }
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_GROUP_HEADER_ID) => {
            "PidTagWlinkGroupHeaderId"
        }
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_SAVE_STAMP) => "PidTagWlinkSaveStamp",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_TYPE) => "PidTagWlinkType",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_FLAGS) => "PidTagWlinkFlags",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_ORDINAL) => "PidTagWlinkOrdinal",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_ENTRY_ID) => "PidTagWlinkEntryId",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_RECORD_KEY) => "PidTagWlinkRecordKey",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_STORE_ENTRY_ID) => {
            "PidTagWlinkStoreEntryId"
        }
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_FOLDER_TYPE) => "PidTagWlinkFolderType",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_GROUP_CLSID) => "PidTagWlinkGroupClsid",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_GROUP_NAME_W) => "PidTagWlinkGroupName",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_SECTION) => "PidTagWlinkSection",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_CALENDAR_COLOR) => {
            "PidTagWlinkCalendarColor"
        }
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_ADDRESS_BOOK_EID) => {
            "PidTagWlinkAddressBookEid"
        }
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_CLIENT_ID) => "PidTagWlinkClientId",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID) => {
            "PidTagWlinkAddressBookStoreEid"
        }
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_RO_GROUP_TYPE) => {
            "PidTagWlinkRoGroupType"
        }
        tag if debug_property_id_matches(tag, PID_TAG_USER_GUID) => "PidTagUserGuid",
        PID_TAG_OST_OSTID => "PR_OST_OSTID",
        _ => "unknown",
    }
}

fn debug_property_id_matches(tag: u32, known_tag: u32) -> bool {
    tag & 0xffff_0000 == known_tag & 0xffff_0000
}

pub(in crate::mapi) fn format_property_errors_for_debug(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    tags: &[u32],
) -> String {
    tags.iter()
        .map(|tag| {
            format!(
                "{tag:#010x}:{}:{:#010x}",
                property_tag_debug_name(*tag),
                flagged_property_error_code(object, principal, mailboxes, emails, snapshot, *tag)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(in crate::mapi) fn log_get_properties_specific_debug(
    request: &RopRequest,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    columns: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) {
    let mut defaulted_tags = Vec::new();
    let mut intentional_default_tags = Vec::new();
    let mut fallback_default_tags = Vec::new();
    for tag in columns {
        if property_is_unsupported_for_object(object, principal, *tag) {
            continue;
        }
        let value = serialize_object_property(object, principal, mailboxes, emails, snapshot, *tag);
        let mut default_value = Vec::new();
        write_property_default(&mut default_value, *tag);
        if value == default_value {
            defaulted_tags.push(*tag);
            if modeled_zero_or_default_property(object, *tag) {
                intentional_default_tags.push(*tag);
            } else {
                fallback_default_tags.push(*tag);
            }
        }
    }
    let flagged_error_tags =
        unsupported_specific_property_tags(object, principal, mailboxes, emails, snapshot, columns);
    let (object_kind, folder_id, item_id) = mapi_object_debug_fields(object);
    let default_folder_mappings = default_folder_property_mappings_for_debug(columns);
    let returned_property_value_shapes = format_property_value_shapes_for_debug(
        object,
        principal,
        columns,
        mailboxes,
        emails,
        snapshot,
        &flagged_error_tags,
    );
    let outlook_bootstrap_getprops = is_outlook_logon_bootstrap_getprops(object, columns);
    let outlook_bootstrap_property_details = if outlook_bootstrap_getprops {
        format_outlook_logon_bootstrap_property_details(principal, columns)
    } else {
        String::new()
    };
    let outlook_bootstrap_row_shape = if outlook_bootstrap_getprops {
        outlook_logon_bootstrap_row_shape(principal, columns)
    } else {
        OutlookLogonBootstrapRowShape::default()
    };
    let ipm_configuration_getprops_contract = format_ipm_configuration_getprops_contract(
        object,
        columns,
        snapshot,
        &fallback_default_tags,
    );
    let folder_type_getprops_contract =
        format_folder_type_getprops_contract(object, principal, columns, mailboxes, snapshot);
    let message_body_getprops_contract =
        format_message_body_getprops_contract(object, columns, mailboxes, emails, snapshot);
    let default_view_entry_id_decoding = format_default_view_entry_id_decoding(
        object, principal, columns, mailboxes, emails, snapshot,
    );
    let common_view_descriptor_getprops_contract =
        format_common_view_descriptor_getprops_contract(object, principal, columns, snapshot);
    let message = "rca debug mapi get properties specific";
    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x07",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.response_handle_index(),
        object_kind = object_kind,
        folder_id = %folder_id,
        item_id = %item_id,
        requested_property_tag_count = columns.len(),
        requested_property_tags = %format_property_tags_for_debug(columns),
        requested_property_names = %format_property_names_for_debug(columns),
        returned_property_tag_count = columns.len().saturating_sub(flagged_error_tags.len()),
        returned_property_tags = %format_returned_property_tags_for_debug(columns, &flagged_error_tags),
        zero_or_default_property_tag_count = defaulted_tags.len(),
        zero_or_default_property_tags = %format_property_tags_for_debug(&defaulted_tags),
        intentional_zero_or_default_property_tag_count = intentional_default_tags.len(),
        intentional_zero_or_default_property_tags = %format_property_tags_for_debug(&intentional_default_tags),
        fallback_default_property_tag_count = fallback_default_tags.len(),
        fallback_default_property_tags = %format_property_tags_for_debug(&fallback_default_tags),
        unsupported_property_tag_count = flagged_error_tags.len(),
        unsupported_property_tags = %format_property_tags_for_debug(&flagged_error_tags),
        default_ipm_folder_mapping_count = default_folder_mappings.len(),
        default_ipm_folder_mappings = %default_folder_mappings.join(","),
        response_property_row_kind = %property_row_kind_for_debug(
            object,
            principal,
            mailboxes,
            emails,
            snapshot,
            columns,
        ),
        unsupported_property_errors = %format_property_errors_for_debug(
            object,
            principal,
            mailboxes,
            emails,
            snapshot,
            &flagged_error_tags
        ),
        returned_property_value_shapes = %returned_property_value_shapes,
        ipm_configuration_getprops_contract = %ipm_configuration_getprops_contract,
        folder_type_getprops_contract = %folder_type_getprops_contract,
        message_body_getprops_contract = %message_body_getprops_contract,
        default_view_entry_id_decoding = %default_view_entry_id_decoding,
        common_view_descriptor_getprops_contract = %common_view_descriptor_getprops_contract,
        outlook_bootstrap_getprops = outlook_bootstrap_getprops,
        outlook_bootstrap_estimated_rop_payload_bytes =
            outlook_bootstrap_row_shape.estimated_rop_payload_bytes,
        outlook_bootstrap_property_row_bytes = outlook_bootstrap_row_shape.property_row_bytes,
        outlook_bootstrap_icon_row_bytes = outlook_bootstrap_row_shape.icon_row_bytes,
        outlook_bootstrap_non_icon_row_bytes = outlook_bootstrap_row_shape.non_icon_row_bytes,
        outlook_bootstrap_property_details = %outlook_bootstrap_property_details,
        message = message,
    );
    log_common_view_descriptor_getprops_summary(principal, request, object, columns, snapshot);
    log_calendar_default_folder_lookup_debug(
        object,
        principal,
        columns,
        mailboxes,
        emails,
        snapshot,
        &flagged_error_tags,
    );
}

#[derive(Default)]
pub(in crate::mapi) struct OutlookLogonBootstrapRowShape {
    pub(in crate::mapi) estimated_rop_payload_bytes: usize,
    pub(in crate::mapi) property_row_bytes: usize,
    pub(in crate::mapi) icon_row_bytes: usize,
    pub(in crate::mapi) non_icon_row_bytes: usize,
}

pub(in crate::mapi) fn outlook_logon_bootstrap_row_shape(
    principal: &AccountPrincipal,
    columns: &[u32],
) -> OutlookLogonBootstrapRowShape {
    let mut shape = OutlookLogonBootstrapRowShape::default();
    for tag in columns {
        if logon_property_value(principal, *tag).is_none() {
            continue;
        }
        let value = serialize_logon_row(principal, &[*tag]);
        shape.property_row_bytes += value.len();
        if matches!(
            *tag,
            PID_TAG_SERVER_CONNECTED_ICON | PID_TAG_SERVER_ACCOUNT_ICON
        ) {
            shape.icon_row_bytes += value.len();
        } else {
            shape.non_icon_row_bytes += value.len();
        }
    }
    shape.estimated_rop_payload_bytes = shape.property_row_bytes + 7;
    shape
}

pub(in crate::mapi) fn is_outlook_logon_bootstrap_getprops(
    object: Option<&MapiObject>,
    columns: &[u32],
) -> bool {
    const OUTLOOK_BOOTSTRAP_LOGON_PROPS: [u32; 9] = [
        PID_TAG_MAILBOX_OWNER_NAME_W,
        PID_TAG_MAILBOX_OWNER_ENTRY_ID,
        PID_TAG_SERVER_TYPE_DISPLAY_NAME_W,
        PID_TAG_SERVER_CONNECTED_ICON,
        PID_TAG_SERVER_ACCOUNT_ICON,
        PID_TAG_PRIVATE,
        PID_TAG_OUTLOOK_STORE_STATE,
        PID_TAG_USER_GUID,
        PID_TAG_MAX_SUBMIT_MESSAGE_SIZE,
    ];
    const OUTLOOK_BOOTSTRAP_LOGON_EXTENSION_PROPS: [u32; 3] = [
        PID_TAG_RESOURCE_FLAGS,
        PID_TAG_USER_ENTRY_ID,
        PID_TAG_IPM_PUBLIC_FOLDERS_ENTRY_ID,
    ];
    const REQUIRED_OUTLOOK_BOOTSTRAP_LOGON_PROPS: [u32; 8] = [
        PID_TAG_MAILBOX_OWNER_NAME_W,
        PID_TAG_MAILBOX_OWNER_ENTRY_ID,
        PID_TAG_SERVER_TYPE_DISPLAY_NAME_W,
        PID_TAG_SERVER_CONNECTED_ICON,
        PID_TAG_SERVER_ACCOUNT_ICON,
        PID_TAG_PRIVATE,
        PID_TAG_OUTLOOK_STORE_STATE,
        PID_TAG_USER_GUID,
    ];

    matches!(object, Some(MapiObject::Logon))
        && columns.len() >= REQUIRED_OUTLOOK_BOOTSTRAP_LOGON_PROPS.len()
        && columns.len()
            <= OUTLOOK_BOOTSTRAP_LOGON_PROPS.len() + OUTLOOK_BOOTSTRAP_LOGON_EXTENSION_PROPS.len()
        && REQUIRED_OUTLOOK_BOOTSTRAP_LOGON_PROPS
            .iter()
            .all(|expected| columns.contains(expected))
        && columns.iter().all(|tag| {
            OUTLOOK_BOOTSTRAP_LOGON_PROPS.contains(tag)
                || OUTLOOK_BOOTSTRAP_LOGON_EXTENSION_PROPS.contains(tag)
        })
}

pub(in crate::mapi) fn format_outlook_logon_bootstrap_property_details(
    principal: &AccountPrincipal,
    columns: &[u32],
) -> String {
    columns
        .iter()
        .filter_map(|tag| {
            let value = logon_property_value(principal, *tag)?;
            let detail = match (*tag, value) {
                (PID_TAG_MAILBOX_OWNER_ENTRY_ID, MapiValue::Binary(bytes)) => {
                    format_mailbox_owner_entry_id_details(&bytes)
                }
                (
                    PID_TAG_SERVER_CONNECTED_ICON | PID_TAG_SERVER_ACCOUNT_ICON,
                    MapiValue::Binary(bytes),
                ) => format_ico_header_details(&bytes),
                (PID_TAG_USER_GUID, MapiValue::Binary(bytes)) => {
                    format!(
                        "user_guid_bytes={};user_guid_hex={}",
                        bytes.len(),
                        hex_preview_for_debug(&bytes, bytes.len())
                    )
                }
                (PID_TAG_OUTLOOK_STORE_STATE, MapiValue::U32(value)) => {
                    format!("outlook_store_state={value:#010x}")
                }
                (PID_TAG_PRIVATE, MapiValue::Bool(value)) => format!("private={value}"),
                (PID_TAG_MAX_SUBMIT_MESSAGE_SIZE, MapiValue::U32(value)) => {
                    format!("max_submit_message_size_kb={value}")
                }
                (PID_TAG_MESSAGE_SIZE_EXTENDED, MapiValue::I64(value)) => {
                    format!("message_size_extended_octets={value}")
                }
                (
                    PID_TAG_PROHIBIT_RECEIVE_QUOTA
                    | PID_TAG_PROHIBIT_SEND_QUOTA
                    | PID_TAG_STORAGE_QUOTA_LIMIT,
                    MapiValue::U32(value),
                ) => format!("quota_limit_kb={value}"),
                (
                    PID_TAG_MAILBOX_OWNER_NAME_W | PID_TAG_SERVER_TYPE_DISPLAY_NAME_W,
                    MapiValue::String(value),
                ) => {
                    format!(
                        "string_chars={};string_preview={}",
                        value.chars().count(),
                        text_preview_for_debug(&value, 32)
                    )
                }
                (_, value) => mapi_value_shape_for_debug(&value),
            };
            Some(format!(
                "{tag:#010x}:{}:{detail}",
                property_tag_debug_name(*tag)
            ))
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn format_mailbox_owner_entry_id_details(bytes: &[u8]) -> String {
    if bytes.len() < 28 {
        return format!(
            "permanent_entry_id_len={};parse_error=too_short",
            bytes.len()
        );
    }

    let id_type = bytes[0];
    let reserved_1 = bytes[1];
    let reserved_2 = bytes[2];
    let reserved_3 = bytes[3];
    let provider_uid = &bytes[4..20];
    let reserved_4 = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
    let display_type = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
    let dn_bytes = &bytes[28..];
    let null_terminated = dn_bytes.last().copied() == Some(0);
    let dn_payload = if null_terminated {
        &dn_bytes[..dn_bytes.len().saturating_sub(1)]
    } else {
        dn_bytes
    };
    let distinguished_name = String::from_utf8_lossy(dn_payload);

    format!(
        "permanent_entry_id_len={};id_type={id_type:#04x};r1={reserved_1:#04x};r2={reserved_2:#04x};r3={reserved_3:#04x};provider_uid={};provider_uid_matches_nspi={};r4={reserved_4:#010x};display_type={display_type:#010x};dn_len={};dn_null_terminated={null_terminated};dn_preview={}",
        bytes.len(),
        hex_preview_for_debug(provider_uid, provider_uid.len()),
        provider_uid == NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID,
        dn_payload.len(),
        text_preview_for_debug(&distinguished_name, 96),
    )
}

fn format_ico_header_details(bytes: &[u8]) -> String {
    if bytes.len() < 22 {
        return format!("ico_len={};parse_error=too_short", bytes.len());
    }

    let reserved = u16::from_le_bytes(bytes[0..2].try_into().unwrap());
    let image_type = u16::from_le_bytes(bytes[2..4].try_into().unwrap());
    let image_count = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
    let width = bytes[6];
    let height = bytes[7];
    let color_count = bytes[8];
    let planes = u16::from_le_bytes(bytes[10..12].try_into().unwrap());
    let bit_count = u16::from_le_bytes(bytes[12..14].try_into().unwrap());
    let image_size = u32::from_le_bytes(bytes[14..18].try_into().unwrap());
    let image_offset = u32::from_le_bytes(bytes[18..22].try_into().unwrap());
    let length_matches_directory = image_offset
        .checked_add(image_size)
        .is_some_and(|expected| expected as usize == bytes.len());

    format!(
        "ico_len={};reserved={reserved:#06x};type={image_type:#06x};count={image_count};width={width};height={height};color_count={color_count};planes={planes};bit_count={bit_count};image_size={image_size};image_offset={image_offset};length_matches_directory={length_matches_directory}",
        bytes.len(),
    )
}

pub(in crate::mapi) fn expected_folder_type_for_debug(
    folder_id: u64,
    mailbox: Option<&JmapMailbox>,
    search_folder_found: bool,
) -> (&'static str, Option<u32>) {
    if matches!(folder_id, ROOT_FOLDER_ID | PUBLIC_FOLDERS_ROOT_FOLDER_ID) {
        return ("root", Some(FOLDER_ROOT));
    }
    if search_folder_found
        || advertised_special_search_folder_for_debug(folder_id)
        || mailbox
            .map(|mailbox| {
                mailbox.role == "__mapi_search" || mailbox.role.starts_with("__mapi_search_folder_")
            })
            .unwrap_or(false)
    {
        return ("search", Some(FOLDER_SEARCH));
    }
    if mailbox.is_some() || is_advertised_special_folder(folder_id) {
        return ("generic", Some(FOLDER_GENERIC));
    }
    ("unknown", None)
}

fn advertised_special_search_folder_for_debug(folder_id: u64) -> bool {
    matches!(
        folder_id,
        SEARCH_FOLDER_ID
            | CONTACTS_SEARCH_FOLDER_ID
            | REMINDERS_FOLDER_ID
            | TRACKED_MAIL_PROCESSING_FOLDER_ID
            | TODO_SEARCH_FOLDER_ID
    )
}

pub(in crate::mapi) fn folder_type_kind_for_debug(value: u32) -> &'static str {
    match value {
        FOLDER_ROOT => "root",
        FOLDER_GENERIC => "generic",
        FOLDER_SEARCH => "search",
        _ => "invalid",
    }
}

pub(in crate::mapi) fn format_property_value_shapes_for_debug(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    columns: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    unsupported_tags: &[u32],
) -> String {
    columns
        .iter()
        .map(|tag| {
            let name = property_tag_debug_name(*tag);
            if unsupported_tags.contains(tag) {
                return format!("{tag:#010x}:{name}:unsupported");
            }
            let encoded =
                serialize_object_property(object, principal, mailboxes, emails, snapshot, *tag);
            let mut default_value = Vec::new();
            write_property_default(&mut default_value, *tag);
            let default_kind = if encoded == default_value {
                if modeled_zero_or_default_property(object, *tag) {
                    ":default=intentional"
                } else {
                    ":default=fallback"
                }
            } else {
                ""
            };
            let semantic_shape =
                semantic_property_shape_for_debug(object, principal, snapshot, *tag)
                    .map(|shape| format!(":{shape}"))
                    .unwrap_or_default();
            format!(
                "{tag:#010x}:{name}:row_bytes={}{}:row_hex={}{}",
                encoded.len(),
                semantic_shape,
                hex_preview_for_debug(&encoded, 16),
                default_kind
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn semantic_property_shape_for_debug(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    snapshot: &MapiMailStoreSnapshot,
    tag: u32,
) -> Option<String> {
    match object {
        Some(MapiObject::Logon) => logon_property_value(principal, tag)
            .as_ref()
            .map(mapi_value_shape_for_debug),
        Some(MapiObject::PublicFolderLogon) => {
            (tag == PID_TAG_PRIVATE).then(|| mapi_value_shape_for_debug(&MapiValue::Bool(false)))
        }
        Some(MapiObject::Folder { .. }) => {
            special_folder_identification_property_value(principal.account_id, tag)
                .as_ref()
                .map(mapi_value_shape_for_debug)
        }
        Some(MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            saved_message,
        }) => snapshot
            .associated_config_message_for_id(*config_id)
            .or_else(|| saved_message.clone())
            .filter(|message| message.folder_id == *folder_id)
            .and_then(|message| {
                associated_config_property_value_with_mailbox_guid(
                    &message,
                    principal.account_id,
                    tag,
                )
            })
            .as_ref()
            .map(mapi_value_shape_for_debug),
        _ => None,
    }
}

pub(in crate::mapi) fn format_associated_config_0e0b_debug(
    columns: &[u32],
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
    fallback_tags: &[u32],
) -> String {
    if !columns.contains(&OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B) {
        return "requested=false".to_string();
    }
    let properties = mapi_properties_from_json(&message.properties_json);
    let mut property_json_tags = properties.keys().copied().collect::<Vec<_>>();
    property_json_tags.sort_unstable();
    let stored_value = properties.get(&OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B);
    let semantic_value =
        associated_config_property_value(message, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B);
    let semantic_shape = semantic_value
        .as_ref()
        .map(mapi_value_shape_for_debug)
        .unwrap_or_else(|| "missing".to_string());
    let roaming_dictionary_shape =
        associated_config_property_value(message, PID_TAG_ROAMING_DICTIONARY)
            .as_ref()
            .map(mapi_value_shape_for_debug)
            .unwrap_or_else(|| "missing".to_string());
    let datatypes = associated_config_property_value(message, PID_TAG_ROAMING_DATATYPES)
        .and_then(|value| value.into_u32());
    let dictionary_advertised = datatypes.is_some_and(|value| value & 0x0000_0004 != 0);
    let dictionary_payload_consistent = !dictionary_advertised
        || matches!(
            semantic_value.as_ref(),
            Some(MapiValue::Binary(value)) if !value.is_empty()
        );
    format!(
        "requested=true;public_ms_oxprops_name=unmapped;stored={};stored_shape={};semantic_shape={};roaming_datatypes={};dictionary_advertised={};roaming_dictionary_shape={};dictionary_payload_consistent={};fallback_default={};property_json_tags={}",
        stored_value.is_some(),
        stored_value
            .map(mapi_value_shape_for_debug)
            .unwrap_or_else(|| "missing".to_string()),
        semantic_shape,
        datatypes
            .map(|value| format!("0x{value:08x}"))
            .unwrap_or_else(|| "missing".to_string()),
        dictionary_advertised,
        roaming_dictionary_shape,
        dictionary_payload_consistent,
        fallback_tags.contains(&OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B),
        format_property_tags_for_debug(&property_json_tags)
    )
}

pub(in crate::mapi) fn common_view_descriptor_property_requested(columns: &[u32]) -> bool {
    columns.iter().any(|tag| {
        matches!(
            canonical_property_storage_tag(*tag),
            PID_TAG_VIEW_DESCRIPTOR_BINARY
                | OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835
                | OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C
                | PID_TAG_VIEW_DESCRIPTOR_STRINGS_W
                | PID_TAG_VIEW_DESCRIPTOR_NAME_W
                | PID_TAG_VIEW_DESCRIPTOR_VERSION
                | PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL
                | OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
        )
    })
}

pub(in crate::mapi) fn format_requested_view_descriptor_contract(columns: &[u32]) -> String {
    let mut parts = Vec::new();
    for (name, tags) in [
        (
            "version",
            &[
                PID_TAG_VIEW_DESCRIPTOR_VERSION,
                PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL,
            ][..],
        ),
        ("name", &[PID_TAG_VIEW_DESCRIPTOR_NAME_W][..]),
        (
            "binary",
            &[
                PID_TAG_VIEW_DESCRIPTOR_BINARY,
                OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835,
            ][..],
        ),
        (
            "strings",
            &[
                PID_TAG_VIEW_DESCRIPTOR_STRINGS_W,
                OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C,
            ][..],
        ),
    ] {
        parts.push(format!(
            "{name}={}",
            columns.iter().any(|column| {
                let storage_tag = canonical_property_storage_tag(*column);
                tags.iter().any(|tag| storage_tag == *tag)
            })
        ));
    }
    parts.join(";")
}

pub(in crate::mapi) fn view_descriptor_debug_property_tags(descriptor: &[u8]) -> Vec<u32> {
    view_descriptor_all_property_tags(descriptor)
}

pub(in crate::mapi) fn default_view_message_entry_id_target(entry_id: &[u8]) -> Option<(u64, u64)> {
    if entry_id.len() != 70
        || entry_id[0..4] != [0, 0, 0, 0]
        || entry_id[20..22] != 0x0007u16.to_le_bytes()
        || entry_id[44..46] != [0, 0]
        || entry_id[68..70] != [0, 0]
    {
        return None;
    }
    let folder_counter = crate::mapi::identity::global_counter_from_globcnt(&entry_id[38..44])?;
    let message_counter = crate::mapi::identity::global_counter_from_globcnt(&entry_id[62..68])?;
    Some((
        crate::mapi::identity::mapi_store_id(folder_counter),
        crate::mapi::identity::mapi_store_id(message_counter),
    ))
}

pub(in crate::mapi) fn log_common_view_descriptor_getprops_summary(
    principal: &AccountPrincipal,
    request: &RopRequest,
    object: Option<&MapiObject>,
    columns: &[u32],
    snapshot: &MapiMailStoreSnapshot,
) {
    let Some(MapiObject::CommonViewNamedView { folder_id, view_id }) = object else {
        return;
    };
    if !common_view_descriptor_property_requested(columns) {
        return;
    }
    let Some(message) = snapshot
        .common_view_named_view_message_for_id(*view_id)
        .or_else(|| snapshot.default_folder_named_view_message(*folder_id, *view_id))
    else {
        tracing::warn!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x07",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            folder_id = %format!("0x{folder_id:016x}"),
            view_message_id = %format!("0x{view_id:016x}"),
            requested_property_tags = %format_property_tags_for_debug(columns),
            ms_oxcfg_reference = "MS-OXOCFG 2.2.6, 2.2.6.1, 2.2.6.2",
            message = "rca debug outlook view descriptor getprops missing view message",
        );
        return;
    };

    let definition = outlook_folder_view_definition(message.folder_id, &message.name);
    let descriptor = view_descriptor_binary(&definition);
    let descriptor_strings = view_descriptor_strings(&definition);
    let descriptor_string_bytes = utf16le_bytes(&descriptor_strings);
    let descriptor_columns = view_descriptor_debug_property_tags(&descriptor);
    let requested_required = format_requested_view_descriptor_contract(columns);
    let response_values =
        format_common_view_descriptor_response_values(principal.account_id, &message, columns);
    let descriptor_strings_terminators = descriptor_strings
        .chars()
        .filter(|value| *value == '\n')
        .count();

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x07",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.response_handle_index(),
        folder_id = %format!("0x{folder_id:016x}"),
        view_message_id = %format!("0x{view_id:016x}"),
        view_name = %message.name,
        view_message_class = "IPM.Microsoft.FolderDesign.NamedView",
        requested_property_tags = %format_property_tags_for_debug(columns),
        requested_view_descriptor_contract = %requested_required,
        requested_view_descriptor_response_values = %response_values,
        ms_oxcfg_reference = "MS-OXOCFG 2.2.6, 2.2.6.1, 2.2.6.2",
        descriptor_version = 8u32,
        descriptor_name_present = !message.name.is_empty(),
        descriptor_binary_bytes = descriptor.len(),
        descriptor_binary_sha256_16 = %sha256_hex_prefix(&descriptor, 16),
        descriptor_binary_preview = %hex_preview_for_debug(&descriptor, 96),
        descriptor_column_count = descriptor_columns.len(),
        descriptor_column_tags = %format_property_tags_for_debug(&descriptor_columns),
        descriptor_strings_utf16_bytes = descriptor_string_bytes.len(),
        descriptor_strings_sha256_16 = %sha256_hex_prefix(&descriptor_string_bytes, 16),
        descriptor_strings_utf16_preview = %hex_preview_for_debug(&descriptor_string_bytes, 96),
        descriptor_strings_terminators,
        descriptor_strings_starts_with_terminator = descriptor_strings.starts_with('\n'),
        descriptor_strings_ends_with_terminator = descriptor_strings.ends_with('\n'),
        message = "rca debug outlook view descriptor getprops",
    );
}

pub(in crate::mapi) fn format_common_view_descriptor_getprops_contract(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    columns: &[u32],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    let Some(MapiObject::CommonViewNamedView { folder_id, view_id }) = object else {
        return String::new();
    };
    let descriptor_requested = columns.iter().any(|tag| {
        matches!(
            canonical_property_storage_tag(*tag),
            PID_TAG_VIEW_DESCRIPTOR_BINARY
                | OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835
                | OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C
                | PID_TAG_VIEW_DESCRIPTOR_STRINGS_W
                | OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
        )
    });
    if !descriptor_requested {
        return String::new();
    }
    let message = snapshot
        .common_view_named_view_message_for_id(*view_id)
        .or_else(|| snapshot.default_folder_named_view_message(*folder_id, *view_id));
    let Some(message) = message else {
        return format!(
            "found=false;folder_id=0x{folder_id:016x};view_id=0x{view_id:016x};requested_descriptor_tags={}",
            format_property_tags_for_debug(columns)
        );
    };
    let definition = outlook_folder_view_definition(message.folder_id, &message.name);
    let descriptor = view_descriptor_binary(&definition);
    let descriptor_columns = view_descriptor_debug_property_tags(&descriptor);
    let descriptor_strings = view_descriptor_strings(&definition);
    let descriptor_string_bytes = utf16le_bytes(&descriptor_strings);
    let descriptor_strings_terminators = descriptor_strings
        .chars()
        .filter(|value| *value == '\n')
        .count();
    let response_values =
        format_common_view_descriptor_response_values(principal.account_id, &message, columns);
    let target = default_view_message_entry_id_target(
        &crate::mapi::identity::message_entry_id_from_object_ids(
            principal.account_id,
            *folder_id,
            *view_id,
        )
        .unwrap_or_default(),
    )
    .map(|(target_folder_id, target_message_id)| {
        format!("folder_id=0x{target_folder_id:016x};message_id=0x{target_message_id:016x}")
    })
    .unwrap_or_else(|| "decode=not_message_entry_id".to_string());

    format!(
        "found=true;folder_id=0x{folder_id:016x};view_id=0x{view_id:016x};view_name={};\
         requested_descriptor_tags={};descriptor_bytes={};descriptor_strings_utf16_bytes={};\
         descriptor_column_count={};descriptor_column_tags={};descriptor_strings_terminators={};\
         descriptor_strings_starts_with_terminator={};descriptor_strings_ends_with_terminator={};\
         descriptor_strings_trailing_nul={};response_values={};\
         descriptor_entry_id_target={target}",
        message.name,
        format_property_tags_for_debug(columns),
        descriptor.len(),
        descriptor_string_bytes.len(),
        descriptor_columns.len(),
        format_property_tags_for_debug(&descriptor_columns),
        descriptor_strings_terminators,
        descriptor_strings.starts_with('\n'),
        descriptor_strings.ends_with('\n'),
        descriptor_string_bytes.ends_with(&[0x00, 0x00]),
        response_values
    )
}

fn format_common_view_descriptor_response_values(
    account_id: uuid::Uuid,
    message: &crate::mapi_store::MapiCommonViewNamedViewMessage,
    columns: &[u32],
) -> String {
    columns
        .iter()
        .filter_map(|tag| {
            if !common_view_descriptor_property_requested(&[*tag]) {
                return None;
            }
            let storage_tag = canonical_property_storage_tag(*tag);
            let value = common_view_named_view_property_value(message, account_id, storage_tag)?;
            Some(format!(
                "{tag:#010x}:{}:{}",
                property_tag_debug_name(storage_tag),
                view_descriptor_value_shape_for_debug(&value)
            ))
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub(in crate::mapi) fn format_default_view_entry_id_decoding(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    columns: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    columns
        .iter()
        .filter(|tag| canonical_property_storage_tag(**tag) == PID_TAG_DEFAULT_VIEW_ENTRY_ID)
        .map(|tag| {
            let encoded =
                serialize_object_property(object, principal, mailboxes, emails, snapshot, *tag);
            let mut cursor = Cursor::new(&encoded);
            match parse_mapi_property_value(&mut cursor, *tag) {
                Ok(MapiValue::Binary(entry_id)) => {
                    match default_view_message_entry_id_target(&entry_id) {
                        Some((folder_id, message_id)) => format!(
                            "{tag:#010x}:bytes={}:folder_id={folder_id:#018x};message_id={message_id:#018x}",
                            entry_id.len()
                        ),
                        None => format!(
                            "{tag:#010x}:bytes={}:decode=not_message_entry_id",
                            entry_id.len()
                        ),
                    }
                }
                Ok(value) => format!("{tag:#010x}:unexpected_value={value:?}"),
                Err(error) => format!("{tag:#010x}:decode_error={error}"),
            }
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub(in crate::mapi) fn format_message_body_getprops_contract(
    object: Option<&MapiObject>,
    columns: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    if !columns.iter().any(|tag| is_message_body_debug_tag(*tag)) {
        return String::new();
    }
    let Some(MapiObject::Message {
        folder_id,
        message_id,
        saved_email,
        ..
    }) = object
    else {
        return String::new();
    };

    let (source, email) =
        if let Some(email) = message_for_id(*folder_id, *message_id, mailboxes, emails) {
            ("mailbox", Some(email))
        } else {
            (
                "search_folder",
                search_folder_message_for_id(snapshot, *folder_id, *message_id)
                    .map(|message| &message.email),
            )
        };
    let saved_email = saved_email.as_ref().map(|saved| &saved.email);
    let (source, email) = match email.or(saved_email) {
        Some(email) if saved_email.is_some_and(|saved| saved.id == email.id) => {
            ("saved_handle", Some(email))
        }
        Some(email) => (source, Some(email)),
        None => (source, None),
    };
    let Some(email) = email else {
        return format!(
            "message_found=false;folder_id={folder_id:#018x};message_id={message_id:#018x};requested_body_tags={}",
            format_property_tags_for_debug(
                &columns
                    .iter()
                    .copied()
                    .filter(|tag| is_message_body_debug_tag(*tag))
                    .collect::<Vec<_>>()
            )
        );
    };

    let body_text_chars = email.body_text.chars().count();
    let body_html_bytes = email
        .body_html_sanitized
        .as_deref()
        .map(str::len)
        .unwrap_or_default();
    format!(
        "message_found=true;source={source};folder_id={folder_id:#018x};message_id={message_id:#018x};subject_chars={};body_text_chars={body_text_chars};body_text_empty={};body_html_bytes={body_html_bytes};body_html_empty={};native_body={};has_attachments={};size_octets={};requested_body_tags={}",
        email.subject.chars().count(),
        email.body_text.trim().is_empty(),
        email.body_html_sanitized
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .is_empty(),
        native_body_format(email),
        email.has_attachments,
        email.size_octets,
        format_property_tags_for_debug(
            &columns
                .iter()
                .copied()
                .filter(|tag| is_message_body_debug_tag(*tag))
                .collect::<Vec<_>>()
        )
    )
}

fn is_message_body_debug_tag(tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(tag),
        PID_TAG_BODY_STRING8
            | PID_TAG_BODY_W
            | PID_TAG_RTF_COMPRESSED
            | PID_TAG_BODY_HTML_W
            | PID_TAG_HTML_BINARY
            | PID_TAG_NATIVE_BODY
            | PID_TAG_RTF_IN_SYNC
    )
}

pub(in crate::mapi) fn format_folder_type_getprops_contract(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    columns: &[u32],
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    if !columns
        .iter()
        .any(|tag| canonical_property_storage_tag(*tag) == PID_TAG_FOLDER_TYPE)
    {
        return String::new();
    }
    let Some(MapiObject::Folder {
        folder_id,
        properties,
    }) = object
    else {
        return String::new();
    };

    let mailbox = folder_row_for_id(*folder_id, mailboxes);
    let collaboration_folder = snapshot.collaboration_folder_for_id(*folder_id);
    let public_folder = snapshot.public_folder_for_id(*folder_id);
    let search_folder_found = snapshot
        .search_folder_definition_for_folder_id(*folder_id)
        .is_some();
    let advertised_special_folder = is_advertised_special_folder(*folder_id);

    let handle_value = properties
        .get(&PID_TAG_FOLDER_TYPE)
        .cloned()
        .and_then(MapiValue::into_u32);
    let (property_source, returned_value) = if search_folder_found {
        ("search_folder_definition", Some(FOLDER_SEARCH))
    } else if handle_value.is_some() {
        ("opened_handle", handle_value)
    } else if let Some(mailbox) = mailbox {
        (
            "mailbox",
            mailbox_property_value_with_context_for_account(
                mailbox,
                mailboxes,
                PID_TAG_FOLDER_TYPE,
                principal.account_id,
            )
            .and_then(MapiValue::into_u32),
        )
    } else if let Some(folder) = collaboration_folder {
        (
            "collaboration_folder",
            collaboration_folder_property_value(folder, PID_TAG_FOLDER_TYPE)
                .and_then(MapiValue::into_u32),
        )
    } else if let Some(folder) = public_folder {
        (
            "public_folder",
            public_folder_property_value(folder, PID_TAG_FOLDER_TYPE).and_then(MapiValue::into_u32),
        )
    } else {
        (
            "special_folder_fallback",
            special_folder_property_value(*folder_id, PID_TAG_FOLDER_TYPE, principal.account_id)
                .and_then(MapiValue::into_u32),
        )
    };
    let (expected_kind, expected_value) =
        expected_folder_type_for_debug(*folder_id, mailbox, search_folder_found);

    let mut issues = Vec::new();
    if returned_value.is_none() {
        issues.push("missing_folder_type");
    }
    if returned_value
        .map(|value| !matches!(value, FOLDER_ROOT | FOLDER_GENERIC | FOLDER_SEARCH))
        .unwrap_or(false)
    {
        issues.push("invalid_folder_type_value");
    }
    if let (Some(returned), Some(expected)) = (returned_value, expected_value) {
        if returned != expected {
            issues.push("folder_type_mismatch");
        }
    }
    if *folder_id == INBOX_FOLDER_ID && mailbox.is_none() {
        issues.push("inbox_without_loaded_mailbox");
    }
    if *folder_id == INBOX_FOLDER_ID && property_source == "special_folder_fallback" {
        issues.push("inbox_answered_from_special_fallback");
    }
    if property_source == "special_folder_fallback" && !advertised_special_folder {
        issues.push("non_advertised_special_fallback");
    }

    format!(
        "folder_id=0x{folder_id:016x};mailbox_folder_found={};collaboration_folder_found={};public_folder_found={};search_folder_definition_found={};advertised_special_folder={};property_source={property_source};returned_value={};returned_kind={};expected_value={};expected_kind={expected_kind};issues={}",
        mailbox.is_some(),
        collaboration_folder.is_some(),
        public_folder.is_some(),
        search_folder_found,
        advertised_special_folder,
        returned_value
            .map(|value| value.to_string())
            .unwrap_or_else(|| "missing".to_string()),
        returned_value
            .map(folder_type_kind_for_debug)
            .unwrap_or("missing"),
        expected_value
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        issues.join("|")
    )
}

pub(in crate::mapi) fn format_ipm_configuration_getprops_contract(
    object: Option<&MapiObject>,
    columns: &[u32],
    snapshot: &MapiMailStoreSnapshot,
    fallback_tags: &[u32],
) -> String {
    let Some(MapiObject::AssociatedConfig {
        folder_id,
        config_id,
        saved_message,
    }) = object
    else {
        return String::new();
    };
    let Some(message) = snapshot
        .associated_config_message_for_id(*config_id)
        .or_else(|| saved_message.clone())
        .filter(|message| message.folder_id == *folder_id)
    else {
        return format!("found=false;folder_id=0x{folder_id:016x};config_id=0x{config_id:016x}");
    };
    if !crate::mapi_store::is_outlook_configuration_message_class(&message.message_class) {
        return String::new();
    }
    let datatypes = associated_config_property_value(&message, PID_TAG_ROAMING_DATATYPES)
        .and_then(|value| value.into_u32());
    let requested_stream_tags = columns
        .iter()
        .copied()
        .filter(|tag| {
            matches!(
                *tag,
                PID_TAG_ROAMING_DICTIONARY | PID_TAG_ROAMING_XML_STREAM
            )
        })
        .collect::<Vec<_>>();
    let missing_requested_streams = requested_stream_tags
        .iter()
        .copied()
        .filter(|tag| associated_config_property_value(&message, *tag).is_none())
        .collect::<Vec<_>>();
    let undocumented_0e0b = format_associated_config_0e0b_debug(columns, &message, fallback_tags);
    format!(
        "found=true;folder_id=0x{folder_id:016x};config_id=0x{config_id:016x};class={};datatypes={};has_dictionary={};has_xml={};requested_streams={};missing_requested_streams={};fallback_tags={};undocumented_0e0b={}",
        message.message_class,
        datatypes
            .map(|value| format!("0x{value:08x}"))
            .unwrap_or_else(|| "missing".to_string()),
        associated_config_property_value(&message, PID_TAG_ROAMING_DICTIONARY).is_some(),
        associated_config_property_value(&message, PID_TAG_ROAMING_XML_STREAM).is_some(),
        format_property_tags_for_debug(&requested_stream_tags),
        format_property_tags_for_debug(&missing_requested_streams),
        format_property_tags_for_debug(fallback_tags),
        undocumented_0e0b
    )
}
