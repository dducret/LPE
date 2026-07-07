use super::super::*;

pub(in crate::mapi::dispatch) fn format_set_property_names_for_debug(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| set_property_debug_name(*tag))
        .collect::<Vec<_>>()
        .join(",")
}

pub(in crate::mapi::dispatch) fn set_property_debug_name(tag: u32) -> &'static str {
    if is_default_folder_identification_property_tag(tag) {
        let name = default_folder_entry_id_property_name(tag);
        if name != "unknown" {
            return name;
        }
    }
    match canonical_property_storage_tag(tag) {
        PID_TAG_MESSAGE_CLASS_W => "PidTagMessageClass",
        PID_TAG_SUBJECT_W => "PidTagSubject",
        PID_TAG_SUBJECT_PREFIX_W => "PidTagSubjectPrefix",
        PID_TAG_NORMALIZED_SUBJECT_W => "PidTagNormalizedSubject",
        PID_TAG_OBJECT_TYPE => "PidTagObjectType",
        PID_TAG_DISPLAY_TYPE => "PidTagDisplayType",
        PID_TAG_DISPLAY_TYPE_EX => "PidTagDisplayTypeEx",
        PID_TAG_ADDRESS_BOOK_DISPLAY_NAME_PRINTABLE_W => "PidTagAddressBookDisplayNamePrintable",
        PID_TAG_SMTP_ADDRESS_W => "PidTagSmtpAddress",
        PID_TAG_SEND_INTERNET_ENCODING => "PidTagSendInternetEncoding",
        PID_TAG_RECIPIENT_DISPLAY_NAME_W => "PidTagRecipientDisplayName",
        PID_TAG_RECIPIENT_ENTRY_ID => "PidTagRecipientEntryId",
        PID_TAG_RECIPIENT_FLAGS => "PidTagRecipientFlags",
        PID_TAG_RECIPIENT_ORDER => "PidTagRecipientOrder",
        PID_TAG_RECIPIENT_TRACK_STATUS => "PidTagRecipientTrackStatus",
        OUTLOOK_RECIPIENT_5FDE => "OutlookRecipient5FDE",
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
        PID_TAG_PRIVATE => "PidTagPrivate",
        PID_TAG_RESOURCE_FLAGS => "PidTagResourceFlags",
        PID_TAG_SERIALIZED_REPLID_GUID_MAP => "PidTagSerializedReplidGuidMap",
        PID_TAG_USER_ENTRY_ID => "PidTagUserEntryId",
        PID_TAG_MAILBOX_OWNER_ENTRY_ID => "PidTagMailboxOwnerEntryId",
        PID_TAG_MAILBOX_OWNER_NAME_W => "PidTagMailboxOwnerName",
        PID_TAG_IPM_PUBLIC_FOLDERS_ENTRY_ID => "PidTagIpmPublicFoldersEntryId",
        PID_TAG_USER_GUID => "PidTagUserGuid",
        PID_TAG_ASSOCIATED_SHARING_PROVIDER => "PidTagAssociatedSharingProvider",
        PID_TAG_SERVER_TYPE_DISPLAY_NAME_W => "PidTagServerTypeDisplayName",
        PID_TAG_MESSAGE_SIZE_EXTENDED => "PidTagMessageSizeExtended",
        PID_TAG_PROHIBIT_RECEIVE_QUOTA => "PidTagProhibitReceiveQuota",
        PID_TAG_PROHIBIT_SEND_QUOTA => "PidTagProhibitSendQuota",
        PID_TAG_STORAGE_QUOTA_LIMIT => "PidTagStorageQuotaLimit",
        PID_TAG_PST_PATH_W => "PidTagPstPath",
        PID_TAG_CHANGE_KEY => "PidTagChangeKey",
        PID_TAG_LAST_MODIFICATION_TIME => "PidTagLastModificationTime",
        PID_TAG_ROAMING_DATATYPES => "PidTagRoamingDatatypes",
        PID_TAG_ROAMING_DICTIONARY => "PidTagRoamingDictionary",
        PID_TAG_ROAMING_XML_STREAM => "PidTagRoamingXmlStream",
        OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B => "OutlookAssociatedConfigBinary0E0B",
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
        PID_TAG_DISPLAY_NAME_W => "PidTagDisplayName",
        PID_TAG_FOLDER_ID => "PidTagFolderId",
        PID_TAG_PARENT_FOLDER_ID => "PidTagParentFolderId",
        PID_TAG_FOLDER_TYPE => "PidTagFolderType",
        PID_TAG_CONTENT_COUNT => "PidTagContentCount",
        PID_TAG_CONTENT_UNREAD_COUNT => "PidTagContentUnreadCount",
        PID_TAG_SUBFOLDERS => "PidTagSubfolders",
        PID_TAG_CONTAINER_CLASS_W => "PidTagContainerClass",
        PID_TAG_DEFAULT_VIEW_ENTRY_ID => "PidTagDefaultViewEntryId",
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => {
            "PidTagDefaultPostMessageClass"
        }
        PID_TAG_DEFAULT_FORM_NAME_W => "PidTagDefaultFormName",
        PID_TAG_FOLDER_FORM_FLAGS => "PidTagFolderFormFlags",
        PID_TAG_FOLDER_WEBVIEWINFO => "PidTagFolderWebViewInfo",
        PID_TAG_FOLDER_XVIEWINFO_E => "PidTagFolderXViewInfoE",
        PID_TAG_FOLDER_VIEWS_ONLY => "PidTagFolderViewsOnly",
        PID_TAG_FOLDER_FORM_STORAGE => "PidTagFolderFormStorage",
        PID_TAG_EXTENDED_RULE_MESSAGE_ACTIONS => "PidTagExtendedRuleMessageActions",
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS => "PidTagAdditionalRenEntryIds",
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX => "PidTagAdditionalRenEntryIdsEx",
        PID_TAG_FREE_BUSY_ENTRY_IDS => "PidTagFreeBusyEntryIds",
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
        PID_TAG_ACCESS => "PidTagAccess",
        OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C => "OutlookUndocumentedFolderBinary120C",
        tag if is_acl_member_name_property_tag(tag) => "PidTagMemberName",
        tag if property_ids_match(tag, PID_TAG_WLINK_SAVE_STAMP) => "PidTagWlinkSaveStamp",
        tag if property_ids_match(tag, PID_TAG_WLINK_TYPE) => "PidTagWlinkType",
        tag if property_ids_match(tag, PID_TAG_WLINK_FLAGS) => "PidTagWlinkFlags",
        tag if property_ids_match(tag, PID_TAG_WLINK_ORDINAL) => "PidTagWlinkOrdinal",
        tag if property_ids_match(tag, PID_TAG_WLINK_ENTRY_ID) => "PidTagWlinkEntryId",
        tag if property_ids_match(tag, PID_TAG_WLINK_RECORD_KEY) => "PidTagWlinkRecordKey",
        tag if property_ids_match(tag, PID_TAG_WLINK_STORE_ENTRY_ID) => "PidTagWlinkStoreEntryId",
        tag if property_ids_match(tag, PID_TAG_WLINK_FOLDER_TYPE) => "PidTagWlinkFolderType",
        tag if property_ids_match(tag, PID_TAG_WLINK_GROUP_CLSID) => "PidTagWlinkGroupClsid",
        tag if property_ids_match(tag, PID_TAG_WLINK_GROUP_NAME_W) => "PidTagWlinkGroupName",
        tag if property_ids_match(tag, PID_TAG_WLINK_SECTION) => "PidTagWlinkSection",
        tag if property_ids_match(tag, PID_TAG_WLINK_CALENDAR_COLOR) => "PidTagWlinkCalendarColor",
        tag if property_ids_match(tag, PID_TAG_WLINK_ADDRESS_BOOK_EID) => {
            "PidTagWlinkAddressBookEid"
        }
        tag if property_ids_match(tag, PID_TAG_WLINK_CLIENT_ID) => "PidTagWlinkClientId",
        tag if property_ids_match(tag, PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID) => {
            "PidTagWlinkAddressBookStoreEid"
        }
        tag if property_ids_match(tag, PID_TAG_WLINK_RO_GROUP_TYPE) => "PidTagWlinkRoGroupType",
        0x7C09_0102 => "PidTagRoamingBinary",
        0x685D_0003 => "OutlookConfigurationStamp",
        _ => "unknown",
    }
}
