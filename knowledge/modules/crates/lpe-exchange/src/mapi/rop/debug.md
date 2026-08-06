---
type: Rust Module
title: debug
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L1-L1451
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-super-properties
  - external/super-associated-config-property-value-associated-config-property-value-with-mailbox-guid-canonical-property-storage-tag-collaboration-folder-property-value-common-view-named-view-property-value-flagged-property-error-code-folder-row-for-id-is-advertised-special-folder-logon-property-value-mailbox-property-value-with-context-for-account-mapi-properties-from-json-message-for-id-modeled-zero-or-default-property-native-body-format-outlook-folder-view-definition-parse-mapi-property-value-property-is-unsupported-for-object-public-folder-property-value-search-folder-message-for-id-serialize-logon-row-serialize-object-property-special-folder-identification-property-value-special-folder-property-value-unsupported-specific-property-tags-utf16le-bytes-view-descriptor-all-property-tags-view-descriptor-binary-view-descriptor-strings-write-property-default-accountprincipal-cursor-jmapemail-jmapmailbox-mapimailstoresnapshot-mapiobject-mapivalue-roprequest-contacts-search-folder-id-folder-generic-folder-root-folder-search-inbox-folder-id-nspi-permanent-entry-id-provider-uid-outlook-associated-config-binary-0e0b-outlook-common-view-descriptor-binary-6835-outlook-common-view-descriptor-strings-683c-pid-tag-body-html-w-pid-tag-body-string8-pid-tag-body-w-pid-tag-common-views-entry-id-pid-tag-default-view-entry-id-pid-tag-finder-entry-id-pid-tag-folder-type-pid-tag-html-binary-pid-tag-ipm-appointment-entry-id-pid-tag-ipm-archive-entry-id-pid-tag-ipm-contact-entry-id-pid-tag-ipm-drafts-entry-id-pid-tag-ipm-journal-entry-id-pid-tag-ipm-note-entry-id-pid-tag-ipm-outbox-entry-id-pid-tag-ipm-public-folders-entry-id-pid-tag-ipm-sentmail-entry-id-pid-tag-ipm-subtree-entry-id-pid-tag-ipm-task-entry-id-pid-tag-ipm-wastebasket-entry-id-pid-tag-mailbox-owner-entry-id-pid-tag-mailbox-owner-name-w-pid-tag-max-submit-message-size-pid-tag-message-size-extended-pid-tag-native-body-pid-tag-outlook-store-state-pid-tag-private-pid-tag-prohibit-receive-quota-pid-tag-prohibit-send-quota-pid-tag-rem-offline-entry-id-pid-tag-rem-online-entry-id-pid-tag-resource-flags-pid-tag-roaming-datatypes-pid-tag-roaming-dictionary-pid-tag-roaming-xml-stream-pid-tag-rtf-compressed-pid-tag-rtf-in-sync-pid-tag-server-account-icon-pid-tag-server-connected-icon-pid-tag-server-type-display-name-w-pid-tag-storage-quota-limit-pid-tag-user-entry-id-pid-tag-user-guid-pid-tag-views-entry-id-pid-tag-view-descriptor-binary-pid-tag-view-descriptor-name-w-pid-tag-view-descriptor-strings-w-pid-tag-view-descriptor-version-pid-tag-view-descriptor-version-canonical-public-folders-root-folder-id-reminders-folder-id-root-folder-id-search-folder-id-todo-search-folder-id-tracked-mail-processing-folder-id
  - external/lpe-domain-crypto-sha256-hex-prefix
  - external/pub-in-crate-mapi-use-folders
  - external/pub-in-crate-mapi-use-shapes
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [property_row_kind_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/property_row_kind_for_debug.md)
- [format_returned_property_tags_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_returned_property_tags_for_debug.md)
- [format_property_tags_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_property_tags_for_debug.md)
- [format_property_names_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_property_names_for_debug.md)
- [property_tag_debug_name](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/property_tag_debug_name.md)
- [debug_property_id_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/debug_property_id_matches.md)
- [format_property_errors_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_property_errors_for_debug.md)
- [log_get_properties_specific_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)
- [OutlookLogonBootstrapRowShape](../../../../../../classes/crates/lpe-exchange/src/mapi/rop/debug/OutlookLogonBootstrapRowShape.md)
- [outlook_logon_bootstrap_row_shape](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/outlook_logon_bootstrap_row_shape.md)
- [is_outlook_logon_bootstrap_getprops](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/is_outlook_logon_bootstrap_getprops.md)
- [format_outlook_logon_bootstrap_property_details](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_outlook_logon_bootstrap_property_details.md)
- [format_mailbox_owner_entry_id_details](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_mailbox_owner_entry_id_details.md)
- [format_ico_header_details](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_ico_header_details.md)
- [expected_folder_type_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/expected_folder_type_for_debug.md)
- [advertised_special_search_folder_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/advertised_special_search_folder_for_debug.md)
- [folder_type_kind_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/folder_type_kind_for_debug.md)
- [format_property_value_shapes_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_property_value_shapes_for_debug.md)
- [semantic_property_shape_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/semantic_property_shape_for_debug.md)
- [format_associated_config_0e0b_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_associated_config_0e0b_debug.md)
- [common_view_descriptor_property_requested](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/common_view_descriptor_property_requested.md)
- [format_requested_view_descriptor_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_requested_view_descriptor_contract.md)
- [view_descriptor_debug_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/view_descriptor_debug_property_tags.md)
- [default_view_message_entry_id_target](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/default_view_message_entry_id_target.md)
- [log_common_view_descriptor_getprops_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary.md)
- [format_common_view_descriptor_getprops_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract.md)
- [format_common_view_descriptor_response_values](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_response_values.md)
- [format_default_view_entry_id_decoding](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_default_view_entry_id_decoding.md)
- [format_message_body_getprops_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_message_body_getprops_contract.md)
- [is_message_body_debug_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/is_message_body_debug_tag.md)
- [format_folder_type_getprops_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract.md)
- [format_ipm_configuration_getprops_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_ipm_configuration_getprops_contract.md)

# Imports

- `super::super::properties::*`
- `super::{
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
}`
- `lpe_domain::crypto::sha256_hex_prefix`
- `pub(in crate::mapi) use folders::*`
- `pub(in crate::mapi) use shapes::*`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)