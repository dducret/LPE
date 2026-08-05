---
type: Rust Module
title: properties
resource: crates/lpe-exchange/src/mapi/properties.rs#L1-L1431
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-rop
  - external/super-session
  - external/super-sync
  - external/super-tables
  - external/super-wire-mapipropertytype
  - external/super
  - external/crate-mapi-identity-recoverable-items-deletions-folder-id-recoverable-items-purges-folder-id-recoverable-items-root-folder-id-recoverable-items-versions-folder-id
  - external/crate-mapi-store-mapiassociatedconfigmessage-mapiattachment-mapicommonviewnamedviewmessage-mapiconversationactionmessage-mapievent-mapimessage-mapinavigationshortcutmessage-mapipublicfolder
  - external/anyhow-bail
  - external/base64-engine-general-purpose-standard-as-base64-standard-engine-as
  - external/lpe-domain-civil-from-days-days-from-civil
  - external/lpe-storage-calendar-attendee-labels-normalize-calendar-email-parse-calendar-participants-metadata-serialize-calendar-participants-metadata-calendarorganizermetadata-calendarparticipantmetadata-searchfolderdefinition
  - external/pub-in-crate-mapi-use-attachments
  - external/pub-in-crate-mapi-use-calendar
  - external/pub-in-crate-mapi-use-contact
  - external/pub-crate-use-folder
  - external/pub-crate-use-message-message-class-for-email
  - external/pub-in-crate-mapi-use-message
  - external/pub-crate-use-named
  - external/pub-in-crate-mapi-use-navigation-shortcut
  - external/pub-in-crate-mapi-use-notes
  - external/recurrence
  - external/pub-in-crate-mapi-use-reminders
  - external/pub-in-crate-mapi-use-restrictions
  - external/pub-in-crate-mapi-use-search-folders
  - external/streams-property-stream-data
  - external/pub-in-crate-mapi-use-streams
  - external/streams-pending-body-text-property-property-tag-type
  - external/pub-crate-use-tags
  - external/pub-in-crate-mapi-use-task
  - external/pub-super-use-values
  - external/pub-in-crate-mapi-use-views
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [rop_read_recipients_response](../../../../../functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response.md)
- [rop_set_message_read_flag_response](../../../../../functions/crates/lpe-exchange/src/mapi/properties/rop_set_message_read_flag_response.md)
- [search_folder_message_for_id](../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id.md)
- [restriction_matches_mailbox_with_context_for_account](../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_mailbox_with_context_for_account.md)
- [restriction_matches_collaboration_folder](../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_collaboration_folder.md)
- [restriction_matches_public_folder](../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_public_folder.md)
- [restriction_matches_email](../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email.md)
- [restriction_matches_email_with_attachments](../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments.md)
- [recipient_property_value](../../../../../functions/crates/lpe-exchange/src/mapi/properties/recipient_property_value.md)
- [restriction_matches_contact_in_folder](../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_contact_in_folder.md)
- [restriction_matches_task](../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_task.md)
- [restriction_matches_note](../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_note.md)
- [restriction_matches_journal_entry](../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_journal_entry.md)
- [restriction_matches_attachment](../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_attachment.md)
- [restriction_matches_navigation_shortcut](../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_navigation_shortcut.md)
- [restriction_matches_common_view_named_view](../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_common_view_named_view.md)
- [restriction_matches_associated_config](../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_associated_config.md)
- [restriction_matches](../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [content_restriction_matches](../../../../../functions/crates/lpe-exchange/src/mapi/properties/content_restriction_matches.md)
- [mailbox_property_value_with_context](../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context.md)
- [mailbox_property_value_with_context_for_account](../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)
- [folder_version_property_value](../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder_version_property_value.md)
- [mapi_mailbox_display_name](../../../../../functions/crates/lpe-exchange/src/mapi/properties/mapi_mailbox_display_name.md)
- [default_post_message_class_for_container_class](../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_post_message_class_for_container_class.md)
- [extended_folder_flags](../../../../../functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags.md)
- [extended_folder_flags_for_folder](../../../../../functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags_for_folder.md)
- [extended_folder_flags_for_search_folder](../../../../../functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags_for_search_folder.md)
- [search_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folder_id.md)
- [mailbox_has_subfolders](../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_has_subfolders.md)
- [mailbox_parent_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_parent_folder_id.md)
- [collaboration_folder_property_value](../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value.md)
- [public_folder_property_value](../../../../../functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value.md)
- [common_view_named_view_property_value](../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)
- [property_tag_id](../../../../../functions/crates/lpe-exchange/src/mapi/properties/property_tag_id.md)
- [named_property_id_matches](../../../../../functions/crates/lpe-exchange/src/mapi/properties/named_property_id_matches.md)
- [wlink_guid_property_value](../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_guid_property_value.md)
- [default_wlink_group_guid](../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_wlink_group_guid.md)
- [default_wlink_group_uuid](../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_wlink_group_uuid.md)
- [wlink_group_name](../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_group_name.md)
- [wlink_save_stamp](../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_save_stamp.md)
- [wlink_mail_folder_type_guid](../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_mail_folder_type_guid.md)
- [wlink_contact_folder_type_guid](../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_contact_folder_type_guid.md)
- [wlink_task_folder_type_guid](../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_task_folder_type_guid.md)
- [wlink_note_folder_type_guid](../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_note_folder_type_guid.md)
- [wlink_journal_folder_type_guid](../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_journal_folder_type_guid.md)
- [common_view_named_view_folder_type_guid](../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_folder_type_guid.md)
- [wlink_folder_type_guid](../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_folder_type_guid.md)
- [wlink_ordinal_bytes](../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_ordinal_bytes.md)
- [conversation_action_property_value](../../../../../functions/crates/lpe-exchange/src/mapi/properties/conversation_action_property_value.md)
- [compare_mapi_values](../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values.md)
- [compare_folder_entry_id_values](../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_folder_entry_id_values.md)
- [compare_i64](../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_i64.md)
- [compare_ordering](../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_ordering.md)
- [default_mapping_rights](../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_mapping_rights.md)
- [apply_mapi_property_values](../../../../../functions/crates/lpe-exchange/src/mapi/properties/apply_mapi_property_values.md)
- [apply_pending_associated_message_property_values](../../../../../functions/crates/lpe-exchange/src/mapi/properties/apply_pending_associated_message_property_values.md)
- [delete_mapi_properties](../../../../../functions/crates/lpe-exchange/src/mapi/properties/delete_mapi_properties.md)

# Imports

- `super::rop::*`
- `super::session::*`
- `super::sync::*`
- `super::tables::*`
- `super::wire::MapiPropertyType`
- `super::*`
- `crate::mapi::identity::{
    RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID, RECOVERABLE_ITEMS_PURGES_FOLDER_ID,
    RECOVERABLE_ITEMS_ROOT_FOLDER_ID, RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID,
}`
- `crate::mapi_store::{
    MapiAssociatedConfigMessage, MapiAttachment, MapiCommonViewNamedViewMessage,
    MapiConversationActionMessage, MapiEvent, MapiMessage, MapiNavigationShortcutMessage,
    MapiPublicFolder,
}`
- `anyhow::bail`
- `base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _}`
- `lpe_domain::{civil_from_days, days_from_civil}`
- `lpe_storage::{
    calendar_attendee_labels, normalize_calendar_email, parse_calendar_participants_metadata,
    serialize_calendar_participants_metadata, CalendarOrganizerMetadata,
    CalendarParticipantMetadata, SearchFolderDefinition,
}`
- `pub(in crate::mapi) use attachments::*`
- `pub(in crate::mapi) use calendar::*`
- `pub(in crate::mapi) use contact::*`
- `pub(crate) use folder::*`
- `pub(crate) use message::message_class_for_email`
- `pub(in crate::mapi) use message::*`
- `pub(crate) use named::*`
- `pub(in crate::mapi) use navigation_shortcut::*`
- `pub(in crate::mapi) use notes::*`
- `recurrence::*`
- `pub(in crate::mapi) use reminders::*`
- `pub(in crate::mapi) use restrictions::*`
- `pub(in crate::mapi) use search_folders::*`
- `streams::property_stream_data`
- `pub(in crate::mapi) use streams::*`
- `streams::{pending_body_text_property, property_tag_type}`
- `pub(crate) use tags::*`
- `pub(in crate::mapi) use task::*`
- `pub(super) use values::*`
- `pub(in crate::mapi) use views::*`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)