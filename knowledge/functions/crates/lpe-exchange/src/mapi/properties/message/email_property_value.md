---
type: Rust Function
title: email_property_value
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L3-L206
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/message/rss_email_named_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_folder_id_for_email
  - functions/crates/lpe-exchange/src/mapi/properties/message/message_class_for_email
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_client_submit_time_filetime
  - functions/crates/lpe-exchange/src/mapi/tables/flags/message_flags
  - functions/crates/lpe-exchange/src/mapi_mailstore/canonical_flag_status
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_percent_complete
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_name
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_address
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sent_representing_name
  - functions/crates/lpe-exchange/src/mapi/properties/folder/sent_representing_entry_id
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sent_representing_address
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/display_to
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/display_cc
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/display_bcc
  - functions/crates/lpe-exchange/src/mapi/properties/message/uncompressed_rtf_body
  - functions/crates/lpe-exchange/src/mapi/properties/message/html_body_from_plain_text
  - functions/crates/lpe-exchange/src/mapi/properties/message/native_body_format
  - functions/crates/lpe-exchange/src/mapi/properties/message/conversation_index_for_uuid
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_mailbox_role
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number
  - functions/crates/lpe-exchange/src/mapi/properties/message/transport_headers
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value_with_durable_identity
  - functions/crates/lpe-exchange/src/mapi/properties/tests/rss_feed_messages_project_rss_message_class_and_named_properties
  - functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property
  - functions/crates/lpe-exchange/src/mapi/tables/contents/category_values_for_email
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/inbox_contents_row_invariant_property_value
---

# Signature

`pub(in crate::mapi) fn email_property_value( email: &JmapEmail, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [rss_email_named_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/rss_email_named_property_value.md)
- [mapi_folder_id_for_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_folder_id_for_email.md)
- [message_class_for_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/message_class_for_email.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [email_client_submit_time_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_client_submit_time_filetime.md)
- [message_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/flags/message_flags.md)
- [canonical_flag_status](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_flag_status.md)
- [email_percent_complete](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_percent_complete.md)
- [mapi_message_size_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value.md)
- [mapi_message_size_extended_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value.md)
- [email_sender_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_name.md)
- [email_sender_address](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_address.md)
- [email_sent_representing_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sent_representing_name.md)
- [sent_representing_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/sent_representing_entry_id.md)
- [email_sent_representing_address](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sent_representing_address.md)
- [display_to](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/display_to.md)
- [display_cc](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/display_cc.md)
- [display_bcc](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/display_bcc.md)
- [uncompressed_rtf_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/uncompressed_rtf_body.md)
- [html_body_from_plain_text](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/html_body_from_plain_text.md)
- [native_body_format](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/native_body_format.md)
- [conversation_index_for_uuid](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/conversation_index_for_uuid.md)
- [source_key_for_uuid](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid.md)
- [source_key_for_mailbox_role](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_mailbox_role.md)
- [canonical_message_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number.md)
- [transport_headers](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/transport_headers.md)

# Called by

- [normal_message_debug_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value.md)
- [restriction_matches_email_with_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments.md)
- [email_property_value_with_durable_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value_with_durable_identity.md)
- [rss_feed_messages_project_rss_message_class_and_named_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/rss_feed_messages_project_rss_message_class_and_named_properties.md)
- [fallback_default_specific_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property.md)
- [category_values_for_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/category_values_for_email.md)
- [inbox_contents_row_invariant_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/inbox_contents_row_invariant_property_value.md)