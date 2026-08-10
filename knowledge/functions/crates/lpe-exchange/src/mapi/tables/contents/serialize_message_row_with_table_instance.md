---
type: Rust Function
title: serialize_message_row_with_table_instance
resource: crates/lpe-exchange/src/mapi/tables/contents.rs#L258-L380
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/tables/contents/write_category_instance_value
  - functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_folder_id_for_email
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-exchange/src/mapi/properties/message/message_class_for_email
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_client_submit_time_filetime
  - functions/crates/lpe-exchange/src/mapi/tables/flags/message_flags
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_name
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_address
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sent_representing_name
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16_prefixed_bytes
  - functions/crates/lpe-exchange/src/mapi/properties/folder/sent_representing_entry_id
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sent_representing_address
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/display_to
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/display_cc
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/display_bcc
  - functions/crates/lpe-exchange/src/mapi/properties/message/uncompressed_rtf_body
  - functions/crates/lpe-exchange/src/mapi/properties/message/native_body_format
  - functions/crates/lpe-exchange/src/mapi/properties/message/content_class_for_email
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value_with_durable_identity
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_durable_identity
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_mapi_message_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_with_durable_identity_and_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_categorized_message_row
  - functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows
---

# Signature

`fn serialize_message_row_with_table_instance( email: &JmapEmail, durable_identity: Option<&crate::store::MapiIdentityRecord>, mailbox_guid: Option<Uuid>, columns: &[u32], instance_num: u32, depth: u32, category_value: Option<(u32, &str)>, ) -> Vec<u8>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [write_category_instance_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/write_category_instance_value.md)
- [mapi_folder_id_for_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_folder_id_for_email.md)
- [write_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [message_class_for_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/message_class_for_email.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [email_client_submit_time_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_client_submit_time_filetime.md)
- [message_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/flags/message_flags.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [email_sender_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_name.md)
- [email_sender_address](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_address.md)
- [email_sent_representing_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sent_representing_name.md)
- [write_u16_prefixed_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16_prefixed_bytes.md)
- [sent_representing_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/sent_representing_entry_id.md)
- [email_sent_representing_address](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sent_representing_address.md)
- [display_to](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/display_to.md)
- [display_cc](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/display_cc.md)
- [display_bcc](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/display_bcc.md)
- [uncompressed_rtf_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/uncompressed_rtf_body.md)
- [native_body_format](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/native_body_format.md)
- [content_class_for_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/content_class_for_email.md)
- [email_property_value_with_durable_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value_with_durable_identity.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [serialize_message_row_with_durable_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_durable_identity.md)
- [serialize_mapi_message_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_mapi_message_row_with_mailbox_guid.md)
- [serialize_message_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_mailbox_guid.md)
- [serialize_message_property_row_with_durable_identity_and_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_with_durable_identity_and_mailbox_guid.md)
- [serialize_categorized_message_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_categorized_message_row.md)
- [categorized_email_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows.md)