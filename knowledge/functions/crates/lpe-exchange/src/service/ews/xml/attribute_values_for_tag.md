---
type: Rust Function
title: attribute_values_for_tag
resource: crates/lpe-exchange/src/service/ews/xml.rs#L126-L158
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/update_folder
  - functions/crates/lpe-exchange/src/service/ews/conversations/requested_conversation_ids
  - functions/crates/lpe-exchange/src/service/ews/conversations/parse_conversation_actions
  - functions/crates/lpe-exchange/src/service/ews/conversations/filter_ignored_conversation_folders
  - functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_payload_debug_detail
  - functions/crates/lpe-exchange/src/service/ews/directory/requested_persona_id
  - functions/crates/lpe-exchange/src/service/ews/fields/field_block_matches
  - functions/crates/lpe-exchange/src/service/ews/message_tracking/requested_message_tracking_report_id
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_attachment_ids
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_folder_ids
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id_in
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_distinguished_folder_id
  - functions/crates/lpe-exchange/src/service/ews/ucs/get_im_items_response
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_group_id
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_member_value
---

# Signature

`pub(in crate::service) fn attribute_values_for_tag<'a>( xml: &'a str, local_name: &str, attr: &str, ) -> Vec<&'a str>`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [attribute_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [update_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/update_folder.md)
- [requested_conversation_ids](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/requested_conversation_ids.md)
- [parse_conversation_actions](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/parse_conversation_actions.md)
- [filter_ignored_conversation_folders](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/filter_ignored_conversation_folders.md)
- [ews_payload_debug_detail](../../../../../../../functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_payload_debug_detail.md)
- [requested_persona_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/requested_persona_id.md)
- [field_block_matches](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/field_block_matches.md)
- [requested_message_tracking_report_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/message_tracking/requested_message_tracking_report_id.md)
- [requested_attachment_ids](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_attachment_ids.md)
- [requested_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_folder_ids.md)
- [requested_collection_id_in](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id_in.md)
- [requested_public_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids.md)
- [requested_distinguished_folder_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_distinguished_folder_id.md)
- [get_im_items_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/get_im_items_response.md)
- [requested_im_group_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_group_id.md)
- [requested_im_member_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_member_value.md)