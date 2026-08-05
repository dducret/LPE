---
type: Rust Function
title: normal_message_debug_property_value
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/message.rs#L221-L304
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/message/message_class_for_email
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_client_submit_time_filetime
  - functions/crates/lpe-exchange/src/mapi/tables/flags/message_flags
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_name
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_address
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sent_representing_name
  - functions/crates/lpe-exchange/src/mapi/properties/folder/sent_representing_entry_id
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_sent_representing_address
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/display_to
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/display_cc
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/display_bcc
  - functions/crates/lpe-exchange/src/mapi/properties/message/uncompressed_rtf_body
  - functions/crates/lpe-exchange/src/mapi/properties/message/native_body_format
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/copy_message_followup_property_values_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/copy_all_message_followup_property_values_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/format_visible_inbox_first_row_projection_audit
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_find_row_failure_candidates
---

# Signature

`pub(in crate::mapi::dispatch) fn normal_message_debug_property_value( email: &JmapEmail, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [message_class_for_email](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/message_class_for_email.md)
- [filetime_from_rfc3339_utc](../../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [email_client_submit_time_filetime](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_client_submit_time_filetime.md)
- [message_flags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/flags/message_flags.md)
- [email_sender_name](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_name.md)
- [email_sender_address](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sender_address.md)
- [email_sent_representing_name](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sent_representing_name.md)
- [sent_representing_entry_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/sent_representing_entry_id.md)
- [email_sent_representing_address](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_sent_representing_address.md)
- [display_to](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/display_to.md)
- [display_cc](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/display_cc.md)
- [display_bcc](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/display_bcc.md)
- [uncompressed_rtf_body](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/uncompressed_rtf_body.md)
- [native_body_format](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/native_body_format.md)
- [email_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)

# Called by

- [format_inbox_view_descriptor_behavior_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract.md)
- [copy_message_followup_property_values_for_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/copy_message_followup_property_values_for_request.md)
- [copy_all_message_followup_property_values_for_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/copy_all_message_followup_property_values_for_request.md)
- [format_visible_inbox_first_row_projection_audit](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/format_visible_inbox_first_row_projection_audit.md)
- [format_normal_message_query_row_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary.md)
- [format_normal_message_find_row_failure_candidates](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_find_row_failure_candidates.md)