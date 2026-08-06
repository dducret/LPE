---
type: Rust Function
title: element_content
resource: crates/lpe-exchange/src/service/ews/xml.rs#L229-L231
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/create_attachment
  - functions/crates/lpe-exchange/src/service/ews/availability/ExchangeService/get_user_availability
  - functions/crates/lpe-exchange/src/service/ews/availability/availability_suggestions_response
  - functions/crates/lpe-exchange/src/service/ews/availability/requested_availability_window
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_ews_recurrence
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_event_participants
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_attendee
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_create_contact_input
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_entry_value
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_address_entry
  - functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/conversation_source_emails
  - functions/crates/lpe-exchange/src/service/ews/conversations/filter_ignored_conversation_folders
  - functions/crates/lpe-exchange/src/service/ews/delegation/validate_delegate_mailbox_owner
  - functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_user
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item
  - functions/crates/lpe-exchange/src/service/ews/mail/parse_create_message_input
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/requested_mail_tips_recipients
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/requested_mail_tips
  - functions/crates/lpe-exchange/src/service/ews/mailboxes/parse_recipients
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/subscribe
  - functions/crates/lpe-exchange/src/service/ews/oof/ExchangeService/set_user_oof_settings
  - functions/crates/lpe-exchange/src/service/ews/oof/parse_oof_duration
  - functions/crates/lpe-exchange/src/service/ews/public_folders/parse_update_public_folder_item_input
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id_in
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_folder_path_segments
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids_in
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_folder_ids_in
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_role_in
  - functions/crates/lpe-exchange/src/service/ews/rooms/requested_room_list_address
  - functions/crates/lpe-exchange/src/service/ews/rules/ExchangeService/update_inbox_rules
  - functions/crates/lpe-exchange/src/service/ews/rules/bounded_ews_rule_to_sieve
  - functions/crates/lpe-exchange/src/service/ews/sharing/parse_sharing_request
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_create_task_input
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_smtp_address
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_key
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_dictionary
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
---

# Signature

`pub(in crate::service) fn element_content<'a>(xml: &'a str, local_name: &str) -> Option<&'a str>`

# Calls

- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [create_attachment](../../../../../../../functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/create_attachment.md)
- [get_user_availability](../../../../../../../functions/crates/lpe-exchange/src/service/ews/availability/ExchangeService/get_user_availability.md)
- [availability_suggestions_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/availability/availability_suggestions_response.md)
- [requested_availability_window](../../../../../../../functions/crates/lpe-exchange/src/service/ews/availability/requested_availability_window.md)
- [parse_create_event_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input.md)
- [parse_update_event_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input.md)
- [parse_ews_recurrence](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_ews_recurrence.md)
- [parse_event_participants](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_event_participants.md)
- [parse_attendee](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_attendee.md)
- [parse_create_contact_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_create_contact_input.md)
- [parse_update_contact_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input.md)
- [contact_entry_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_entry_value.md)
- [ews_contact_address_entry](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_contact_address_entry.md)
- [conversation_source_emails](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/conversation_source_emails.md)
- [filter_ignored_conversation_folders](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/filter_ignored_conversation_folders.md)
- [validate_delegate_mailbox_owner](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/validate_delegate_mailbox_owner.md)
- [parse_ews_delegate_user](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_user.md)
- [create_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)
- [parse_create_message_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/parse_create_message_input.md)
- [requested_mail_tips_recipients](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/requested_mail_tips_recipients.md)
- [requested_mail_tips](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/requested_mail_tips.md)
- [parse_recipients](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mailboxes/parse_recipients.md)
- [subscribe](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/subscribe.md)
- [set_user_oof_settings](../../../../../../../functions/crates/lpe-exchange/src/service/ews/oof/ExchangeService/set_user_oof_settings.md)
- [parse_oof_duration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/oof/parse_oof_duration.md)
- [parse_update_public_folder_item_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/parse_update_public_folder_item_input.md)
- [requested_collection_id_in](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id_in.md)
- [requested_folder_path_segments](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_folder_path_segments.md)
- [requested_public_folder_ids_in](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids_in.md)
- [requested_mailbox_folder_ids_in](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_folder_ids_in.md)
- [requested_mailbox_role_in](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_role_in.md)
- [requested_room_list_address](../../../../../../../functions/crates/lpe-exchange/src/service/ews/rooms/requested_room_list_address.md)
- [update_inbox_rules](../../../../../../../functions/crates/lpe-exchange/src/service/ews/rules/ExchangeService/update_inbox_rules.md)
- [bounded_ews_rule_to_sieve](../../../../../../../functions/crates/lpe-exchange/src/service/ews/rules/bounded_ews_rule_to_sieve.md)
- [parse_sharing_request](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/parse_sharing_request.md)
- [parse_create_task_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_create_task_input.md)
- [parse_update_task_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input.md)
- [requested_smtp_address](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_smtp_address.md)
- [parse_ews_user_configuration_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_key.md)
- [parse_ews_user_configuration_dictionary](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_dictionary.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)