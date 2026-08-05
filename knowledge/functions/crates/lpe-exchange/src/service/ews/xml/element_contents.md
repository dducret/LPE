---
type: Rust Function
title: element_contents
resource: crates/lpe-exchange/src/service/ews/xml.rs#L233-L272
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/create_attachment
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_event_participants
  - functions/crates/lpe-exchange/src/service/ews/conversations/parse_conversation_actions
  - functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_users
  - functions/crates/lpe-exchange/src/service/ews/delegation/parse_delegate_user_id_emails
  - functions/crates/lpe-exchange/src/service/ews/fields/field_deleted
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/create_managed_folder
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/requested_mail_app_token_scopes
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/requested_mail_tips_recipients
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/requested_service_configurations
  - functions/crates/lpe-exchange/src/service/ews/mailboxes/parse_recipients
  - functions/crates/lpe-exchange/src/service/ews/mailboxes/parse_first_mailbox
  - functions/crates/lpe-exchange/src/service/ews/mailboxes/requested_mailbox_emails
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_transfer_item_ids
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_folder_path_segments
  - functions/crates/lpe-exchange/src/service/ews/rules/ExchangeService/update_inbox_rules
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/requested_user_configuration_properties
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_dictionary
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
---

# Signature

`pub(in crate::service) fn element_contents<'a>(xml: &'a str, local_name: &str) -> Vec<&'a str>`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [create_attachment](../../../../../../../functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/create_attachment.md)
- [parse_event_participants](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_event_participants.md)
- [parse_conversation_actions](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/parse_conversation_actions.md)
- [parse_ews_delegate_users](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_users.md)
- [parse_delegate_user_id_emails](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/parse_delegate_user_id_emails.md)
- [field_deleted](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/field_deleted.md)
- [create_managed_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/create_managed_folder.md)
- [requested_mail_app_token_scopes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/requested_mail_app_token_scopes.md)
- [requested_mail_tips_recipients](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/requested_mail_tips_recipients.md)
- [requested_service_configurations](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/requested_service_configurations.md)
- [parse_recipients](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mailboxes/parse_recipients.md)
- [parse_first_mailbox](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mailboxes/parse_first_mailbox.md)
- [requested_mailbox_emails](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mailboxes/requested_mailbox_emails.md)
- [requested_transfer_item_ids](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_transfer_item_ids.md)
- [requested_folder_path_segments](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_folder_path_segments.md)
- [update_inbox_rules](../../../../../../../functions/crates/lpe-exchange/src/service/ews/rules/ExchangeService/update_inbox_rules.md)
- [requested_user_configuration_properties](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/requested_user_configuration_properties.md)
- [parse_ews_user_configuration_dictionary](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_dictionary.md)
- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)