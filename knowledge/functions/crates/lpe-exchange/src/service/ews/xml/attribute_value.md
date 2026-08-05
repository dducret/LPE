---
type: Rust Function
title: attribute_value
resource: crates/lpe-exchange/src/service/ews/xml.rs#L192-L202
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input
  - functions/crates/lpe-exchange/src/service/ews/calendar/requested_time_zone
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_create_contact_input
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_entry_value
  - functions/crates/lpe-exchange/src/service/ews/ids/convert_id_sources_for_tag
  - functions/crates/lpe-exchange/src/service/ews/mail/parse_create_message_input
  - functions/crates/lpe-exchange/src/service/ews/notifications/pull_subscription_subscribes_to_all_folders
  - functions/crates/lpe-exchange/src/service/ews/public_folders/parse_update_public_folder_item_input
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_create_task_input
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_key
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after
---

# Signature

`pub(in crate::service) fn attribute_value<'a>(tag_text: &'a str, attr: &str) -> Option<&'a str>`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [parse_create_event_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input.md)
- [parse_update_event_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input.md)
- [requested_time_zone](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/requested_time_zone.md)
- [parse_create_contact_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_create_contact_input.md)
- [parse_update_contact_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input.md)
- [contact_entry_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_entry_value.md)
- [convert_id_sources_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/convert_id_sources_for_tag.md)
- [parse_create_message_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/parse_create_message_input.md)
- [pull_subscription_subscribes_to_all_folders](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/pull_subscription_subscribes_to_all_folders.md)
- [parse_update_public_folder_item_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/parse_update_public_folder_item_input.md)
- [parse_create_task_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_create_task_input.md)
- [parse_update_task_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input.md)
- [parse_ews_user_configuration_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_key.md)
- [attribute_values_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)
- [attribute_value_after](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after.md)