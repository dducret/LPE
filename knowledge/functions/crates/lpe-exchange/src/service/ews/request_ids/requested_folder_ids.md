---
type: Rust Function
title: requested_folder_ids
resource: crates/lpe-exchange/src/service/ews/request_ids.rs#L52-L57
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/notifications/notification_subscription_id
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_folder_ids
---

# Signature

`pub(in crate::service) fn requested_folder_ids(request: &str) -> Vec<String>`

# Calls

- [attribute_values_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)

# Called by

- [notification_subscription_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/notification_subscription_id.md)
- [requested_mailbox_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_folder_ids.md)