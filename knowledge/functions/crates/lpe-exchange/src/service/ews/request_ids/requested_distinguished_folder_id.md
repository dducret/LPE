---
type: Rust Function
title: requested_distinguished_folder_id
resource: crates/lpe-exchange/src/service/ews/request_ids.rs#L150-L159
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/notifications/notification_subscription_id
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_role
---

# Signature

`pub(in crate::service) fn requested_distinguished_folder_id(request: &str) -> Option<&str>`

# Calls

- [attribute_values_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [notification_subscription_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/notification_subscription_id.md)
- [requested_mailbox_role](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_role.md)