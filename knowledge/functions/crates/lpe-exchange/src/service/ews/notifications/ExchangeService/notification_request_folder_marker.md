---
type: Rust Method
title: notification_request_folder_marker
resource: crates/lpe-exchange/src/service/ews/notifications.rs#L194-L221
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_role
  - functions/crates/lpe-exchange/src/service/ews/notifications/pull_subscription_subscribes_to_all_folders
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/register_pull_subscription
---

# Signature

`async fn notification_request_folder_marker( &self, principal: &AccountPrincipal, request: &str, ) -> Result<Option<String>>`

# Calls

- [next](../../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [requested_mailbox_role](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_role.md)
- [pull_subscription_subscribes_to_all_folders](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/pull_subscription_subscribes_to_all_folders.md)

# Called by

- [register_pull_subscription](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/register_pull_subscription.md)