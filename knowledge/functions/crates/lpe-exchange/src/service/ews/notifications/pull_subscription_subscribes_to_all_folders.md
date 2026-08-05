---
type: Rust Function
title: pull_subscription_subscribes_to_all_folders
resource: crates/lpe-exchange/src/service/ews/notifications.rs#L423-L427
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/notification_request_folder_marker
---

# Signature

`pub(in crate::service) fn pull_subscription_subscribes_to_all_folders(request: &str) -> bool`

# Calls

- [open_tag_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text.md)
- [attribute_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value.md)

# Called by

- [notification_request_folder_marker](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/notification_request_folder_marker.md)