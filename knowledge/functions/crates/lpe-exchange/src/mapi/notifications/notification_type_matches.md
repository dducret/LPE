---
type: Rust Function
title: notification_type_matches
resource: crates/lpe-exchange/src/mapi/notifications.rs#L812-L814
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/notifications/registration_matches_event
---

# Signature

`fn notification_type_matches(requested: u16, event_mask: u16) -> bool`

# Called by

- [registration_matches_event](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/registration_matches_event.md)