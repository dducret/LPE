---
type: Rust Function
title: incomplete_message_move_notifications_are_not_serialized
resource: crates/lpe-exchange/src/mapi/notifications.rs#L560-L600
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_old_message_id
---

# Signature

`fn incomplete_message_move_notifications_are_not_serialized()`

# Calls

- [legacy_for_tests](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests.md)
- [canonical](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical.md)
- [with_old_message_id](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_old_message_id.md)