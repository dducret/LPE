---
type: Rust Function
title: object_moved_and_copied_notifications_preserve_source_message_id
resource: crates/lpe-exchange/src/mapi/notifications.rs#L454-L504
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_old_message_id
  - functions/crates/lpe-exchange/src/mapi/notifications/rop_notify_response
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn object_moved_and_copied_notifications_preserve_source_message_id()`

# Calls

- [legacy_for_tests](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests.md)
- [canonical](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical.md)
- [with_old_message_id](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_old_message_id.md)
- [rop_notify_response](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/rop_notify_response.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)