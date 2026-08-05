---
type: Rust Function
title: new_mail_notification_without_message_class_defaults_to_ipm_note_and_zero_message_flags
resource: crates/lpe-exchange/src/mapi/notifications.rs#L354-L380
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical
  - functions/crates/lpe-exchange/src/mapi/notifications/rop_notify_response
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn new_mail_notification_without_message_class_defaults_to_ipm_note_and_zero_message_flags()`

# Calls

- [legacy_for_tests](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests.md)
- [canonical](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical.md)
- [rop_notify_response](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/rop_notify_response.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)