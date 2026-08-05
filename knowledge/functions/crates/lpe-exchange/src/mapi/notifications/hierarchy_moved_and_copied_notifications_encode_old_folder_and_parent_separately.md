---
type: Rust Function
title: hierarchy_moved_and_copied_notifications_encode_old_folder_and_parent_separately
resource: crates/lpe-exchange/src/mapi/notifications.rs#L468-L519
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/hierarchy_move_or_copy
  - functions/crates/lpe-exchange/src/mapi/notifications/rop_notify_response
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn hierarchy_moved_and_copied_notifications_encode_old_folder_and_parent_separately()`

# Calls

- [legacy_for_tests](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests.md)
- [hierarchy_move_or_copy](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/hierarchy_move_or_copy.md)
- [rop_notify_response](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/rop_notify_response.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)