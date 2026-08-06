---
type: Rust Function
title: new_mail_hierarchy_row_notification_encodes_message_row_keys
resource: crates/lpe-exchange/src/mapi/notifications.rs#L392-L419
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests
  - functions/crates/lpe-exchange/src/mapi/notifications/rop_hierarchy_table_row_modified_response
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn new_mail_hierarchy_row_notification_encodes_message_row_keys()`

# Calls

- [legacy_for_tests](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests.md)
- [rop_hierarchy_table_row_modified_response](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/rop_hierarchy_table_row_modified_response.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)