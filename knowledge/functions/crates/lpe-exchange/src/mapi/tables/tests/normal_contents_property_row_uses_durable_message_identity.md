---
type: Rust Function
title: normal_contents_property_row_uses_durable_message_identity
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L9066-L9144
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/test_table_email
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/new_with_scoped_calendar_identities
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_in_snapshot_with_mailbox_guid
---

# Signature

`fn normal_contents_property_row_uses_durable_message_identity()`

# Calls

- [test_table_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/test_table_email.md)
- [legacy_for_tests](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests.md)
- [new_with_scoped_calendar_identities](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/new_with_scoped_calendar_identities.md)
- [serialize_message_property_row_in_snapshot_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_in_snapshot_with_mailbox_guid.md)