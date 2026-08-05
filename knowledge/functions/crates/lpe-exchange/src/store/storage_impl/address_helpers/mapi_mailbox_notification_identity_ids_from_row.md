---
type: Rust Function
title: mapi_mailbox_notification_identity_ids_from_row
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L896-L967
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/push_unique_uuid
---

# Signature

`fn mapi_mailbox_notification_identity_ids_from_row(row: &sqlx::postgres::PgRow) -> Vec<Uuid>`

# Calls

- [push_unique_uuid](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/push_unique_uuid.md)