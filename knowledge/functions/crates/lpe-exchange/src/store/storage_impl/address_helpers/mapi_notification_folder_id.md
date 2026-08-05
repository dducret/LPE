---
type: Rust Function
title: mapi_notification_folder_id
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L811-L819
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_folder_id_from_role_or_identity
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_hierarchy_old_parent_folder_id
---

# Signature

`fn mapi_notification_folder_id( role: Option<&str>, durable_identity: Option<i64>, canonical_id: Option<Uuid>, allocated_ids: &std::collections::HashMap<Uuid, u64>, ) -> Option<u64>`

# Calls

- [mapi_folder_id_from_role_or_identity](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_folder_id_from_role_or_identity.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [mapi_notification_event_from_change_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)
- [mapi_hierarchy_old_parent_folder_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_hierarchy_old_parent_folder_id.md)