---
type: Rust Function
title: mapi_special_object_kind_for_checkpoint_mailbox
resource: crates/lpe-exchange/src/store/storage_impl/mapi_helpers.rs#L47-L121
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn mapi_special_object_kind_for_checkpoint_mailbox( storage: &Storage, tenant_id: &Uuid, account_id: Uuid, checkpoint_kind: MapiCheckpointKind, mailbox_id: Option<Uuid>, ) -> Result<Option<&'static str>>`

# Calls

- [virtual_special_mailbox](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)