---
type: Rust Function
title: mapi_message_ids_for_deleted_changes
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L1121-L1136
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_object_ids_for_deleted_changes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(super) async fn mapi_message_ids_for_deleted_changes<S>( store: &S, principal: &AccountPrincipal, message_ids: &[Uuid], ) -> Result<Vec<u64>> where S: ExchangeStore,`

# Calls

- [mapi_object_ids_for_deleted_changes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_object_ids_for_deleted_changes.md)

# Called by

- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)