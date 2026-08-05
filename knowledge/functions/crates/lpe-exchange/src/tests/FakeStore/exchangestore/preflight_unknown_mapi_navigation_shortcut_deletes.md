---
type: Rust Method
title: preflight_unknown_mapi_navigation_shortcut_deletes
resource: crates/lpe-exchange/src/tests/mod.rs#L10100-L10164
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
---

# Signature

`fn preflight_unknown_mapi_navigation_shortcut_deletes<'a>( &'a self, account_id: Uuid, folder_id: u64, source_keys: &'a [Vec<u8>], ) -> StoreFuture<'a, ()>`

# Calls

- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)

# Called by

- [append_synchronization_import_deletes_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)