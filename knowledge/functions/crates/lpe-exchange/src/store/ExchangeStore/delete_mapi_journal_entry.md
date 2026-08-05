---
type: Rust Method
title: delete_mapi_journal_entry
resource: crates/lpe-exchange/src/store.rs#L845-L856
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
---

# Signature

`fn delete_mapi_journal_entry<'a>( &'a self, account_id: Uuid, entry_id: Uuid, ) -> StoreFuture<'a, ()>`

# Called by

- [append_delete_messages_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [append_synchronization_import_deletes_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)