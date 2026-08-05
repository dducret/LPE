---
type: Rust Function
title: completed_message_move_replay_identity
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move.rs#L257-L319
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_identities_by_object_ids
  - functions/crates/lpe-exchange/src/mapi/sync/email_matches_folder
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/append_synchronization_import_message_move_response
---

# Signature

`async fn completed_message_move_replay_identity<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, source_folder_id: u64, source_message_id: u64, target_folder_id: u64, destination_message_id: u64, imported_identity: &MapiMessageImportedMoveIdentity, mailboxes: &[JmapMailbox], emails: &[JmapEmail], ) -> anyhow::Result<Option<crate::store::MapiIdentityRecord>>`

# Calls

- [fetch_mapi_identities_by_object_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_identities_by_object_ids.md)
- [email_matches_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/email_matches_folder.md)
- [fetch_or_allocate_mapi_identities](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)

# Called by

- [append_synchronization_import_message_move_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/append_synchronization_import_message_move_response.md)