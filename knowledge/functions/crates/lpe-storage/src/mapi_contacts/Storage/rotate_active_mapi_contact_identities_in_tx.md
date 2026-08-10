---
type: Rust Method
title: rotate_active_mapi_contact_identities_in_tx
resource: crates/lpe-storage/src/mapi_contacts.rs#L148-L213
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx
  - functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx
  - functions/crates/lpe-storage/src/mapi_events/mapi_change_key
  - functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role
---

# Signature

`pub(crate) async fn rotate_active_mapi_contact_identities_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, contact_id: Uuid, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [allocate_mapi_store_global_counter_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx.md)
- [ensure_mapi_mailbox_replica_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx.md)
- [mapi_change_key](../../../../../../functions/crates/lpe-storage/src/mapi_events/mapi_change_key.md)
- [merge_predecessor_change_list](../../../../../../functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [commit_mapi_contact_update](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update.md)
- [upsert_client_contact_in_book_role](../../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role.md)