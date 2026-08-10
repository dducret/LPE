---
type: Rust Function
title: mapi_change_key
resource: crates/lpe-storage/src/mapi_events.rs#L1428-L1433
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/collaboration/deleted_events/rekey_active_event_identities_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/rotate_active_mapi_contact_identities_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/allocate_contact_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/imported_identity
  - functions/crates/lpe-storage/src/mapi_contacts/imported_contact_change_key_must_use_a_foreign_identifier
  - functions/crates/lpe-storage/src/mapi_events/rotate_mapi_event_identities_in_tx
  - functions/crates/lpe-storage/src/mapi_events/pcl_merge_keeps_the_latest_xid_per_replica_and_sorts_replicas
  - functions/crates/lpe-storage/src/mapi_events/imported_identity/allocate_mapi_event_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_events/imported_identity/realistic_imported_identity
  - functions/crates/lpe-storage/src/mapi_events/imported_identity/imported_identity_accepts_a_canonical_multi_replica_pcl_containing_its_change_key
  - functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_message_identity/rekey_active_mapi_message_identity_for_server_move_in_tx
---

# Signature

`pub(crate) fn mapi_change_key(replica_guid: Uuid, change_number: u64) -> Vec<u8>`

# Called by

- [rekey_active_event_identities_in_tx](../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/rekey_active_event_identities_in_tx.md)
- [rotate_active_mapi_contact_identities_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/rotate_active_mapi_contact_identities_in_tx.md)
- [allocate_contact_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/allocate_contact_identity_in_tx.md)
- [imported_identity](../../../../../functions/crates/lpe-storage/src/mapi_contacts/imported_identity.md)
- [imported_contact_change_key_must_use_a_foreign_identifier](../../../../../functions/crates/lpe-storage/src/mapi_contacts/imported_contact_change_key_must_use_a_foreign_identifier.md)
- [rotate_mapi_event_identities_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_events/rotate_mapi_event_identities_in_tx.md)
- [pcl_merge_keeps_the_latest_xid_per_replica_and_sorts_replicas](../../../../../functions/crates/lpe-storage/src/mapi_events/pcl_merge_keeps_the_latest_xid_per_replica_and_sorts_replicas.md)
- [allocate_mapi_event_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/allocate_mapi_event_identity_in_tx.md)
- [realistic_imported_identity](../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/realistic_imported_identity.md)
- [imported_identity_accepts_a_canonical_multi_replica_pcl_containing_its_change_key](../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/imported_identity_accepts_a_canonical_multi_replica_pcl_containing_its_change_key.md)
- [rotate_active_mapi_message_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx.md)
- [rekey_active_mapi_message_identity_for_server_move_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_message_identity/rekey_active_mapi_message_identity_for_server_move_in_tx.md)