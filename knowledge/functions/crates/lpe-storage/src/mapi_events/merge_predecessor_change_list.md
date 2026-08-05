---
type: Rust Function
title: merge_predecessor_change_list
resource: crates/lpe-storage/src/mapi_events.rs#L1420-L1435
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/collaboration/deleted_events/rekey_active_event_identities_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/validate_imported_identity
  - functions/crates/lpe-storage/src/mapi_contacts/allocate_contact_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_events/rotate_mapi_event_identities_in_tx
  - functions/crates/lpe-storage/src/mapi_events/pcl_merge_keeps_the_latest_xid_per_replica_and_sorts_replicas
  - functions/crates/lpe-storage/src/mapi_events/imported_identity/validate_imported_identity
  - functions/crates/lpe-storage/src/mapi_events/imported_identity/allocate_mapi_event_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_events/imported_identity/imported_identity_accepts_a_canonical_multi_replica_pcl_containing_its_change_key
  - functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_message_identity/rekey_active_mapi_message_identity_for_server_move_in_tx
  - functions/crates/lpe-storage/src/message_ops/rekey_mapi_message_identity_in_tx
---

# Signature

`pub(crate) fn merge_predecessor_change_list(current: &[u8], change_key: &[u8]) -> Result<Vec<u8>>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rekey_active_event_identities_in_tx](../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/rekey_active_event_identities_in_tx.md)
- [validate_imported_identity](../../../../../functions/crates/lpe-storage/src/mapi_contacts/validate_imported_identity.md)
- [allocate_contact_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/allocate_contact_identity_in_tx.md)
- [rotate_mapi_event_identities_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_events/rotate_mapi_event_identities_in_tx.md)
- [pcl_merge_keeps_the_latest_xid_per_replica_and_sorts_replicas](../../../../../functions/crates/lpe-storage/src/mapi_events/pcl_merge_keeps_the_latest_xid_per_replica_and_sorts_replicas.md)
- [validate_imported_identity](../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/validate_imported_identity.md)
- [allocate_mapi_event_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/allocate_mapi_event_identity_in_tx.md)
- [imported_identity_accepts_a_canonical_multi_replica_pcl_containing_its_change_key](../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/imported_identity_accepts_a_canonical_multi_replica_pcl_containing_its_change_key.md)
- [rotate_active_mapi_message_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx.md)
- [rekey_active_mapi_message_identity_for_server_move_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_message_identity/rekey_active_mapi_message_identity_for_server_move_in_tx.md)
- [rekey_mapi_message_identity_in_tx](../../../../../functions/crates/lpe-storage/src/message_ops/rekey_mapi_message_identity_in_tx.md)