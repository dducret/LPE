---
type: Rust Method
title: fetch_mapi_identities_by_object_ids
resource: crates/lpe-exchange/src/tests/mod.rs#L6791-L6801
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/fake_mapi_identity_lookup_for_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_guid_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/optimized_send_replay_email
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_canonical_message_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/completed_message_move_replay_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
---

# Signature

`fn fetch_mapi_identities_by_object_ids<'a>( &'a self, _account_id: Uuid, object_ids: &'a [u64], ) -> StoreFuture<'a, Vec<MapiIdentityLookupRecord>>`

# Calls

- [fake_mapi_identity_lookup_for_object_id](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/fake_mapi_identity_lookup_for_object_id.md)

# Called by

- [append_modify_permissions_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response.md)
- [append_get_per_user_guid_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_guid_response.md)
- [optimized_send_replay_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/optimized_send_replay_email.md)
- [abort_submit_canonical_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_canonical_message_id.md)
- [completed_message_move_replay_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/completed_message_move_replay_identity.md)
- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)