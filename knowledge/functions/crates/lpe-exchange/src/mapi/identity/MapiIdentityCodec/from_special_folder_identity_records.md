---
type: Rust Method
title: from_special_folder_identity_records
resource: crates/lpe-exchange/src/mapi/identity.rs#L182-L261
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-exchange/src/mapi/identity/logical_special_folder_ids
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/scoped_codec_maps_logical_default_folder_ids_to_durable_ids
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/scoped_final_sync_state_uses_the_durable_inbox_counter
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
---

# Signature

`pub(crate) fn from_special_folder_identity_records( replica_guid: Uuid, requests: &[MapiIdentityRequest], records: &[MapiIdentityRecord], ) -> Result<Self>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [logical_special_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/logical_special_folder_ids.md)

# Called by

- [scoped_codec_maps_logical_default_folder_ids_to_durable_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/scoped_codec_maps_logical_default_folder_ids_to_durable_ids.md)
- [load_mapi_identity_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope.md)
- [scoped_final_sync_state_uses_the_durable_inbox_counter](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/scoped_final_sync_state_uses_the_durable_inbox_counter.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)