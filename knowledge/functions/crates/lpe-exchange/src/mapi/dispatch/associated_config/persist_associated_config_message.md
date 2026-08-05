---
type: Rust Function
title: persist_associated_config_message
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L193-L278
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_fai_identity
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_class_and_subject
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_uuid
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/normalized_associated_config_persisted_properties
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_to_json
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/remove_associated_config_server_owned_properties
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_import
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_create
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_pending_associated_config_save_response
---

# Signature

`pub(super) async fn persist_associated_config_message<S>( store: &S, principal: &AccountPrincipal, folder_id: u64, properties: &HashMap<u32, MapiValue>, imported_message_id: Option<u64>, fail_on_conflict: bool, ) -> Result<( crate::store::MapiAssociatedConfigRecord, crate::store::MapiIdentityRecord, Option<MapiFaiImportDisposition>, )> where S: ExchangeStore,`

# Calls

- [imported_fai_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_fai_identity.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [as_i64](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [associated_config_class_and_subject](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_class_and_subject.md)
- [associated_config_uuid](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_uuid.md)
- [normalized_associated_config_persisted_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/normalized_associated_config_persisted_properties.md)
- [mapi_properties_to_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_to_json.md)
- [mapi_properties_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json.md)
- [remove_associated_config_server_owned_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/remove_associated_config_server_owned_properties.md)
- [commit_mapi_associated_config_import](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_import.md)
- [remember_mapi_identity_with_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key.md)
- [commit_mapi_associated_config_create](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_create.md)

# Called by

- [append_pending_associated_config_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_pending_associated_config_save_response.md)