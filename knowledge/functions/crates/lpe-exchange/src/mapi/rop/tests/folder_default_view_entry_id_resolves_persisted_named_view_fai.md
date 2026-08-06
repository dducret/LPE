---
type: Rust Function
title: folder_default_view_entry_id_resolves_persisted_named_view_fai
resource: crates/lpe-exchange/src/mapi/rop/tests.rs#L4141-L4227
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_config_identity_ids
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn folder_default_view_entry_id_resolves_persisted_named_view_fai()`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [with_associated_configs](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs.md)
- [with_associated_config_identity_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_config_identity_ids.md)
- [rop_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)