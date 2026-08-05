---
type: Rust Function
title: associated_config_fai_no_foreign_identifiers_uses_local_source_key
resource: crates/lpe-exchange/src/mapi/sync/tests.rs#L663-L741
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
  - functions/crates/lpe-exchange/src/mapi/sync/tests/sync_principal
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer_with_flags
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
---

# Signature

`fn associated_config_fai_no_foreign_identifiers_uses_local_source_key()`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [with_associated_configs](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs.md)
- [special_sync_objects_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [sync_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/sync_principal.md)
- [associated_content_sync_buffer_with_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer_with_flags.md)
- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)