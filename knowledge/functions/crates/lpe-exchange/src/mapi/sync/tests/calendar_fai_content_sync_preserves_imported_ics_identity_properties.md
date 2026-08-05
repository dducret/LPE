---
type: Rust Function
title: calendar_fai_content_sync_preserves_imported_ics_identity_properties
resource: crates/lpe-exchange/src/mapi/sync/tests.rs#L388-L474
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
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/fast_transfer_message_content_buffer_with_special_object
---

# Signature

`fn calendar_fai_content_sync_preserves_imported_ics_identity_properties()`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [with_associated_configs](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs.md)
- [special_sync_objects_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [sync_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/sync_principal.md)
- [associated_content_sync_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer.md)
- [fast_transfer_message_content_buffer_with_special_object](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/fast_transfer_message_content_buffer_with_special_object.md)