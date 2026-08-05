---
type: Rust Function
title: outlook_inbox_fai_ics_omits_unsupported_message_identity_properties
resource: crates/lpe-exchange/src/mapi/sync/tests.rs#L477-L529
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
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary
  - functions/crates/lpe-exchange/src/mapi/sync/tests/assert_has_tags
---

# Signature

`fn outlook_inbox_fai_ics_omits_unsupported_message_identity_properties()`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [with_associated_configs](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs.md)
- [special_sync_objects_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [sync_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/sync_principal.md)
- [associated_content_sync_buffer_with_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer_with_flags.md)
- [decode_content_transfer_fai_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary.md)
- [assert_has_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/assert_has_tags.md)