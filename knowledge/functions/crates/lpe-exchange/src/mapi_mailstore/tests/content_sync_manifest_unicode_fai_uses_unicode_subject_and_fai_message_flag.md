---
type: Rust Function
title: content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L3429-L3477
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_absent_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_i32_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary
---

# Signature

`fn content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag()`

# Calls

- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [filetime_from_rfc3339_utc](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [sync_manifest_buffer_with_special_objects_and_final_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state.md)
- [assert_variable_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property.md)
- [assert_absent_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_absent_property.md)
- [assert_i32_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_i32_property.md)
- [decode_content_transfer_fai_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary.md)