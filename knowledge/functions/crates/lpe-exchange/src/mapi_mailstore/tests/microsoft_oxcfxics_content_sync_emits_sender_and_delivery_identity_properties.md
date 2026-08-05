---
type: Rust Function
title: microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L780-L857
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_i64_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_absent_property
---

# Signature

`fn microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties()`

# Calls

- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [filetime_from_rfc3339_utc](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [sync_manifest_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments.md)
- [assert_i64_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_i64_property.md)
- [assert_variable_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property.md)
- [replguid_idset_from_counters](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters.md)
- [assert_absent_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_absent_property.md)