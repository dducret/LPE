---
type: Rust Function
title: unchanged_object_keeps_source_key_and_changed_object_advances_change_number
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L416-L441
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number
---

# Signature

`fn unchanged_object_keeps_source_key_and_changed_object_advances_change_number()`

# Calls

- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [source_key_for_uuid](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid.md)
- [canonical_message_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number.md)