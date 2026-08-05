---
type: Rust Function
title: scoped_final_sync_state_uses_the_durable_inbox_counter
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L3900-L3953
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/from_special_folder_identity_records
  - functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_object_ids
  - functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_counters
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property
---

# Signature

`async fn scoped_final_sync_state_uses_the_durable_inbox_counter()`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [from_special_folder_identity_records](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/from_special_folder_identity_records.md)
- [with_current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec.md)
- [replguid_idset_from_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_object_ids.md)
- [final_sync_state_stream](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream.md)
- [replguid_globset_counters](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_counters.md)
- [assert_variable_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property.md)