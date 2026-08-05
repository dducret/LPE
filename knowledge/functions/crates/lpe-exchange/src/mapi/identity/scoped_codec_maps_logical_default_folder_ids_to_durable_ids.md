---
type: Rust Function
title: scoped_codec_maps_logical_default_folder_ids_to_durable_ids
resource: crates/lpe-exchange/src/mapi/identity.rs#L1062-L1175
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/from_special_folder_identity_records
  - functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical
  - functions/crates/lpe-exchange/src/mapi/notifications/rop_notify_response
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn scoped_codec_maps_logical_default_folder_ids_to_durable_ids()`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [from_special_folder_identity_records](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/from_special_folder_identity_records.md)
- [with_current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec.md)
- [canonical](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical.md)
- [rop_notify_response](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/rop_notify_response.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)