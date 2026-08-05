---
type: Rust Function
title: copy_associated_config_server_metadata
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L16-L31
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delete_associated_config_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/set_associated_config_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_with_identity
  - functions/crates/lpe-exchange/src/mapi/properties/streams/sync_stream_target
---

# Signature

`pub(crate) fn copy_associated_config_server_metadata( source: &serde_json::Value, target: &mut serde_json::Value, )`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [delete_associated_config_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delete_associated_config_properties.md)
- [set_associated_config_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/set_associated_config_properties.md)
- [associated_config_message_with_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_with_identity.md)
- [sync_stream_target](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/sync_stream_target.md)