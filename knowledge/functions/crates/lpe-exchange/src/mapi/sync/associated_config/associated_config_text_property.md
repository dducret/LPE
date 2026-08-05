---
type: Rust Function
title: associated_config_text_property
resource: crates/lpe-exchange/src/mapi/sync/associated_config.rs#L141-L148
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object
---

# Signature

`fn associated_config_text_property( message: &crate::mapi_store::MapiAssociatedConfigMessage, tag: u32, ) -> Option<String>`

# Calls

- [mapi_properties_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [associated_config_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object.md)