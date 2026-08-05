---
type: Rust Function
title: associated_config_message_with_identity
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L285-L324
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_to_json
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/copy_associated_config_server_metadata
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_pending_associated_config_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response
---

# Signature

`fn associated_config_message_with_identity( saved: &crate::store::MapiAssociatedConfigRecord, identity: &crate::store::MapiIdentityRecord, ) -> crate::mapi_store::MapiAssociatedConfigMessage`

# Calls

- [mapi_properties_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json.md)
- [mapi_properties_to_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_to_json.md)
- [copy_associated_config_server_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/copy_associated_config_server_metadata.md)

# Called by

- [append_pending_associated_config_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_pending_associated_config_save_response.md)
- [append_existing_associated_config_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response.md)