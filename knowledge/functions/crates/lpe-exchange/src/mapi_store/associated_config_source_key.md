---
type: Rust Function
title: associated_config_source_key
resource: crates/lpe-exchange/src/mapi_store.rs#L320-L327
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key
---

# Signature

`fn associated_config_source_key(properties_json: &serde_json::Value) -> Option<Vec<u8>>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [associated_config_message_for_folder_and_source_key](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key.md)