---
type: Rust Function
title: is_associated_config_server_owned_property_tag
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L41-L58
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_read_only_property_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/remove_associated_config_server_owned_properties
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/apply_associated_config_identities
---

# Signature

`pub(crate) fn is_associated_config_server_owned_property_tag(property_tag: u32) -> bool`

# Calls

- [is_associated_config_read_only_property_tag](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_read_only_property_tag.md)

# Called by

- [remove_associated_config_server_owned_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/remove_associated_config_server_owned_properties.md)
- [apply_associated_config_identities](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/apply_associated_config_identities.md)