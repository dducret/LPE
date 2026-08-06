---
type: Rust Function
title: associated_config_modeled_property
resource: crates/lpe-exchange/src/mapi/rop.rs#L750-L772
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property
---

# Signature

`fn associated_config_modeled_property( object: Option<&MapiObject>, principal: &AccountPrincipal, snapshot: &MapiMailStoreSnapshot, tag: u32, ) -> bool`

# Calls

- [associated_config_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)

# Called by

- [fallback_default_specific_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property.md)