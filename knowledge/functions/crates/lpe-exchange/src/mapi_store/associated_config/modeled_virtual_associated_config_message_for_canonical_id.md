---
type: Rust Function
title: modeled_virtual_associated_config_message_for_canonical_id
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L395-L401
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_associated_config_sync_defaults
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi/store_adapter/requested_identity_has_backing_row
---

# Signature

`pub(crate) fn modeled_virtual_associated_config_message_for_canonical_id( canonical_id: Uuid, ) -> Option<MapiAssociatedConfigMessage>`

# Calls

- [outlook_inbox_associated_config_sync_defaults](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_associated_config_sync_defaults.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [requested_identity_has_backing_row](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/requested_identity_has_backing_row.md)