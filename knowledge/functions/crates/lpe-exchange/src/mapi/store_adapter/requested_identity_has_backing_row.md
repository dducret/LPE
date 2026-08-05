---
type: Rust Function
title: requested_identity_has_backing_row
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L935-L955
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/modeled_virtual_associated_config_message_for_canonical_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
---

# Signature

`fn requested_identity_has_backing_row( identity: &MapiIdentityLookupRecord, mailbox_ids: &HashSet<Uuid>, search_folder_definition_ids: &HashSet<Uuid>, associated_config_ids: &HashSet<Uuid>, ) -> bool`

# Calls

- [modeled_virtual_associated_config_message_for_canonical_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/modeled_virtual_associated_config_message_for_canonical_id.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)