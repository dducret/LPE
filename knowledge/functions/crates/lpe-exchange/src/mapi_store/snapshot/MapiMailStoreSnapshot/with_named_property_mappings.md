---
type: Rust Method
title: with_named_property_mappings
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L423-L432
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
---

# Signature

`pub(crate) fn with_named_property_mappings( mut self, mappings: Vec<MapiNamedPropertyMapping>, ) -> Self`

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [associated_config_fai_content_sync_emits_valid_property_definitions](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)