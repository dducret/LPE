---
type: Rust Method
title: associated_config_sync_messages_for_folder
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1288-L1293
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_messages_for_folder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
  - functions/crates/lpe-exchange/src/mapi_store/tests/associated_config_sync_messages_use_persisted_rows_before_narrow_defaults
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql
---

# Signature

`pub(crate) fn associated_config_sync_messages_for_folder( &self, folder_id: u64, ) -> Vec<MapiAssociatedConfigMessage>`

# Calls

- [associated_config_messages_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_messages_for_folder.md)

# Called by

- [special_sync_objects_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [associated_config_sync_messages_use_persisted_rows_before_narrow_defaults](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/associated_config_sync_messages_use_persisted_rows_before_narrow_defaults.md)
- [mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql.md)