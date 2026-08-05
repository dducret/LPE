---
type: Rust Method
title: add_mapi_local_replica_deleted_ranges
resource: crates/lpe-exchange/src/tests/mod.rs#L4824-L4867
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_set_local_replica_midset_deleted_response
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_sync_import_save_reports_deleted_source_key
---

# Signature

`fn add_mapi_local_replica_deleted_ranges<'a>( &'a self, account_id: Uuid, folder_id: u64, ranges: &'a [crate::store::MapiLocalReplicaDeletedRange], ) -> StoreFuture<'a, ()>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_set_local_replica_midset_deleted_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_set_local_replica_midset_deleted_response.md)
- [mapi_over_http_contact_sync_import_save_reports_deleted_source_key](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_sync_import_save_reports_deleted_source_key.md)