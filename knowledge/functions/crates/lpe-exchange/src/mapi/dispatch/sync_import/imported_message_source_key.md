---
type: Rust Function
title: imported_message_source_key
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L708-L716
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_uuid
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/transient_associated_message_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/pending_message_is_trash_sync_artifact
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_fai_identity
---

# Signature

`pub(super) fn imported_message_source_key(properties: &HashMap<u32, MapiValue>) -> Option<Vec<u8>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [current_store_replica_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid.md)

# Called by

- [associated_config_uuid](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_uuid.md)
- [transient_associated_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/transient_associated_message_id.md)
- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [pending_message_is_trash_sync_artifact](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/pending_message_is_trash_sync_artifact.md)
- [imported_fai_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_fai_identity.md)