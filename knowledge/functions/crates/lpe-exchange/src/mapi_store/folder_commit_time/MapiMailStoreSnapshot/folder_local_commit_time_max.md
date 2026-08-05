---
type: Rust Method
title: folder_local_commit_time_max
resource: crates/lpe-exchange/src/mapi_store/folder_commit_time.rs#L4-L113
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/LPE-CT/web/app/smoke/test/MockIntersectionObserver/observe
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_parent_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/properties/folder/folder_local_commit_time_max_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_contacts_local_commit_time_tracks_canonical_update
---

# Signature

`pub(crate) fn folder_local_commit_time_max( &self, folder_id: u64, mailboxes: &[JmapMailbox], ) -> Option<u64>`

# Calls

- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [search_folder_definition_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [observe](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockIntersectionObserver/observe.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [mapi_folder_parent_id_for_mailbox](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_parent_id_for_mailbox.md)
- [folder_version](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version.md)

# Called by

- [folder_properties_for_open_from_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes.md)
- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [folder_local_commit_time_max_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/folder_local_commit_time_max_property_value.md)
- [serialize_hierarchy_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row.md)
- [postgres_mapi_contacts_local_commit_time_tracks_canonical_update](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_contacts_local_commit_time_tracks_canonical_update.md)