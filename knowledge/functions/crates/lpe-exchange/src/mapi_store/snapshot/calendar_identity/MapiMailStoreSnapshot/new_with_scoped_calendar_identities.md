---
type: Rust Method
title: new_with_scoped_calendar_identities
resource: crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity.rs#L210-L243
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/ScopedCalendarIdentities/from_records
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/tests/saved_message_handle_getprops_keeps_batch_email_and_durable_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi/tables/tests/normal_contents_property_row_uses_durable_message_identity
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/scoped_snapshot_retains_all_durable_identity_records
---

# Signature

`pub(crate) fn new_with_scoped_calendar_identities( mailboxes: Vec<JmapMailbox>, emails: Vec<JmapEmail>, attachments: Vec<(Uuid, Vec<ActiveSyncAttachment>)>, contact_collections: Vec<CollaborationCollection>, calendar_collections: Vec<CollaborationCollection>, task_collections: Vec<CollaborationCollection>, contacts: Vec<AccessibleContact>, events: Vec<AccessibleEvent>, deleted_events: Vec<AccessibleEvent>, tasks: Vec<ClientTask>, folder_permissions: Vec<MapiFolderPermission>, identity_records: &[MapiIdentityRecord], identity_codec: &crate::mapi::identity::MapiIdentityCodec, ) -> Result<Self>`

# Calls

- [from_records](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/ScopedCalendarIdentities/from_records.md)
- [build](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)

# Called by

- [saved_message_handle_getprops_keeps_batch_email_and_durable_identity](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/saved_message_handle_getprops_keeps_batch_email_and_durable_identity.md)
- [load_mapi_store_for_access_plan](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [normal_contents_property_row_uses_durable_message_identity](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/normal_contents_property_row_uses_durable_message_identity.md)
- [load_mapi_mail_store](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [scoped_snapshot_retains_all_durable_identity_records](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/scoped_snapshot_retains_all_durable_identity_records.md)