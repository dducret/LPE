---
type: Rust Method
title: with_delegate_freebusy_messages
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L181-L196
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/ensure_virtual_local_freebusy_message
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/freebusy_open_prefers_delegate_message_over_stale_associated_config_identity
  - functions/crates/lpe-exchange/src/mapi/rop/tests/delegate_freebusy_getprops_rejects_message_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_delegate_freebusy_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/sync/tests/local_freebusy_direct_copy_projects_account_scoped_entry_id
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_computed_delegate_freebusy_messages
---

# Signature

`pub(crate) fn with_delegate_freebusy_messages( mut self, messages: Vec<DelegateFreeBusyMessageObject>, ) -> Self`

# Calls

- [ensure_virtual_local_freebusy_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/ensure_virtual_local_freebusy_message.md)

# Called by

- [freebusy_open_prefers_delegate_message_over_stale_associated_config_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/freebusy_open_prefers_delegate_message_over_stale_associated_config_identity.md)
- [delegate_freebusy_getprops_rejects_message_from_wrong_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/delegate_freebusy_getprops_rejects_message_from_wrong_folder.md)
- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [fast_transfer_manifest_rejects_delegate_freebusy_from_wrong_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_delegate_freebusy_from_wrong_folder.md)
- [local_freebusy_direct_copy_projects_account_scoped_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/local_freebusy_direct_copy_projects_account_scoped_entry_id.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [snapshot_projects_computed_delegate_freebusy_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_computed_delegate_freebusy_messages.md)