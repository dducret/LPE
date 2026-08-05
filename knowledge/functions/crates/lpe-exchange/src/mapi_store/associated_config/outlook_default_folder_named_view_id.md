---
type: Rust Function
title: outlook_default_folder_named_view_id
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L191-L200
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_match_state_reports_pre_advertised_folder_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_tracks_multiple_owner_folders
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/folder_default_named_view_open_rejects_unpersisted_contact_view
  - functions/crates/lpe-exchange/src/mapi/properties/tests/inbox_mailbox_properties_advertise_openable_default_view
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_folder_view_entry_id
  - functions/crates/lpe-exchange/src/mapi/rop/tests/common_view_descriptor_getprops_contract_reports_unpersisted_view_missing
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/default_folder_associated_named_view
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message
  - functions/crates/lpe-exchange/src/mapi_store/tests/inbox_default_named_view_is_materialized_for_the_advertised_entry_id
---

# Signature

`pub(crate) fn outlook_default_folder_named_view_id(folder_id: u64) -> u64`

# Calls

- [global_counter_from_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)

# Called by

- [format_outlook_view_handoff_table_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [debug_default_folder_associated_named_view](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view.md)
- [default_view_match_state_reports_pre_advertised_folder_open](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_match_state_reports_pre_advertised_folder_open.md)
- [default_view_advertisement_state_tracks_multiple_owner_folders](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_tracks_multiple_owner_folders.md)
- [folder_default_named_view_open_rejects_unpersisted_contact_view](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/folder_default_named_view_open_rejects_unpersisted_contact_view.md)
- [inbox_mailbox_properties_advertise_openable_default_view](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/inbox_mailbox_properties_advertise_openable_default_view.md)
- [default_folder_view_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_folder_view_entry_id.md)
- [common_view_descriptor_getprops_contract_reports_unpersisted_view_missing](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/common_view_descriptor_getprops_contract_reports_unpersisted_view_missing.md)
- [default_folder_associated_named_view](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/default_folder_associated_named_view.md)
- [default_folder_named_view_message](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message.md)
- [inbox_default_named_view_is_materialized_for_the_advertised_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/inbox_default_named_view_is_materialized_for_the_advertised_entry_id.md)