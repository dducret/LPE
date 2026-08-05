---
type: Rust Function
title: default_folder_hierarchy_projection_for_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders.rs#L100-L145
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_discovery_specs
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/expected_special_folder_parent_id
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_default_folder_response_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/default_folder_hierarchy_projection_reports_calendar_and_contacts_identity
---

# Signature

`pub(in crate::mapi::dispatch) fn default_folder_hierarchy_projection_for_debug( principal: &AccountPrincipal, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [default_folder_discovery_specs](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_discovery_specs.md)
- [special_folder_identification_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value.md)
- [source_key_for_store_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [expected_special_folder_parent_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/expected_special_folder_parent_id.md)
- [folder_row_for_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [virtual_special_mailbox](../../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [collaboration_folder_for_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)

# Called by

- [log_get_properties_default_folder_response_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_default_folder_response_debug.md)
- [default_folder_hierarchy_projection_reports_calendar_and_contacts_identity](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/default_folder_hierarchy_projection_reports_calendar_and_contacts_identity.md)