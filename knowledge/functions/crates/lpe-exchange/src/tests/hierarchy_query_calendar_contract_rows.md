---
type: Rust Function
title: hierarchy_query_calendar_contract_rows
resource: crates/lpe-exchange/src/tests/mod.rs#L13473-L13510
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/tests/read_rop_utf16z
  - functions/crates/lpe-exchange/src/tests/read_rop_binary_u16
  - functions/crates/lpe-exchange/src/tests/read_rop_ascii_z
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_custom_only_calendar_collections_keep_default_calendar_openable
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_hierarchy_row_projects_entry_id_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_custom_calendar_hierarchy_row_projects_owner_entry_id_identity
---

# Signature

`fn hierarchy_query_calendar_contract_rows( response_rops: &[u8], query_offset: usize, ) -> Result<Vec<HierarchyCalendarFolderRow>, String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [read_rop_utf16z](../../../../../functions/crates/lpe-exchange/src/tests/read_rop_utf16z.md)
- [read_rop_binary_u16](../../../../../functions/crates/lpe-exchange/src/tests/read_rop_binary_u16.md)
- [read_rop_ascii_z](../../../../../functions/crates/lpe-exchange/src/tests/read_rop_ascii_z.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_custom_only_calendar_collections_keep_default_calendar_openable](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_custom_only_calendar_collections_keep_default_calendar_openable.md)
- [mapi_over_http_calendar_hierarchy_row_projects_entry_id_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_hierarchy_row_projects_entry_id_identity.md)
- [mapi_over_http_custom_calendar_hierarchy_row_projects_owner_entry_id_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_custom_calendar_hierarchy_row_projects_owner_entry_id_identity.md)