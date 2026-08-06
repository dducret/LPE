---
type: Rust Function
title: hierarchy_query_display_container_rows
resource: crates/lpe-exchange/src/tests/mod.rs#L13510-L13569
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/tests/read_rop_utf16z
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_root_hierarchy_table_uses_none_container_classes_for_root_children
---

# Signature

`fn hierarchy_query_display_container_rows( response_rops: &[u8], query_offset: usize, ) -> Result<Vec<(String, String)>, String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [read_rop_utf16z](../../../../../functions/crates/lpe-exchange/src/tests/read_rop_utf16z.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_root_hierarchy_table_uses_none_container_classes_for_root_children](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_root_hierarchy_table_uses_none_container_classes_for_root_children.md)