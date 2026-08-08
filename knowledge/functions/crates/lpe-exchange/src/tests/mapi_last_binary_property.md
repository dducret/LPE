---
type: Rust Function
title: mapi_last_binary_property
resource: crates/lpe-exchange/src/tests/mod.rs#L14941-L14946
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_manifest_includes_folder_change_key_facts
---

# Signature

`fn mapi_last_binary_property(bytes: &[u8], property_tag: u32) -> Option<&[u8]>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [mapi_over_http_hierarchy_sync_manifest_includes_folder_change_key_facts](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_manifest_includes_folder_change_key_facts.md)