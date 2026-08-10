---
type: Rust Function
title: append_mapi_multi_binary_property
resource: crates/lpe-exchange/src/tests/mod.rs#L15077-L15084
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_same_execute_additional_ren_junk_alias_opens_junk
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_additional_ren_entry_ids_canonicalize_reserved_slots_across_reconnect
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_open_folder_accepts_additional_ren_junk_alias_in_a_new_session
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_folder_set_properties_accepts_additional_ren_entry_ids
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_root_set_properties_accepts_additional_ren_entry_ids_as_cache_write
---

# Signature

`fn append_mapi_multi_binary_property(values: &mut Vec<u8>, property_tag: u32, items: &[&[u8]])`

# Called by

- [mapi_over_http_same_execute_additional_ren_junk_alias_opens_junk](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_same_execute_additional_ren_junk_alias_opens_junk.md)
- [mapi_over_http_additional_ren_entry_ids_canonicalize_reserved_slots_across_reconnect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_additional_ren_entry_ids_canonicalize_reserved_slots_across_reconnect.md)
- [mapi_over_http_open_folder_accepts_additional_ren_junk_alias_in_a_new_session](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_open_folder_accepts_additional_ren_junk_alias_in_a_new_session.md)
- [mapi_over_http_folder_set_properties_accepts_additional_ren_entry_ids](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_folder_set_properties_accepts_additional_ren_entry_ids.md)
- [mapi_over_http_root_set_properties_accepts_additional_ren_entry_ids_as_cache_write](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_root_set_properties_accepts_additional_ren_entry_ids_as_cache_write.md)