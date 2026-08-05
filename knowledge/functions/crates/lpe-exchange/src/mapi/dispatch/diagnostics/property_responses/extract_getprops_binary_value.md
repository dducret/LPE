---
type: Rust Function
title: extract_getprops_binary_value
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L458-L488
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_umolk_roaming_dictionary_contract
---

# Signature

`fn extract_getprops_binary_value( property_tags: &[u32], response: &[u8], target_tag: u32, ) -> Option<Vec<u8>>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [canonical_property_storage_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [read_u8](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [parse_property_value_for_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag.md)

# Called by

- [summarize_umolk_roaming_dictionary_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_umolk_roaming_dictionary_contract.md)