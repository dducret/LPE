---
type: Rust Function
title: post_hierarchy_getprops_contract
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy.rs#L76-L96
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/getprops_contract_response_summary
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
---

# Signature

`pub(in crate::mapi::dispatch) fn post_hierarchy_getprops_contract( request: &RopRequest, object: Option<&MapiObject>, property_response: &[u8], ) -> String`

# Calls

- [property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [getprops_contract_response_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/getprops_contract_response_summary.md)

# Called by

- [append_get_properties_specific_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)