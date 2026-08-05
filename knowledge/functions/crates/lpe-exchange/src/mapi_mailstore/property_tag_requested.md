---
type: Rust Function
title: property_tag_requested
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L294-L298
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_matches
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_excluded
  - functions/crates/lpe-exchange/src/mapi_mailstore/content_property_in_scope
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_microsoft_payload_comparison
---

# Signature

`fn property_tag_requested(requested_property_tags: &[u32], property_tag: u32) -> bool`

# Calls

- [property_tag_matches](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_matches.md)

# Called by

- [property_tag_excluded](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_excluded.md)
- [content_property_in_scope](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/content_property_in_scope.md)
- [hierarchy_microsoft_payload_comparison](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_microsoft_payload_comparison.md)