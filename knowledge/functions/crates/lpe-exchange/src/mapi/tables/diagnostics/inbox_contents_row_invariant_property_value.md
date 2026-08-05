---
type: Rust Function
title: inbox_contents_row_invariant_property_value
resource: crates/lpe-exchange/src/mapi/tables/diagnostics.rs#L177-L189
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
---

# Signature

`fn inbox_contents_row_invariant_property_value( email: &JmapEmail, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [email_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)

# Called by

- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)