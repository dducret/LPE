---
type: Rust Function
title: hierarchy_property_filter_mode
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L869-L880
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_microsoft_payload_comparison
---

# Signature

`fn hierarchy_property_filter_mode( sync_flags: u16, requested_property_tags: &[u32], ) -> &'static str`

# Called by

- [hierarchy_microsoft_payload_comparison](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_microsoft_payload_comparison.md)