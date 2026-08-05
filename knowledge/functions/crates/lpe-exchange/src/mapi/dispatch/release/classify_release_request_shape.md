---
type: Rust Function
title: classify_release_request_shape
resource: crates/lpe-exchange/src/mapi/dispatch/release.rs#L611-L627
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/format_visible_inbox_release_request_metrics
---

# Signature

`fn classify_release_request_shape( request_rop_names: &str, rop_count: usize, release_rop_count: usize, ) -> &'static str`

# Called by

- [format_visible_inbox_release_request_metrics](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/format_visible_inbox_release_request_metrics.md)