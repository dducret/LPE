---
type: Rust Function
title: format_visible_inbox_release_request_metrics
resource: crates/lpe-exchange/src/mapi/dispatch/release.rs#L556-L602
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/classify_release_request_shape
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
---

# Signature

`fn format_visible_inbox_release_request_metrics( request: &RopRequest, request_rop_names: &str, handle_slots: &[u32], released_handle: Option<u32>, same_execute_released_handles: &HashSet<u32>, session: &MapiSession, ) -> String`

# Calls

- [classify_release_request_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/classify_release_request_shape.md)

# Called by

- [append_release_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)