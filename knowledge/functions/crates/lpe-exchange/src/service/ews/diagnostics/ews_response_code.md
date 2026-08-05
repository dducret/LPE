---
type: Rust Function
title: ews_response_code
resource: crates/lpe-exchange/src/service/ews/diagnostics.rs#L82-L87
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/post_handler
---

# Signature

`pub(in crate::service) fn ews_response_code(response: &Response) -> Option<&str>`

# Called by

- [post_handler](../../../../../../../functions/crates/lpe-exchange/src/service/post_handler.md)