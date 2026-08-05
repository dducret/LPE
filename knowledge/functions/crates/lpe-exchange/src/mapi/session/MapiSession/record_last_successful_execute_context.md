---
type: Rust Method
title: record_last_successful_execute_context
resource: crates/lpe-exchange/src/mapi/session.rs#L559-L569
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
---

# Signature

`pub(in crate::mapi) fn record_last_successful_execute_context( &mut self, context: String, has_non_release_rop: bool, )`

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)