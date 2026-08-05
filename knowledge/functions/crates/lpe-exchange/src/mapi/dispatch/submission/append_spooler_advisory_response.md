---
type: Rust Function
title: append_spooler_advisory_response
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L304-L310
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/spooler_advisory_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_spooler_advisory_dispatch_response
---

# Signature

`pub(super) fn append_spooler_advisory_response( request: &RopRequest, has_input_handle: bool, responses: &mut Vec<u8>, )`

# Calls

- [spooler_advisory_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/spooler_advisory_response.md)

# Called by

- [append_spooler_advisory_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_spooler_advisory_dispatch_response.md)