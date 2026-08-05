---
type: Rust Function
title: abort_submit_source_is_sent
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L237-L243
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_canonical_message_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_abort_submit_response
---

# Signature

`pub(super) fn abort_submit_source_is_sent(email: &JmapEmail) -> bool`

# Called by

- [abort_submit_canonical_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_canonical_message_id.md)
- [append_abort_submit_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_abort_submit_response.md)