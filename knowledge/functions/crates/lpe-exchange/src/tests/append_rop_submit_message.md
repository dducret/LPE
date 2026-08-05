---
type: Rust Function
title: append_rop_submit_message
resource: crates/lpe-exchange/src/tests/mod.rs#L15443-L15445
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_replayed_execute_request_id_does_not_resubmit_message
  - functions/crates/lpe-exchange/src/tests/mapi_submit_execute_body
---

# Signature

`fn append_rop_submit_message(rops: &mut Vec<u8>, input: u8)`

# Called by

- [mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end.md)
- [mapi_over_http_replayed_execute_request_id_does_not_resubmit_message](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_replayed_execute_request_id_does_not_resubmit_message.md)
- [mapi_submit_execute_body](../../../../../functions/crates/lpe-exchange/src/tests/mapi_submit_execute_body.md)