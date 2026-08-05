---
type: Rust Function
title: append_rop_transport_send
resource: crates/lpe-exchange/src/tests/mod.rs#L15431-L15433
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_uses_canonical_submission
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_opened_draft_preserves_canonical_attachment_and_bcc_guards
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_opened_outbox_message_uses_canonical_submission
---

# Signature

`fn append_rop_transport_send(rops: &mut Vec<u8>, input: u8)`

# Called by

- [mapi_over_http_transport_send_uses_canonical_submission](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_uses_canonical_submission.md)
- [mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move.md)
- [mapi_over_http_transport_send_opened_draft_preserves_canonical_attachment_and_bcc_guards](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_opened_draft_preserves_canonical_attachment_and_bcc_guards.md)
- [mapi_over_http_transport_send_opened_outbox_message_uses_canonical_submission](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_opened_outbox_message_uses_canonical_submission.md)