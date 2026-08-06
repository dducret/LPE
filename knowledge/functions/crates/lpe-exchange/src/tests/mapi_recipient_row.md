---
type: Rust Function
title: mapi_recipient_row
resource: crates/lpe-exchange/src/tests/mod.rs#L15680-L15686
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_reload_cached_information_returns_pending_message_summary
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_accepts_type_flags_and_rejects_invalid_type
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_remove_all_recipients_clears_pending_message_recipients
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_stages_on_open_message_until_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_submit_pending_message_uses_canonical_submission
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_uses_canonical_submission
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_replayed_execute_request_id_does_not_resubmit_message
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_modify_recipients_imports_pending_message_recipients
  - functions/crates/lpe-exchange/src/tests/mapi_submit_execute_body
---

# Signature

`fn mapi_recipient_row(display_name: &str, address: &str, recipient_type: u8) -> Vec<u8>`

# Called by

- [mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end.md)
- [mapi_over_http_reload_cached_information_returns_pending_message_summary](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_reload_cached_information_returns_pending_message_summary.md)
- [mapi_over_http_microsoft_modify_recipients_accepts_type_flags_and_rejects_invalid_type](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_accepts_type_flags_and_rejects_invalid_type.md)
- [mapi_over_http_remove_all_recipients_clears_pending_message_recipients](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_remove_all_recipients_clears_pending_message_recipients.md)
- [mapi_over_http_microsoft_modify_recipients_stages_on_open_message_until_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_stages_on_open_message_until_save.md)
- [mapi_over_http_submit_pending_message_uses_canonical_submission](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_submit_pending_message_uses_canonical_submission.md)
- [mapi_over_http_transport_send_uses_canonical_submission](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_uses_canonical_submission.md)
- [mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move.md)
- [mapi_over_http_replayed_execute_request_id_does_not_resubmit_message](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_replayed_execute_request_id_does_not_resubmit_message.md)
- [mapi_over_http_modify_recipients_imports_pending_message_recipients](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_modify_recipients_imports_pending_message_recipients.md)
- [mapi_submit_execute_body](../../../../../functions/crates/lpe-exchange/src/tests/mapi_submit_execute_body.md)