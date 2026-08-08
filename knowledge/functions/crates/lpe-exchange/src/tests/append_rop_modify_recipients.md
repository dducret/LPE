---
type: Rust Function
title: append_rop_modify_recipients
resource: crates/lpe-exchange/src/tests/mod.rs#L15324-L15331
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_rop_modify_recipients_with_columns
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_pending_message_display_recipients_follow_modify_recipients
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_create_message_rejects_recipients
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/submit_new_mapi_message_with_identities
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_uses_canonical_submission
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_replayed_execute_request_id_does_not_resubmit_message
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_trash_collector_import_then_save
  - functions/crates/lpe-exchange/src/tests/mapi_submit_execute_body
---

# Signature

`fn append_rop_modify_recipients(rops: &mut Vec<u8>, input: u8, rows: &[(u32, u8, &[u8])])`

# Calls

- [append_rop_modify_recipients_with_columns](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_modify_recipients_with_columns.md)

# Called by

- [mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end.md)
- [mapi_over_http_pending_message_display_recipients_follow_modify_recipients](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_pending_message_display_recipients_follow_modify_recipients.md)
- [mapi_over_http_public_folder_create_message_rejects_recipients](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_create_message_rejects_recipients.md)
- [submit_new_mapi_message_with_identities](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/submit_new_mapi_message_with_identities.md)
- [mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically.md)
- [mapi_over_http_transport_send_uses_canonical_submission](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_uses_canonical_submission.md)
- [mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move.md)
- [mapi_over_http_replayed_execute_request_id_does_not_resubmit_message](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_replayed_execute_request_id_does_not_resubmit_message.md)
- [mapi_over_http_replays_outlook_trash_collector_import_then_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_trash_collector_import_then_save.md)
- [mapi_submit_execute_body](../../../../../functions/crates/lpe-exchange/src/tests/mapi_submit_execute_body.md)