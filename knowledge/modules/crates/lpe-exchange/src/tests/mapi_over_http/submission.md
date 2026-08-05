---
type: Rust Module
title: submission
resource: crates/lpe-exchange/src/tests/mapi_over_http/submission.rs#L1-L2454
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [mapi_over_http_microsoft_subrestriction_matches_message_recipients](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_subrestriction_matches_message_recipients.md)
- [mapi_over_http_microsoft_modify_recipients_accepts_type_flags_and_rejects_invalid_type](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_accepts_type_flags_and_rejects_invalid_type.md)
- [mapi_over_http_modify_recipients_string8_rows_save_canonically](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_string8_rows_save_canonically.md)
- [mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically.md)
- [mapi_over_http_modify_recipients_x500_rows_save_canonically](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_x500_rows_save_canonically.md)
- [mapi_over_http_microsoft_modify_recipients_example_saves_canonically](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_example_saves_canonically.md)
- [mapi_over_http_remove_all_recipients_clears_pending_message_recipients](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_remove_all_recipients_clears_pending_message_recipients.md)
- [mapi_over_http_microsoft_remove_all_recipients_stages_on_open_message_until_save](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_remove_all_recipients_stages_on_open_message_until_save.md)
- [mapi_over_http_microsoft_modify_recipients_stages_on_open_message_until_save](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_stages_on_open_message_until_save.md)
- [mapi_over_http_submit_pending_message_uses_canonical_submission](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_submit_pending_message_uses_canonical_submission.md)
- [mapi_over_http_transport_send_uses_canonical_submission](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_uses_canonical_submission.md)
- [mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move.md)
- [mapi_over_http_transport_send_opened_draft_preserves_canonical_attachment_and_bcc_guards](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_opened_draft_preserves_canonical_attachment_and_bcc_guards.md)
- [mapi_over_http_transport_send_opened_outbox_message_uses_canonical_submission](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_opened_outbox_message_uses_canonical_submission.md)
- [mapi_over_http_replayed_execute_request_id_does_not_resubmit_message](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_replayed_execute_request_id_does_not_resubmit_message.md)
- [mapi_over_http_duplicate_execute_request_id_with_different_body_does_not_resubmit_message](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_duplicate_execute_request_id_with_different_body_does_not_resubmit_message.md)
- [mapi_over_http_submit_opened_draft_uses_source_draft_id](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_submit_opened_draft_uses_source_draft_id.md)
- [mapi_over_http_open_message_returns_visible_recipient_rows](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_open_message_returns_visible_recipient_rows.md)
- [mapi_over_http_read_recipients_returns_canonical_message_recipients](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_read_recipients_returns_canonical_message_recipients.md)
- [mapi_over_http_microsoft_read_recipients_rejects_nonzero_reserved_field](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_read_recipients_rejects_nonzero_reserved_field.md)
- [mapi_over_http_read_recipients_hides_sent_message_bcc_by_default](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_read_recipients_hides_sent_message_bcc_by_default.md)
- [mapi_over_http_execute_returns_transport_folder_without_protocol_outbox_state](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_execute_returns_transport_folder_without_protocol_outbox_state.md)
- [mapi_over_http_microsoft_transport_spooler_rops_keep_batch_aligned_without_mutation](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_transport_spooler_rops_keep_batch_aligned_without_mutation.md)
- [mapi_over_http_microsoft_abort_submit_cancels_pre_handoff_submission](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_abort_submit_cancels_pre_handoff_submission.md)
- [mapi_over_http_microsoft_abort_submit_rejects_handed_off_and_terminal_submissions](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_abort_submit_rejects_handed_off_and_terminal_submissions.md)

# Imports

- `super::*`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)