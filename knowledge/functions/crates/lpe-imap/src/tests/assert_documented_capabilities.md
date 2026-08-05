---
type: Rust Function
title: assert_documented_capabilities
resource: crates/lpe-imap/src/tests.rs#L31-L72
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-imap/src/tests/login_list_select_fetch_store_search_and_append_work
  - functions/crates/lpe-imap/src/tests/utf8_accept_enables_utf8_mailbox_response_quoting
  - functions/crates/lpe-imap/src/tests/outlook_first_login_list_select_sync_transcript
  - functions/crates/lpe-imap/src/tests/thunderbird_copy_to_trash_then_expunge_removes_source_only
  - functions/crates/lpe-imap/src/tests/thunderbird_delete_draft_by_move_to_trash_removes_drafts_copy
  - functions/crates/lpe-imap/src/tests/outlook_large_mailbox_refresh_keeps_uid_fetch_and_search_stable
  - functions/crates/lpe-imap/src/tests/condstore_rejects_invalid_tokens_and_keeps_qresync_unadvertised
  - functions/crates/lpe-imap/src/tests/reconnect_select_refreshes_from_canonical_mailbox_state
  - functions/crates/lpe-imap/src/tests/acl_commands_project_canonical_mailbox_and_sender_delegation
---

# Signature

`fn assert_documented_capabilities(capability: &str)`

# Calls

- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [login_list_select_fetch_store_search_and_append_work](../../../../../functions/crates/lpe-imap/src/tests/login_list_select_fetch_store_search_and_append_work.md)
- [utf8_accept_enables_utf8_mailbox_response_quoting](../../../../../functions/crates/lpe-imap/src/tests/utf8_accept_enables_utf8_mailbox_response_quoting.md)
- [outlook_first_login_list_select_sync_transcript](../../../../../functions/crates/lpe-imap/src/tests/outlook_first_login_list_select_sync_transcript.md)
- [thunderbird_copy_to_trash_then_expunge_removes_source_only](../../../../../functions/crates/lpe-imap/src/tests/thunderbird_copy_to_trash_then_expunge_removes_source_only.md)
- [thunderbird_delete_draft_by_move_to_trash_removes_drafts_copy](../../../../../functions/crates/lpe-imap/src/tests/thunderbird_delete_draft_by_move_to_trash_removes_drafts_copy.md)
- [outlook_large_mailbox_refresh_keeps_uid_fetch_and_search_stable](../../../../../functions/crates/lpe-imap/src/tests/outlook_large_mailbox_refresh_keeps_uid_fetch_and_search_stable.md)
- [condstore_rejects_invalid_tokens_and_keeps_qresync_unadvertised](../../../../../functions/crates/lpe-imap/src/tests/condstore_rejects_invalid_tokens_and_keeps_qresync_unadvertised.md)
- [reconnect_select_refreshes_from_canonical_mailbox_state](../../../../../functions/crates/lpe-imap/src/tests/reconnect_select_refreshes_from_canonical_mailbox_state.md)
- [acl_commands_project_canonical_mailbox_and_sender_delegation](../../../../../functions/crates/lpe-imap/src/tests/acl_commands_project_canonical_mailbox_and_sender_delegation.md)