---
type: Rust Function
title: postgres_mapi_audit
resource: crates/lpe-exchange/src/tests/mod.rs#L179-L185
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_non_inbox_message_notification_allocates_message_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/create_notification_mailbox
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_mailbox_content_commit_time_tracks_canonical_mail_mutations
---

# Signature

`fn postgres_mapi_audit(action: &str, subject: Uuid) -> lpe_storage::AuditEntryInput`

# Called by

- [mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql.md)
- [mapi_non_inbox_message_notification_allocates_message_identity_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_non_inbox_message_notification_allocates_message_identity_in_postgresql.md)
- [create_notification_mailbox](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/create_notification_mailbox.md)
- [mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql.md)
- [mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql.md)
- [mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql.md)
- [postgres_mapi_mailbox_content_commit_time_tracks_canonical_mail_mutations](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_mailbox_content_commit_time_tracks_canonical_mail_mutations.md)