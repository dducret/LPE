---
type: Rust Method
title: compute_push_changes
resource: crates/lpe-jmap/src/websocket.rs#L382-L533
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/contains_category
  - functions/crates/lpe-jmap/src/websocket/JmapService/is_mail_push_type
  - functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/accounts_for
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state
  - functions/crates/lpe-jmap/src/state/entry
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state
  called_by:
  - functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source
  - functions/crates/lpe-jmap/src/tests/scoped_push_change_is_stable_for_noop_mail_notifications
  - functions/crates/lpe-jmap/src/tests/scoped_push_change_wakes_principal_when_shared_mailbox_visibility_changes
  - functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_delegated_mailbox_right_changes
  - functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_delegated_identity_right_changes
  - functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_email_delivery_for_new_messages_only_state
  - functions/crates/lpe-jmap/src/tests/scoped_push_change_limits_recompute_to_requested_categories
  - functions/crates/lpe-jmap/src/tests/websocket_push_tracks_private_outlook_note_and_journal_types
  - functions/crates/lpe-jmap/src/tests/shared_task_push_change_wakes_grantee_principal
  - functions/crates/lpe-jmap/src/tests/shared_task_list_rights_push_change_wakes_grantee_principal
  - functions/crates/lpe-jmap/src/tests/deleted_shared_task_list_push_change_wakes_former_grantee_principal
  - functions/crates/lpe-jmap/src/tests/benchmark_mailbox_listing_and_push_paths
  - functions/crates/lpe-jmap/src/websocket/JmapService/publish_state_changes
  - functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change
---

# Signature

`pub(crate) async fn compute_push_changes( &self, principal_account_id: Uuid, subscription: &PushSubscription, change_set: &CanonicalPushChangeSet, ) -> Result<( HashMap<String, HashMap<String, String>>, HashMap<String, HashMap<String, String>>, )>`

# Calls

- [contains_category](../../../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/contains_category.md)
- [is_mail_push_type](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/is_mail_push_type.md)
- [accounts_for](../../../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/accounts_for.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [mail_push_type_state](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state.md)
- [entry](../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [remove](../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state.md)

# Called by

- [handle_event_source](../../../../../../functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source.md)
- [scoped_push_change_is_stable_for_noop_mail_notifications](../../../../../../functions/crates/lpe-jmap/src/tests/scoped_push_change_is_stable_for_noop_mail_notifications.md)
- [scoped_push_change_wakes_principal_when_shared_mailbox_visibility_changes](../../../../../../functions/crates/lpe-jmap/src/tests/scoped_push_change_wakes_principal_when_shared_mailbox_visibility_changes.md)
- [scoped_push_change_reports_delegated_mailbox_right_changes](../../../../../../functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_delegated_mailbox_right_changes.md)
- [scoped_push_change_reports_delegated_identity_right_changes](../../../../../../functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_delegated_identity_right_changes.md)
- [scoped_push_change_reports_email_delivery_for_new_messages_only_state](../../../../../../functions/crates/lpe-jmap/src/tests/scoped_push_change_reports_email_delivery_for_new_messages_only_state.md)
- [scoped_push_change_limits_recompute_to_requested_categories](../../../../../../functions/crates/lpe-jmap/src/tests/scoped_push_change_limits_recompute_to_requested_categories.md)
- [websocket_push_tracks_private_outlook_note_and_journal_types](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_push_tracks_private_outlook_note_and_journal_types.md)
- [shared_task_push_change_wakes_grantee_principal](../../../../../../functions/crates/lpe-jmap/src/tests/shared_task_push_change_wakes_grantee_principal.md)
- [shared_task_list_rights_push_change_wakes_grantee_principal](../../../../../../functions/crates/lpe-jmap/src/tests/shared_task_list_rights_push_change_wakes_grantee_principal.md)
- [deleted_shared_task_list_push_change_wakes_former_grantee_principal](../../../../../../functions/crates/lpe-jmap/src/tests/deleted_shared_task_list_push_change_wakes_former_grantee_principal.md)
- [benchmark_mailbox_listing_and_push_paths](../../../../../../functions/crates/lpe-jmap/src/tests/benchmark_mailbox_listing_and_push_paths.md)
- [publish_state_changes](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/publish_state_changes.md)
- [recover_push_enable_change](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change.md)