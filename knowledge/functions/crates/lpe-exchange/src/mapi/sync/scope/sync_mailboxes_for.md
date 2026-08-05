---
type: Rust Function
title: sync_mailboxes_for
resource: crates/lpe-exchange/src/mapi/sync/scope.rs#L75-L81
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_state_mailboxes_for
  - functions/crates/lpe-exchange/src/mapi/sync/tests/hierarchy_sync_mailboxes_deduplicate_fixed_special_folder_ids
  - functions/crates/lpe-exchange/src/mapi/sync/tests/hierarchy_sync_mailboxes_include_custom_sync_root
  - functions/crates/lpe-exchange/src/mapi/sync/tests/hierarchy_sync_mailboxes_deduplicate_outlook_special_roles
  - functions/crates/lpe-exchange/src/mapi/sync/tests/ipm_hierarchy_state_matches_emitted_folder_projection
  - functions/crates/lpe-exchange/src/mapi/tables/tests/ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders
---

# Signature

`pub(in crate::mapi) fn sync_mailboxes_for( folder_id: u64, sync_type: u8, mailboxes: &[JmapMailbox], ) -> Vec<JmapMailbox>`

# Calls

- [sync_mailboxes_for_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted.md)

# Called by

- [sync_state_mailboxes_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_state_mailboxes_for.md)
- [hierarchy_sync_mailboxes_deduplicate_fixed_special_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/hierarchy_sync_mailboxes_deduplicate_fixed_special_folder_ids.md)
- [hierarchy_sync_mailboxes_include_custom_sync_root](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/hierarchy_sync_mailboxes_include_custom_sync_root.md)
- [hierarchy_sync_mailboxes_deduplicate_outlook_special_roles](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/hierarchy_sync_mailboxes_deduplicate_outlook_special_roles.md)
- [ipm_hierarchy_state_matches_emitted_folder_projection](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/ipm_hierarchy_state_matches_emitted_folder_projection.md)
- [ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders.md)