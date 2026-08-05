---
type: Rust Function
title: sync_state_mailboxes_for
resource: crates/lpe-exchange/src/mapi/sync/scope.rs#L130-L136
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/tests/ipm_hierarchy_state_matches_emitted_folder_projection
---

# Signature

`pub(in crate::mapi) fn sync_state_mailboxes_for( folder_id: u64, sync_type: u8, mailboxes: &[JmapMailbox], ) -> Vec<JmapMailbox>`

# Calls

- [sync_mailboxes_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for.md)

# Called by

- [ipm_hierarchy_state_matches_emitted_folder_projection](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/ipm_hierarchy_state_matches_emitted_folder_projection.md)