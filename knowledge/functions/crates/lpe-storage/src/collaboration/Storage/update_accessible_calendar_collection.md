---
type: Rust Method
title: update_accessible_calendar_collection
resource: crates/lpe-storage/src/collaboration.rs#L325-L403
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/Storage/resolve_collection_access
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change
---

# Signature

`pub async fn update_accessible_calendar_collection( &self, principal_account_id: Uuid, collection_id: &str, display_name: &str, ) -> Result<CollaborationCollection>`

# Calls

- [resolve_collection_access](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/resolve_collection_access.md)
- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_collaboration_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change.md)