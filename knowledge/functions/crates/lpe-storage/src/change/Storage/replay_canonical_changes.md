---
type: Rust Method
title: replay_canonical_changes
resource: crates/lpe-storage/src/change.rs#L591-L665
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/change/cursor_is_before_retained_floor
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/insert_accounts
  - functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/set_journal_cursor
---

# Signature

`pub async fn replay_canonical_changes( &self, principal_account_id: Uuid, after_cursor: i64, categories: &[CanonicalChangeCategory], max_rows: u64, ) -> Result<CanonicalChangeReplay>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [cursor_is_before_retained_floor](../../../../../../functions/crates/lpe-storage/src/change/cursor_is_before_retained_floor.md)
- [from_str](../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [insert_accounts](../../../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/insert_accounts.md)
- [set_journal_cursor](../../../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/set_journal_cursor.md)