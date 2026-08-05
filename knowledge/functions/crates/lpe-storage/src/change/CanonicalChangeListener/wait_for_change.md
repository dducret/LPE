---
type: Rust Method
title: wait_for_change
resource: crates/lpe-storage/src/change.rs#L116-L163
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dkim_signing/payload
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/insert_accounts
  - functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/set_journal_cursor
---

# Signature

`pub async fn wait_for_change( &mut self, categories: &[CanonicalChangeCategory], ) -> Result<CanonicalPushChangeSet>`

# Calls

- [payload](../../../../../../functions/LPE-CT/src/dkim_signing/payload.md)
- [from_str](../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [insert_accounts](../../../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/insert_accounts.md)
- [set_journal_cursor](../../../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/set_journal_cursor.md)