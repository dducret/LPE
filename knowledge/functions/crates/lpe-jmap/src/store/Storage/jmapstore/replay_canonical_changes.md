---
type: Rust Method
title: replay_canonical_changes
resource: crates/lpe-jmap/src/store.rs#L624-L633
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn replay_canonical_changes( &self, principal_account_id: Uuid, after_cursor: i64, categories: &[CanonicalChangeCategory], max_rows: u64, ) -> Result<CanonicalChangeReplay>`