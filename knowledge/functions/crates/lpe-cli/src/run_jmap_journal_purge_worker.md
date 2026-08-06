---
type: Rust Function
title: run_jmap_journal_purge_worker
resource: crates/lpe-cli/src/main.rs#L252-L290
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/readiness/ha_allows_active_work
  - functions/crates/lpe-storage/src/change/Storage/purge_canonical_change_journals
  called_by:
  - functions/crates/lpe-cli/src/main
---

# Signature

`async fn run_jmap_journal_purge_worker(storage: Storage) -> Result<()>`

# Calls

- [ha_allows_active_work](../../../../functions/crates/lpe-admin-api/src/readiness/ha_allows_active_work.md)
- [purge_canonical_change_journals](../../../../functions/crates/lpe-storage/src/change/Storage/purge_canonical_change_journals.md)

# Called by

- [main](../../../../functions/crates/lpe-cli/src/main.md)