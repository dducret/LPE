---
type: Rust Method
title: purge_canonical_change_journals
resource: crates/lpe-storage/src/change.rs#L667-L686
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-cli/src/run_jmap_journal_purge_worker
---

# Signature

`pub async fn purge_canonical_change_journals(&self) -> Result<u64>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [run_jmap_journal_purge_worker](../../../../../../functions/crates/lpe-cli/src/run_jmap_journal_purge_worker.md)