---
type: Rust Function
title: prune_retained_rows_from_db
resource: LPE-CT/src/reporting.rs#L1429-L1441
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/LPE-CT/src/reporting/history_cutoff
  called_by:
  - functions/LPE-CT/src/reporting/enforce_retention
---

# Signature

`async fn prune_retained_rows_from_db( config: &RuntimeConfig, settings: &ReportingSettings, ) -> Result<()>`

# Calls

- [query](../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [history_cutoff](../../../../functions/LPE-CT/src/reporting/history_cutoff.md)

# Called by

- [enforce_retention](../../../../functions/LPE-CT/src/reporting/enforce_retention.md)