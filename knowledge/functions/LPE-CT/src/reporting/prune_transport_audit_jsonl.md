---
type: Rust Function
title: prune_transport_audit_jsonl
resource: LPE-CT/src/reporting.rs#L1384-L1407
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/history_cutoff
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/LPE-CT/src/reporting/enforce_retention
---

# Signature

`fn prune_transport_audit_jsonl(spool_dir: &Path, retention_days: u32) -> Result<()>`

# Calls

- [history_cutoff](../../../../functions/LPE-CT/src/reporting/history_cutoff.md)
- [from_str](../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [enforce_retention](../../../../functions/LPE-CT/src/reporting/enforce_retention.md)