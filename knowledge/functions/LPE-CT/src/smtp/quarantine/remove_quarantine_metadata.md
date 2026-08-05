---
type: Rust Function
title: remove_quarantine_metadata
resource: LPE-CT/src/smtp/quarantine.rs#L326-L335
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/LPE-CT/src/smtp/quarantine/remove_quarantine_metadata_or_warn
---

# Signature

`async fn remove_quarantine_metadata(config: &RuntimeConfig, trace_id: &str) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [remove_quarantine_metadata_or_warn](../../../../../functions/LPE-CT/src/smtp/quarantine/remove_quarantine_metadata_or_warn.md)