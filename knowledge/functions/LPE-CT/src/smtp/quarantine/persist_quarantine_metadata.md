---
type: Rust Function
title: persist_quarantine_metadata
resource: LPE-CT/src/smtp/quarantine.rs#L227-L310
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/quarantine/quarantine_metadata
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/LPE-CT/src/smtp/reindex_quarantine_spool
  - functions/LPE-CT/src/smtp/quarantine/persist_quarantine_metadata_or_warn
---

# Signature

`pub(in crate::smtp) async fn persist_quarantine_metadata( spool_dir: &Path, config: &RuntimeConfig, message: &QueuedMessage, ) -> Result<()>`

# Calls

- [quarantine_metadata](../../../../../functions/LPE-CT/src/smtp/quarantine/quarantine_metadata.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [reindex_quarantine_spool](../../../../../functions/LPE-CT/src/smtp/reindex_quarantine_spool.md)
- [persist_quarantine_metadata_or_warn](../../../../../functions/LPE-CT/src/smtp/quarantine/persist_quarantine_metadata_or_warn.md)