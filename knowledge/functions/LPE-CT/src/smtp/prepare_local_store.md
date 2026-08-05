---
type: Rust Function
title: prepare_local_store
resource: LPE-CT/src/smtp.rs#L429-L435
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/reindex_quarantine_spool
  called_by:
  - functions/LPE-CT/src/main
---

# Signature

`pub(crate) async fn prepare_local_store(spool_dir: &Path, config: &RuntimeConfig) -> Result<()>`

# Calls

- [reindex_quarantine_spool](../../../../functions/LPE-CT/src/smtp/reindex_quarantine_spool.md)

# Called by

- [main](../../../../functions/LPE-CT/src/main.md)