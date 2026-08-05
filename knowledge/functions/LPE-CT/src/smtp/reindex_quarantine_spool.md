---
type: Rust Function
title: reindex_quarantine_spool
resource: LPE-CT/src/smtp.rs#L443-L458
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/queue_store/load_message_from_path
  - functions/LPE-CT/src/smtp/quarantine/persist_quarantine_metadata
  called_by:
  - functions/LPE-CT/src/smtp/prepare_local_store
---

# Signature

`async fn reindex_quarantine_spool(spool_dir: &Path, config: &RuntimeConfig) -> Result<()>`

# Calls

- [load_message_from_path](../../../../functions/LPE-CT/src/smtp/queue_store/load_message_from_path.md)
- [persist_quarantine_metadata](../../../../functions/LPE-CT/src/smtp/quarantine/persist_quarantine_metadata.md)

# Called by

- [prepare_local_store](../../../../functions/LPE-CT/src/smtp/prepare_local_store.md)