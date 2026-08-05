---
type: Rust Function
title: load_message_from_path
resource: LPE-CT/src/smtp/queue_store.rs#L67-L69
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/LPE-CT/src/smtp/reindex_quarantine_spool
  - functions/LPE-CT/src/smtp/quarantine/list_quarantine_items_from_spool
  - functions/LPE-CT/src/smtp/queue_store/find_message
---

# Signature

`pub(in crate::smtp) fn load_message_from_path(path: &Path) -> Result<QueuedMessage>`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [reindex_quarantine_spool](../../../../../functions/LPE-CT/src/smtp/reindex_quarantine_spool.md)
- [list_quarantine_items_from_spool](../../../../../functions/LPE-CT/src/smtp/quarantine/list_quarantine_items_from_spool.md)
- [find_message](../../../../../functions/LPE-CT/src/smtp/queue_store/find_message.md)