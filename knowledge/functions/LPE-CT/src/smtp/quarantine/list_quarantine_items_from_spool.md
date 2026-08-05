---
type: Rust Function
title: list_quarantine_items_from_spool
resource: LPE-CT/src/smtp/quarantine.rs#L44-L61
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/queue_store/load_message_from_path
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/smtp/trace/quarantine_summary_from_message
  - functions/LPE-CT/src/smtp/trace/quarantine_matches
  called_by:
  - functions/LPE-CT/src/reporting/run_digest_generation
  - functions/LPE-CT/src/smtp/quarantine/list_quarantine_items
---

# Signature

`pub(crate) fn list_quarantine_items_from_spool( spool_dir: &Path, query: QuarantineQuery, ) -> Result<Vec<QuarantineSummary>>`

# Calls

- [load_message_from_path](../../../../../functions/LPE-CT/src/smtp/queue_store/load_message_from_path.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [quarantine_summary_from_message](../../../../../functions/LPE-CT/src/smtp/trace/quarantine_summary_from_message.md)
- [quarantine_matches](../../../../../functions/LPE-CT/src/smtp/trace/quarantine_matches.md)

# Called by

- [run_digest_generation](../../../../../functions/LPE-CT/src/reporting/run_digest_generation.md)
- [list_quarantine_items](../../../../../functions/LPE-CT/src/smtp/quarantine/list_quarantine_items.md)