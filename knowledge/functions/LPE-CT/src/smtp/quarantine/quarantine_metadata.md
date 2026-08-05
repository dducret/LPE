---
type: Rust Function
title: quarantine_metadata
resource: LPE-CT/src/smtp/quarantine.rs#L185-L225
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/parse_rfc822_header_value
  - functions/LPE-CT/src/smtp/queue_store/spool_path
  - functions/LPE-CT/src/smtp/audit/quarantine_search_text
  called_by:
  - functions/LPE-CT/src/smtp/quarantine/persist_quarantine_metadata
---

# Signature

`fn quarantine_metadata(spool_dir: &Path, message: &QueuedMessage) -> QuarantineMetadata`

# Calls

- [parse_rfc822_header_value](../../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_header_value.md)
- [spool_path](../../../../../functions/LPE-CT/src/smtp/queue_store/spool_path.md)
- [quarantine_search_text](../../../../../functions/LPE-CT/src/smtp/audit/quarantine_search_text.md)

# Called by

- [persist_quarantine_metadata](../../../../../functions/LPE-CT/src/smtp/quarantine/persist_quarantine_metadata.md)