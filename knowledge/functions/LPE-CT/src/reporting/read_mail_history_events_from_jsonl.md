---
type: Rust Function
title: read_mail_history_events_from_jsonl
resource: LPE-CT/src/reporting.rs#L487-L509
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/reporting/read_mail_history_events
---

# Signature

`fn read_mail_history_events_from_jsonl( spool_dir: &Path, retention_days: u32, ) -> Result<Vec<MailHistoryEvent>>`

# Calls

- [from_str](../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [read_mail_history_events](../../../../functions/LPE-CT/src/reporting/read_mail_history_events.md)