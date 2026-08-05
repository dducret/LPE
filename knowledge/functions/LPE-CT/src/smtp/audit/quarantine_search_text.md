---
type: Rust Function
title: quarantine_search_text
resource: LPE-CT/src/smtp/audit.rs#L310-L351
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/parse_rfc822_header_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/smtp/trace/latest_decision_summary
  called_by:
  - functions/LPE-CT/src/smtp/quarantine/quarantine_metadata
---

# Signature

`pub(in crate::smtp) fn quarantine_search_text(message: &QueuedMessage) -> String`

# Calls

- [parse_rfc822_header_value](../../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_header_value.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [latest_decision_summary](../../../../../functions/LPE-CT/src/smtp/trace/latest_decision_summary.md)

# Called by

- [quarantine_metadata](../../../../../functions/LPE-CT/src/smtp/quarantine/quarantine_metadata.md)