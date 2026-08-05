---
type: Rust Function
title: quarantine_summary_from_message
resource: LPE-CT/src/smtp/trace.rs#L3-L33
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/parse_rfc822_header_value
  - functions/LPE-CT/src/smtp/trace/latest_decision_summary
  called_by:
  - functions/LPE-CT/src/smtp/quarantine/list_quarantine_items_from_spool
---

# Signature

`pub(in crate::smtp) fn quarantine_summary_from_message( message: &QueuedMessage, ) -> QuarantineSummary`

# Calls

- [parse_rfc822_header_value](../../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_header_value.md)
- [latest_decision_summary](../../../../../functions/LPE-CT/src/smtp/trace/latest_decision_summary.md)

# Called by

- [list_quarantine_items_from_spool](../../../../../functions/LPE-CT/src/smtp/quarantine/list_quarantine_items_from_spool.md)