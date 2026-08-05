---
type: Rust Function
title: render_digest_content
resource: LPE-CT/src/reporting.rs#L1059-L1136
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/summarize_digest_counts
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/reporting/build_digest_report
---

# Signature

`fn render_digest_content( generated_at: &str, scope: &str, scope_label: &str, recipient: &str, items: &[QuarantineSummary], ) -> String`

# Calls

- [summarize_digest_counts](../../../../functions/LPE-CT/src/reporting/summarize_digest_counts.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [build_digest_report](../../../../functions/LPE-CT/src/reporting/build_digest_report.md)