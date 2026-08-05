---
type: Rust Function
title: enrich_digest_detail
resource: LPE-CT/src/reporting.rs#L1357-L1382
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

`fn enrich_digest_detail(mut detail: DigestReportDetails) -> DigestReportDetails`

# Calls

- [summarize_digest_counts](../../../../functions/LPE-CT/src/reporting/summarize_digest_counts.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [build_digest_report](../../../../functions/LPE-CT/src/reporting/build_digest_report.md)