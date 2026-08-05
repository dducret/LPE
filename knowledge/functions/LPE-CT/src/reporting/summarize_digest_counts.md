---
type: Rust Function
title: summarize_digest_counts
resource: LPE-CT/src/reporting.rs#L1327-L1344
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/LPE-CT/src/reporting/build_digest_report
  - functions/LPE-CT/src/reporting/render_digest_content
  - functions/LPE-CT/src/reporting/enrich_digest_detail
---

# Signature

`fn summarize_digest_counts(values: Vec<String>, limit: usize) -> Vec<DigestMetricCount>`

# Calls

- [entry](../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [build_digest_report](../../../../functions/LPE-CT/src/reporting/build_digest_report.md)
- [render_digest_content](../../../../functions/LPE-CT/src/reporting/render_digest_content.md)
- [enrich_digest_detail](../../../../functions/LPE-CT/src/reporting/enrich_digest_detail.md)