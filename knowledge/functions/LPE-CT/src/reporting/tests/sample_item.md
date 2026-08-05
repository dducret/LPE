---
type: Rust Function
title: sample_item
resource: LPE-CT/src/reporting/tests.rs#L24-L49
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/reporting/tests/digest_report_enriches_status_and_domain_counts_and_persists_artifact
---

# Signature

`fn sample_item(trace_id: &str, mail_from: &str, rcpt_to: &[&str]) -> QuarantineSummary`

# Called by

- [digest_report_enriches_status_and_domain_counts_and_persists_artifact](../../../../../functions/LPE-CT/src/reporting/tests/digest_report_enriches_status_and_domain_counts_and_persists_artifact.md)