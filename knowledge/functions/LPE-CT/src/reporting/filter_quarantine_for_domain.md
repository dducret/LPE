---
type: Rust Function
title: filter_quarantine_for_domain
resource: LPE-CT/src/reporting.rs#L1138-L1165
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/reporting/run_digest_generation
  - functions/LPE-CT/src/reporting/tests/domain_filter_matches_sender_and_recipient_domains
---

# Signature

`fn filter_quarantine_for_domain( items: &[QuarantineSummary], domain: &str, max_items: u32, ) -> Vec<QuarantineSummary>`

# Called by

- [run_digest_generation](../../../../functions/LPE-CT/src/reporting/run_digest_generation.md)
- [domain_filter_matches_sender_and_recipient_domains](../../../../functions/LPE-CT/src/reporting/tests/domain_filter_matches_sender_and_recipient_domains.md)