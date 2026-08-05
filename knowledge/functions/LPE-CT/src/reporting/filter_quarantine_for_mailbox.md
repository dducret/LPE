---
type: Rust Function
title: filter_quarantine_for_mailbox
resource: LPE-CT/src/reporting.rs#L1167-L1187
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/reporting/run_digest_generation
  - functions/LPE-CT/src/reporting/tests/mailbox_filter_matches_sender_and_recipient_mailboxes
---

# Signature

`fn filter_quarantine_for_mailbox( items: &[QuarantineSummary], mailbox: &str, max_items: u32, ) -> Vec<QuarantineSummary>`

# Called by

- [run_digest_generation](../../../../functions/LPE-CT/src/reporting/run_digest_generation.md)
- [mailbox_filter_matches_sender_and_recipient_mailboxes](../../../../functions/LPE-CT/src/reporting/tests/mailbox_filter_matches_sender_and_recipient_mailboxes.md)