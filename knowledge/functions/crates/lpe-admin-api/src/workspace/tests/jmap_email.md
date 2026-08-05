---
type: Rust Function
title: jmap_email
resource: crates/lpe-admin-api/src/workspace/tests.rs#L555-L626
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/tests/FakeSubmissionStore/super-clientsubmissionstore/update_jmap_email_followup_flags
  - functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientrecoverablestore/restore_recoverable_item
---

# Signature

`fn jmap_email(id: Uuid, account_id: Uuid, flagged: bool) -> JmapEmail`

# Called by

- [update_jmap_email_followup_flags](../../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeSubmissionStore/super-clientsubmissionstore/update_jmap_email_followup_flags.md)
- [restore_recoverable_item](../../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientrecoverablestore/restore_recoverable_item.md)