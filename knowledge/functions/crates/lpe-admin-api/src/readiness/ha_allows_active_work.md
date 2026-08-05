---
type: Rust Function
title: ha_allows_active_work
resource: crates/lpe-admin-api/src/readiness.rs#L149-L154
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/integration/deliver_inbound_message
  - functions/crates/lpe-admin-api/src/integration/verify_lpe_ct_recipient
  - functions/crates/lpe-admin-api/src/integration/accept_smtp_submission
  - functions/crates/lpe-cli/src/run_outbound_worker
  - functions/crates/lpe-cli/src/run_jmap_journal_purge_worker
---

# Signature

`pub fn ha_allows_active_work() -> anyhow::Result<bool>`

# Called by

- [deliver_inbound_message](../../../../../functions/crates/lpe-admin-api/src/integration/deliver_inbound_message.md)
- [verify_lpe_ct_recipient](../../../../../functions/crates/lpe-admin-api/src/integration/verify_lpe_ct_recipient.md)
- [accept_smtp_submission](../../../../../functions/crates/lpe-admin-api/src/integration/accept_smtp_submission.md)
- [run_outbound_worker](../../../../../functions/crates/lpe-cli/src/run_outbound_worker.md)
- [run_jmap_journal_purge_worker](../../../../../functions/crates/lpe-cli/src/run_jmap_journal_purge_worker.md)