---
type: Rust Function
title: ha_current_role
resource: crates/lpe-admin-api/src/readiness.rs#L156-L158
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/integration/deliver_inbound_message
  - functions/crates/lpe-admin-api/src/integration/verify_lpe_ct_recipient
  - functions/crates/lpe-admin-api/src/integration/accept_smtp_submission
  - functions/crates/lpe-cli/src/run_outbound_worker
---

# Signature

`pub fn ha_current_role() -> anyhow::Result<Option<String>>`

# Called by

- [deliver_inbound_message](../../../../../functions/crates/lpe-admin-api/src/integration/deliver_inbound_message.md)
- [verify_lpe_ct_recipient](../../../../../functions/crates/lpe-admin-api/src/integration/verify_lpe_ct_recipient.md)
- [accept_smtp_submission](../../../../../functions/crates/lpe-admin-api/src/integration/accept_smtp_submission.md)
- [run_outbound_worker](../../../../../functions/crates/lpe-cli/src/run_outbound_worker.md)