---
type: Rust Method
title: verify_local_recipient
resource: crates/lpe-storage/src/inbound.rs#L35-L54
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/integration/verify_lpe_ct_recipient
---

# Signature

`pub async fn verify_local_recipient(&self, recipient: &str) -> Result<bool>`

# Called by

- [verify_lpe_ct_recipient](../../../../../../functions/crates/lpe-admin-api/src/integration/verify_lpe_ct_recipient.md)