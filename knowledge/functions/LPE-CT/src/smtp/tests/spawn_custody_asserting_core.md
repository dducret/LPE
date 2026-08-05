---
type: Rust Function
title: spawn_custody_asserting_core
resource: LPE-CT/src/smtp/tests.rs#L3583-L3614
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/tests/inbound_delivery_keeps_durable_spool_custody_until_core_accepts
---

# Signature

`async fn spawn_custody_asserting_core( spool: PathBuf, observed_spool_custody: Arc<Mutex<bool>>, ) -> String`

# Called by

- [inbound_delivery_keeps_durable_spool_custody_until_core_accepts](../../../../../functions/LPE-CT/src/smtp/tests/inbound_delivery_keeps_durable_spool_custody_until_core_accepts.md)