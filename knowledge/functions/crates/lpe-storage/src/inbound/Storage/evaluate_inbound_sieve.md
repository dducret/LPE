---
type: Rust Method
title: evaluate_inbound_sieve
resource: crates/lpe-storage/src/inbound.rs#L255-L286
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/evaluate_script
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message
---

# Signature

`async fn evaluate_inbound_sieve( &self, account_id: Uuid, envelope_from: &str, envelope_to: &str, headers: &std::collections::HashMap<String, String>, account_email: &str, ) -> Result<SieveExecutionOutcome>`

# Calls

- [evaluate_script](../../../../../../functions/crates/lpe-core/src/sieve/evaluate_script.md)

# Called by

- [deliver_inbound_message](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)