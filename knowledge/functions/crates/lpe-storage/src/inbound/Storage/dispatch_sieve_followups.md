---
type: Rust Method
title: dispatch_sieve_followups
resource: crates/lpe-storage/src/inbound.rs#L288-L389
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/inbound/estimate_generated_message_size
  - functions/crates/lpe-storage/src/inbound/Storage/should_send_sieve_vacation
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message
---

# Signature

`async fn dispatch_sieve_followups(&self, followup: &SieveFollowUp) -> Result<()>`

# Calls

- [estimate_generated_message_size](../../../../../../functions/crates/lpe-storage/src/inbound/estimate_generated_message_size.md)
- [should_send_sieve_vacation](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/should_send_sieve_vacation.md)

# Called by

- [deliver_inbound_message](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)