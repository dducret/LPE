---
type: Rust Function
title: hash_sieve_vacation_key
resource: crates/lpe-storage/src/inbound.rs#L775-L783
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/should_send_sieve_vacation
---

# Signature

`fn hash_sieve_vacation_key(vacation: &VacationAction) -> String`

# Called by

- [should_send_sieve_vacation](../../../../../functions/crates/lpe-storage/src/inbound/Storage/should_send_sieve_vacation.md)