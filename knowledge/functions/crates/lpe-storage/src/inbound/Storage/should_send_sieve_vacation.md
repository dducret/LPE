---
type: Rust Method
title: should_send_sieve_vacation
resource: crates/lpe-storage/src/inbound.rs#L391-L422
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/inbound/hash_sieve_vacation_key
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/dispatch_sieve_followups
---

# Signature

`async fn should_send_sieve_vacation( &self, account_id: Uuid, sender_address: &str, vacation: &VacationAction, ) -> Result<bool>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [hash_sieve_vacation_key](../../../../../../functions/crates/lpe-storage/src/inbound/hash_sieve_vacation_key.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [dispatch_sieve_followups](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/dispatch_sieve_followups.md)