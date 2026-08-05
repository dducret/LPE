---
type: Rust Function
title: parse_sender_delegation_right
resource: crates/lpe-admin-api/src/util.rs#L15-L21
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/delegation/upsert_sender_delegation_grant
  - functions/crates/lpe-admin-api/src/delegation/delete_sender_delegation_grant
---

# Signature

`pub(crate) fn parse_sender_delegation_right(value: &str) -> Result<SenderDelegationRight, String>`

# Called by

- [upsert_sender_delegation_grant](../../../../../functions/crates/lpe-admin-api/src/delegation/upsert_sender_delegation_grant.md)
- [delete_sender_delegation_grant](../../../../../functions/crates/lpe-admin-api/src/delegation/delete_sender_delegation_grant.md)