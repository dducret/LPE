---
type: Rust Function
title: vacation_audit
resource: crates/lpe-jmap/src/vacation.rs#L287-L297
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set
  - functions/crates/lpe-jmap/src/vacation/save_vacation_response
---

# Signature

`fn vacation_audit( account: &AuthenticatedAccount, subject_account_id: uuid::Uuid, action: &str, ) -> AuditEntryInput`

# Called by

- [handle_vacation_response_set](../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set.md)
- [save_vacation_response](../../../../../functions/crates/lpe-jmap/src/vacation/save_vacation_response.md)