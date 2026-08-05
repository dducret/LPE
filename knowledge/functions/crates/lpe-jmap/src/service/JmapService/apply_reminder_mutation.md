---
type: Rust Method
title: apply_reminder_mutation
resource: crates/lpe-jmap/src/service.rs#L1032-L1139
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_reminder_set
---

# Signature

`async fn apply_reminder_mutation( &self, account: &AuthenticatedAccount, account_id: Uuid, value: &Value, default_set: bool, audit_subject: &str, ) -> Result<String>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_reminder_set](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_reminder_set.md)