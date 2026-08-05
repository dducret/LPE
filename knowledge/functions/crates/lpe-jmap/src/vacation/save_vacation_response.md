---
type: Rust Function
title: save_vacation_response
resource: crates/lpe-jmap/src/vacation.rs#L207-L269
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/vacation/vacation_audit
  called_by:
  - functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set
---

# Signature

`async fn save_vacation_response<S: crate::store::JmapStore, V: lpe_magika::Detector>( service: &JmapService<S, V>, account_id: uuid::Uuid, value: &Value, existing: &VacationResponseProjection, account: &AuthenticatedAccount, ) -> Result<VacationResponseProjection>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [vacation_audit](../../../../../functions/crates/lpe-jmap/src/vacation/vacation_audit.md)

# Called by

- [handle_vacation_response_set](../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set.md)