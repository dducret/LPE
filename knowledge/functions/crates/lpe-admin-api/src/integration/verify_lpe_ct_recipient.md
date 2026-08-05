---
type: Rust Function
title: verify_lpe_ct_recipient
resource: crates/lpe-admin-api/src/integration.rs#L101-L134
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/integration/require_integration
  - functions/crates/lpe-admin-api/src/readiness/ha_allows_active_work
  - functions/crates/lpe-admin-api/src/readiness/ha_current_role
  - functions/crates/lpe-storage/src/inbound/Storage/verify_local_recipient
---

# Signature

`pub(crate) async fn verify_lpe_ct_recipient( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<RecipientVerificationRequest>, ) -> ApiResult<RecipientVerificationResponse>`

# Calls

- [require_integration](../../../../../functions/crates/lpe-admin-api/src/integration/require_integration.md)
- [ha_allows_active_work](../../../../../functions/crates/lpe-admin-api/src/readiness/ha_allows_active_work.md)
- [ha_current_role](../../../../../functions/crates/lpe-admin-api/src/readiness/ha_current_role.md)
- [verify_local_recipient](../../../../../functions/crates/lpe-storage/src/inbound/Storage/verify_local_recipient.md)