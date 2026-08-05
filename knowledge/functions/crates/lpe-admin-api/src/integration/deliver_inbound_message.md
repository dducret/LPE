---
type: Rust Function
title: deliver_inbound_message
resource: crates/lpe-admin-api/src/integration.rs#L63-L99
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/integration/require_integration
  - functions/crates/lpe-admin-api/src/readiness/ha_allows_active_work
  - functions/crates/lpe-admin-api/src/readiness/ha_current_role
---

# Signature

`pub(crate) async fn deliver_inbound_message( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<InboundDeliveryRequest>, ) -> ApiResult<InboundDeliveryResponse>`

# Calls

- [require_integration](../../../../../functions/crates/lpe-admin-api/src/integration/require_integration.md)
- [ha_allows_active_work](../../../../../functions/crates/lpe-admin-api/src/readiness/ha_allows_active_work.md)
- [ha_current_role](../../../../../functions/crates/lpe-admin-api/src/readiness/ha_current_role.md)