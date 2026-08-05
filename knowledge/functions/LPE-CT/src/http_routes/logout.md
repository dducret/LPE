---
type: Rust Function
title: logout
resource: LPE-CT/src/http_routes.rs#L141-L172
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/LPE-CT/src/append_audit_event_with_actor
---

# Signature

`pub(crate) async fn logout( State(state): State<AppState>, headers: HeaderMap, ) -> Result<Json<HealthResponse>, ApiError>`

# Calls

- [remove](../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [append_audit_event_with_actor](../../../../functions/LPE-CT/src/append_audit_event_with_actor.md)