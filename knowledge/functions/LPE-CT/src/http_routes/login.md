---
type: Rust Function
title: login
resource: LPE-CT/src/http_routes.rs#L89-L139
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/append_audit_event_with_actor
---

# Signature

`pub(crate) async fn login( State(state): State<AppState>, Json(payload): Json<LoginRequest>, ) -> Result<Json<LoginResponse>, ApiError>`

# Calls

- [read_state](../../../../functions/LPE-CT/src/read_state.md)
- [append_audit_event_with_actor](../../../../functions/LPE-CT/src/append_audit_event_with_actor.md)