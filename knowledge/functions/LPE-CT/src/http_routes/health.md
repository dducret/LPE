---
type: Rust Function
title: health
resource: LPE-CT/src/http_routes.rs#L3-L13
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/read_state
---

# Signature

`pub(crate) async fn health( State(state): State<AppState>, ) -> Result<Json<HealthResponse>, ApiError>`

# Calls

- [read_state](../../../../functions/LPE-CT/src/read_state.md)