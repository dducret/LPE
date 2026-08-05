---
type: Rust Function
title: metrics_endpoint
resource: LPE-CT/src/observability.rs#L115-L125
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response
---

# Signature

`pub async fn metrics_endpoint(spool_dir: std::sync::Arc<std::path::PathBuf>) -> impl IntoResponse`

# Calls

- [into_response](../../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)