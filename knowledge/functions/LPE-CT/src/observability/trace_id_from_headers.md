---
type: Rust Function
title: trace_id_from_headers
resource: LPE-CT/src/observability.rs#L71-L79
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`pub fn trace_id_from_headers(headers: &axum::http::HeaderMap) -> String`

# Calls

- [get](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)