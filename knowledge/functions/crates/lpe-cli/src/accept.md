---
type: Rust Function
title: accept
resource: crates/lpe-cli/src/main.rs#L467-L501
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`async fn accept( State(capture): State<Capture>, headers: HeaderMap, Json(request): Json<OutboundMessageHandoffRequest>, ) -> Json<OutboundMessageHandoffResponse>`

# Calls

- [get](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)