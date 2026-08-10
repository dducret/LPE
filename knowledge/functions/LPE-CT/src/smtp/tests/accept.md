---
type: Rust Function
title: accept
resource: LPE-CT/src/smtp/tests.rs#L3535-L3547
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn accept( axum::extract::State(captured): axum::extract::State< Arc<Mutex<Option<InboundDeliveryRequest>>>, >, Json(request): Json<InboundDeliveryRequest>, ) -> Json<InboundDeliveryResponse>`