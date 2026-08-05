---
type: Rust Function
title: accept
resource: LPE-CT/src/smtp/tests.rs#L3587-L3603
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn accept( axum::extract::State((spool, observed_spool_custody)): axum::extract::State<( PathBuf, Arc<Mutex<bool>>, )>, Json(request): Json<InboundDeliveryRequest>, ) -> Json<InboundDeliveryResponse>`