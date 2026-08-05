---
type: Rust Function
title: dav_handler
resource: crates/lpe-dav/src/service.rs#L70-L82
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn dav_handler( State(storage): State<Storage>, method: Method, uri: Uri, headers: HeaderMap, body: Bytes, ) -> Response`