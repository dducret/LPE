---
type: Rust Function
title: upload_handler
resource: crates/lpe-jmap/src/service.rs#L223-L246
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/authorization_header
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/service/blobs/JmapService/handle_upload
---

# Signature

`async fn upload_handler( State(storage): State<Storage>, axum::extract::Path(account_id): axum::extract::Path<String>, headers: HeaderMap, body: Bytes, ) -> std::result::Result<impl IntoResponse, (StatusCode, Json<Value>)>`

# Calls

- [authorization_header](../../../../../functions/crates/lpe-jmap/src/service/helpers/authorization_header.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [handle_upload](../../../../../functions/crates/lpe-jmap/src/service/blobs/JmapService/handle_upload.md)