---
type: Rust Function
title: download_handler
resource: crates/lpe-jmap/src/service.rs#L248-L264
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/authorization_header
  - functions/crates/lpe-jmap/src/service/blobs/JmapService/handle_download
---

# Signature

`async fn download_handler( State(storage): State<Storage>, axum::extract::Path((account_id, blob_id, _name)): axum::extract::Path<( String, String, String, )>, headers: HeaderMap, ) -> std::result::Result<impl IntoResponse, (StatusCode, Json<Value>)>`

# Calls

- [authorization_header](../../../../../functions/crates/lpe-jmap/src/service/helpers/authorization_header.md)
- [handle_download](../../../../../functions/crates/lpe-jmap/src/service/blobs/JmapService/handle_download.md)