---
type: Rust Function
title: text_response
resource: crates/lpe-dav/src/responses.rs#L40-L46
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-dav/src/responses/response_with_headers
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_get
---

# Signature

`pub(crate) fn text_response(content_type: &str, body: String, etag: Option<String>) -> Response`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [response_with_headers](../../../../../functions/crates/lpe-dav/src/responses/response_with_headers.md)

# Called by

- [handle_get](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_get.md)