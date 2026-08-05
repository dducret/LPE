---
type: Rust Function
title: multistatus_response
resource: crates/lpe-dav/src/responses.rs#L8-L20
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/responses/response_with_headers
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_propfind
  - functions/crates/lpe-dav/src/service/DavService/handle_report
---

# Signature

`pub(crate) fn multistatus_response(entries: Vec<String>) -> Response`

# Calls

- [response_with_headers](../../../../../functions/crates/lpe-dav/src/responses/response_with_headers.md)

# Called by

- [handle_propfind](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_propfind.md)
- [handle_report](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_report.md)