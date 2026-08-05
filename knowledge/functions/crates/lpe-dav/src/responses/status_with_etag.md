---
type: Rust Function
title: status_with_etag
resource: crates/lpe-dav/src/responses.rs#L70-L76
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_put
---

# Signature

`pub(crate) fn status_with_etag(status: u16, etag: String) -> Response`

# Calls

- [status](../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [empty](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)

# Called by

- [handle_put](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_put.md)