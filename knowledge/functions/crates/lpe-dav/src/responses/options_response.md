---
type: Rust Function
title: options_response
resource: crates/lpe-dav/src/responses.rs#L22-L30
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle
---

# Signature

`pub(crate) fn options_response() -> Response`

# Calls

- [status](../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [empty](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)

# Called by

- [handle](../../../../../functions/crates/lpe-dav/src/service/DavService/handle.md)