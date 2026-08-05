---
type: Rust Function
title: status_only
resource: crates/lpe-dav/src/responses.rs#L63-L68
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_get
  - functions/crates/lpe-dav/src/service/DavService/handle_delete
---

# Signature

`pub(crate) fn status_only(status: u16) -> Response`

# Calls

- [status](../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [empty](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)

# Called by

- [handle_get](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_get.md)
- [handle_delete](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_delete.md)