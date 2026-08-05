---
type: Rust Function
title: redirect_response
resource: crates/lpe-dav/src/responses.rs#L32-L38
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  called_by:
  - functions/crates/lpe-dav/src/service/carddav_redirect
  - functions/crates/lpe-dav/src/service/caldav_redirect
---

# Signature

`pub(crate) fn redirect_response(location: &str) -> Response`

# Calls

- [status](../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [empty](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)

# Called by

- [carddav_redirect](../../../../../functions/crates/lpe-dav/src/service/carddav_redirect.md)
- [caldav_redirect](../../../../../functions/crates/lpe-dav/src/service/caldav_redirect.md)