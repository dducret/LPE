---
type: Rust Function
title: authorization_header
resource: crates/lpe-jmap/src/service/helpers.rs#L646-L663
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/service/session_handler
  - functions/crates/lpe-jmap/src/service/api_handler
  - functions/crates/lpe-jmap/src/service/upload_handler
  - functions/crates/lpe-jmap/src/service/download_handler
  - functions/crates/lpe-jmap/src/service/websocket_handler
  - functions/crates/lpe-jmap/src/service/event_source_handler
---

# Signature

`pub(super) fn authorization_header(headers: &HeaderMap) -> Option<String>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [session_handler](../../../../../../functions/crates/lpe-jmap/src/service/session_handler.md)
- [api_handler](../../../../../../functions/crates/lpe-jmap/src/service/api_handler.md)
- [upload_handler](../../../../../../functions/crates/lpe-jmap/src/service/upload_handler.md)
- [download_handler](../../../../../../functions/crates/lpe-jmap/src/service/download_handler.md)
- [websocket_handler](../../../../../../functions/crates/lpe-jmap/src/service/websocket_handler.md)
- [event_source_handler](../../../../../../functions/crates/lpe-jmap/src/service/event_source_handler.md)