---
type: Rust Function
title: public_base_path
resource: crates/lpe-jmap/src/session.rs#L93-L99
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/normalize_public_base_path
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/service/session_handler
  - functions/crates/lpe-jmap/src/session/websocket_url
---

# Signature

`pub(crate) fn public_base_path(headers: &HeaderMap) -> String`

# Calls

- [normalize_public_base_path](../../../../../functions/crates/lpe-jmap/src/session/normalize_public_base_path.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [session_handler](../../../../../functions/crates/lpe-jmap/src/service/session_handler.md)
- [websocket_url](../../../../../functions/crates/lpe-jmap/src/session/websocket_url.md)