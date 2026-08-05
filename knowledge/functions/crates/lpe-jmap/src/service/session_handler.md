---
type: Rust Function
title: session_handler
resource: crates/lpe-jmap/src/service.rs#L129-L148
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/authorization_header
  - functions/crates/lpe-jmap/src/session/public_base_url
  - functions/crates/lpe-jmap/src/session/public_base_path
  - functions/crates/lpe-jmap/src/session/JmapService/session_document
---

# Signature

`async fn session_handler( State(storage): State<Storage>, headers: HeaderMap, ) -> HttpResult<SessionDocument>`

# Calls

- [authorization_header](../../../../../functions/crates/lpe-jmap/src/service/helpers/authorization_header.md)
- [public_base_url](../../../../../functions/crates/lpe-jmap/src/session/public_base_url.md)
- [public_base_path](../../../../../functions/crates/lpe-jmap/src/session/public_base_path.md)
- [session_document](../../../../../functions/crates/lpe-jmap/src/session/JmapService/session_document.md)