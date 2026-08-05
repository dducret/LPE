---
type: Rust Function
title: event_source_handler
resource: crates/lpe-jmap/src/service.rs#L282-L301
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/authorization_header
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source
---

# Signature

`async fn event_source_handler( State(storage): State<Storage>, headers: HeaderMap, Query(query): Query<EventSourceQuery>, ) -> std::result::Result<impl IntoResponse, (StatusCode, Json<Value>)>`

# Calls

- [authorization_header](../../../../../functions/crates/lpe-jmap/src/service/helpers/authorization_header.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [handle_event_source](../../../../../functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source.md)