---
type: Rust Module
title: eventsource
resource: crates/lpe-jmap/src/eventsource.rs#L1-L195
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/axum-response-sse-event-keepalive-sse-intoresponse-response
  - external/lpe-storage-authenticatedaccount-storage
  - external/serde-deserialize
  - external/std-collections-hashmap-convert-infallible-time-duration
  - external/tokio-sync-mpsc
  - external/tokio-stream-wrappers-receiverstream
  - external/crate-protocol-websocketstatechange-state-encode-push-state-store-jmapstore-websocket-finalize-push-change-normalize-push-data-types-pushsubscription-jmapservice
  - external/super
  - external/std-collections-hashset
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [EventSourceQuery](../../../../classes/crates/lpe-jmap/src/eventsource/EventSourceQuery.md)
- [handle_event_source](../../../../functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source.md)
- [event_source_data_types](../../../../functions/crates/lpe-jmap/src/eventsource/event_source_data_types.md)
- [event_source_close_after](../../../../functions/crates/lpe-jmap/src/eventsource/event_source_close_after.md)
- [send_state_change_event](../../../../functions/crates/lpe-jmap/src/eventsource/send_state_change_event.md)
- [event_source_types_use_all_supported_types_when_missing](../../../../functions/crates/lpe-jmap/src/eventsource/event_source_types_use_all_supported_types_when_missing.md)
- [event_source_types_parse_comma_separated_values](../../../../functions/crates/lpe-jmap/src/eventsource/event_source_types_parse_comma_separated_values.md)

# Imports

- `anyhow::Result`
- `axum::response::{
    sse::{Event, KeepAlive, Sse},
    IntoResponse, Response,
}`
- `lpe_storage::{AuthenticatedAccount, Storage}`
- `serde::Deserialize`
- `std::{collections::HashMap, convert::Infallible, time::Duration}`
- `tokio::sync::mpsc`
- `tokio_stream::wrappers::ReceiverStream`
- `crate::{
    protocol::WebSocketStateChange,
    state::encode_push_state,
    store::JmapStore,
    websocket::{finalize_push_change, normalize_push_data_types, PushSubscription},
    JmapService,
}`
- `super::*`
- `std::collections::HashSet`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)