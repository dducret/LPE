---
type: Rust Module
title: websocket
resource: crates/lpe-jmap/src/websocket.rs#L1-L901
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/axum-extract-ws-message-websocket-http-statuscode
  - external/serde-de-deserializeowned
  - external/serde-json-value
  - external/std-collections-hashmap-hashset
  - external/uuid-uuid
  - external/lpe-storage-authenticatedaccount-canonicalchangecategory-canonicalpushchangeset
  - external/crate-protocol-jmapapirequest-websocketpushdisable-websocketpushenable-websocketrequestenvelope-websocketrequesterror-websocketresponse-websocketstatechange-state-decode-push-state-encode-push-state-push-state-entries-to-types-store-jmappushlistener-jmapservice
  - external/super
  - external/serde-json-json
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [PushSubscription](../../../../classes/crates/lpe-jmap/src/websocket/PushSubscription.md)
- [handle_websocket](../../../../functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket.md)
- [handle_websocket_message](../../../../functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket_message.md)
- [enable_push](../../../../functions/crates/lpe-jmap/src/websocket/JmapService/enable_push.md)
- [publish_state_changes](../../../../functions/crates/lpe-jmap/src/websocket/JmapService/publish_state_changes.md)
- [recover_push_enable_change](../../../../functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change.md)
- [compute_push_changes](../../../../functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes.md)
- [send_request_error_object](../../../../functions/crates/lpe-jmap/src/websocket/JmapService/send_request_error_object.md)
- [send_request_error](../../../../functions/crates/lpe-jmap/src/websocket/JmapService/send_request_error.md)
- [send_state_change](../../../../functions/crates/lpe-jmap/src/websocket/JmapService/send_state_change.md)
- [push_categories](../../../../functions/crates/lpe-jmap/src/websocket/JmapService/push_categories.md)
- [is_mail_push_type](../../../../functions/crates/lpe-jmap/src/websocket/JmapService/is_mail_push_type.md)
- [mail_push_type_state](../../../../functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state.md)
- [current_push_states](../../../../functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states.md)
- [normalize_push_data_types](../../../../functions/crates/lpe-jmap/src/websocket/normalize_push_data_types.md)
- [parse_websocket_object](../../../../functions/crates/lpe-jmap/src/websocket/parse_websocket_object.md)
- [websocket_request_id](../../../../functions/crates/lpe-jmap/src/websocket/websocket_request_id.md)
- [websocket_request_error](../../../../functions/crates/lpe-jmap/src/websocket/websocket_request_error.md)
- [filter_push_state_types](../../../../functions/crates/lpe-jmap/src/websocket/filter_push_state_types.md)
- [merge_journal_cursor](../../../../functions/crates/lpe-jmap/src/websocket/merge_journal_cursor.md)
- [finalize_push_change](../../../../functions/crates/lpe-jmap/src/websocket/finalize_push_change.md)
- [push_enable_null_or_missing_data_types_subscribes_to_all_supported_types](../../../../functions/crates/lpe-jmap/src/websocket/push_enable_null_or_missing_data_types_subscribes_to_all_supported_types.md)
- [push_enable_filters_unsupported_data_types](../../../../functions/crates/lpe-jmap/src/websocket/push_enable_filters_unsupported_data_types.md)
- [malformed_websocket_request_objects_map_to_request_error](../../../../functions/crates/lpe-jmap/src/websocket/malformed_websocket_request_objects_map_to_request_error.md)
- [finalize_push_change_emits_cursor_only_push_state](../../../../functions/crates/lpe-jmap/src/websocket/finalize_push_change_emits_cursor_only_push_state.md)

# Imports

- `anyhow::Result`
- `axum::{
    extract::ws::{Message, WebSocket},
    http::StatusCode,
}`
- `serde::de::DeserializeOwned`
- `serde_json::Value`
- `std::collections::{HashMap, HashSet}`
- `uuid::Uuid`
- `lpe_storage::{AuthenticatedAccount, CanonicalChangeCategory, CanonicalPushChangeSet}`
- `crate::{
    protocol::{
        JmapApiRequest, WebSocketPushDisable, WebSocketPushEnable, WebSocketRequestEnvelope,
        WebSocketRequestError, WebSocketResponse, WebSocketStateChange,
    },
    state::{decode_push_state, encode_push_state, push_state_entries_to_types},
    store::JmapPushListener,
    JmapService,
}`
- `super::*`
- `serde_json::json`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)