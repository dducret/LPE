---
type: Rust Method
title: record_transport_request
resource: crates/lpe-exchange/src/mapi/session.rs#L50-L65
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_records_transport_request_lifetime
  - functions/crates/lpe-exchange/src/mapi/transport/disconnect_response
---

# Signature

`pub(in crate::mapi) fn record_transport_request( &mut self, request_type: &str, request_id: &str, )`

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [reconnect_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session.md)
- [create_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)
- [established_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request.md)
- [session_records_transport_request_lifetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_records_transport_request_lifetime.md)
- [disconnect_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_response.md)