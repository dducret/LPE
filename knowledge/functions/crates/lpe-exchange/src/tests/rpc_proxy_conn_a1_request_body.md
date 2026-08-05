---
type: Rust Function
title: rpc_proxy_conn_a1_request_body
resource: crates/lpe-exchange/src/tests/mod.rs#L12066-L12083
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_referral_endpoint_ping_returns_a3_without_synthetic_bind_ack
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_endpoint_ping_waits_for_b1_before_bind_ack
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_returns_a3_without_synthetic_bind_ack
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_includes_pending_conn_b1_when_in_arrives_first
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_suppresses_duplicate_conn_b1_when_out_arrives_first
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack
---

# Signature

`fn rpc_proxy_conn_a1_request_body(receive_window_size: u32) -> Vec<u8>`

# Called by

- [rpc_proxy_referral_endpoint_ping_returns_a3_without_synthetic_bind_ack](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_referral_endpoint_ping_returns_a3_without_synthetic_bind_ack.md)
- [rpc_proxy_mailstore_endpoint_ping_waits_for_b1_before_bind_ack](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_endpoint_ping_waits_for_b1_before_bind_ack.md)
- [rpc_proxy_address_book_endpoint_ping_returns_a3_without_synthetic_bind_ack](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_returns_a3_without_synthetic_bind_ack.md)
- [rpc_proxy_address_book_endpoint_ping_includes_pending_conn_b1_when_in_arrives_first](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_includes_pending_conn_b1_when_in_arrives_first.md)
- [rpc_proxy_address_book_endpoint_ping_suppresses_duplicate_conn_b1_when_out_arrives_first](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_suppresses_duplicate_conn_b1_when_out_arrives_first.md)
- [rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack.md)