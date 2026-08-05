---
type: Python Function
title: ws_recv_text
resource: tools/operations_benchmark.py#L344-L364
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/operations_benchmark/websocket_push_enable_round_trip
---

# Signature

`def ws_recv_text(sock: socket.socket) -> dict[str, Any]:`

# Called by

- [websocket_push_enable_round_trip](../../../functions/tools/operations_benchmark/websocket_push_enable_round_trip.md)