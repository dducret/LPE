---
type: Python Module
title: operations_benchmark
resource: tools/operations_benchmark.py#L1-L884
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/argparse
  - external/base64
  - external/json
  - external/os
  - external/socket
  - external/ssl
  - external/statistics
  - external/struct
  - external/subprocess
  - external/sys
  - external/time
  - external/urllib-error
  - external/urllib-parse
  - external/urllib-request
  - external/dataclasses
  - external/typing
---

# Contains

- [Measurement](../../classes/tools/operations_benchmark/Measurement.md)
- [summary](../../functions/tools/operations_benchmark/Measurement/summary.md)
- [AccountLogin](../../classes/tools/operations_benchmark/AccountLogin.md)
- [env](../../functions/tools/operations_benchmark/env.md)
- [require_env](../../functions/tools/operations_benchmark/require_env.md)
- [bool_env](../../functions/tools/operations_benchmark/bool_env.md)
- [percentile](../../functions/tools/operations_benchmark/percentile.md)
- [timed](../../functions/tools/operations_benchmark/timed.md)
- [http_json](../../functions/tools/operations_benchmark/http_json.md)
- [http_bytes](../../functions/tools/operations_benchmark/http_bytes.md)
- [require_status](../../functions/tools/operations_benchmark/require_status.md)
- [login](../../functions/tools/operations_benchmark/login.md)
- [jmap](../../functions/tools/operations_benchmark/jmap.md)
- [method_response](../../functions/tools/operations_benchmark/method_response.md)
- [websocket_url](../../functions/tools/operations_benchmark/websocket_url.md)
- [recv_exact](../../functions/tools/operations_benchmark/recv_exact.md)
- [ws_connect](../../functions/tools/operations_benchmark/ws_connect.md)
- [ws_send_text](../../functions/tools/operations_benchmark/ws_send_text.md)
- [ws_send_close](../../functions/tools/operations_benchmark/ws_send_close.md)
- [ws_recv_text](../../functions/tools/operations_benchmark/ws_recv_text.md)
- [ws_send_pong](../../functions/tools/operations_benchmark/ws_send_pong.md)
- [wbxml_node](../../functions/tools/operations_benchmark/wbxml_node.md)
- [encode_wbxml](../../functions/tools/operations_benchmark/encode_wbxml.md)
- [encode_wbxml_node](../../functions/tools/operations_benchmark/encode_wbxml_node.md)
- [basic_header](../../functions/tools/operations_benchmark/basic_header.md)
- [active_sync_url](../../functions/tools/operations_benchmark/active_sync_url.md)
- [benchmark_cold_start](../../functions/tools/operations_benchmark/benchmark_cold_start.md)
- [benchmark_jmap](../../functions/tools/operations_benchmark/benchmark_jmap.md)
- [websocket_push_enable_round_trip](../../functions/tools/operations_benchmark/websocket_push_enable_round_trip.md)
- [benchmark_imap](../../functions/tools/operations_benchmark/benchmark_imap.md)
- [imap_connect](../../functions/tools/operations_benchmark/imap_connect.md)
- [imap_read_until_greeting](../../functions/tools/operations_benchmark/imap_read_until_greeting.md)
- [imap_command](../../functions/tools/operations_benchmark/imap_command.md)
- [imap_read_until](../../functions/tools/operations_benchmark/imap_read_until.md)
- [imap_exists_count](../../functions/tools/operations_benchmark/imap_exists_count.md)
- [benchmark_activesync](../../functions/tools/operations_benchmark/benchmark_activesync.md)
- [jmap_inbox_mailbox_id](../../functions/tools/operations_benchmark/jmap_inbox_mailbox_id.md)
- [benchmark_smtp_data](../../functions/tools/operations_benchmark/benchmark_smtp_data.md)
- [smtp_connect](../../functions/tools/operations_benchmark/smtp_connect.md)
- [smtp_read_reply](../../functions/tools/operations_benchmark/smtp_read_reply.md)
- [smtp_command](../../functions/tools/operations_benchmark/smtp_command.md)
- [smtp_send_data](../../functions/tools/operations_benchmark/smtp_send_data.md)
- [benchmark_outbound_retry](../../functions/tools/operations_benchmark/benchmark_outbound_retry.md)
- [run_section](../../functions/tools/operations_benchmark/run_section.md)
- [markdown_report](../../functions/tools/operations_benchmark/markdown_report.md)
- [main](../../functions/tools/operations_benchmark/main.md)

# Imports

- `argparse`
- `base64`
- `json`
- `os`
- `socket`
- `ssl`
- `statistics`
- `struct`
- `subprocess`
- `sys`
- `time`
- `urllib.error`
- `urllib.parse`
- `urllib.request`
- `dataclasses`
- `typing`