---
type: Python Function
title: main
resource: tools/operations_benchmark.py#L830-L880
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/operations_benchmark/timed
  - functions/tools/operations_benchmark/run_section
  - functions/tools/operations_benchmark/benchmark_cold_start
  - functions/tools/operations_benchmark/benchmark_jmap
  - functions/tools/operations_benchmark/benchmark_imap
  - functions/tools/operations_benchmark/benchmark_activesync
  - functions/tools/operations_benchmark/benchmark_smtp_data
  - functions/tools/operations_benchmark/benchmark_outbound_retry
  - functions/tools/operations_benchmark/markdown_report
  - functions/tools/operations_benchmark/Measurement/summary
---

# Signature

`def main() -> int:`

# Calls

- [timed](../../../functions/tools/operations_benchmark/timed.md)
- [run_section](../../../functions/tools/operations_benchmark/run_section.md)
- [benchmark_cold_start](../../../functions/tools/operations_benchmark/benchmark_cold_start.md)
- [benchmark_jmap](../../../functions/tools/operations_benchmark/benchmark_jmap.md)
- [benchmark_imap](../../../functions/tools/operations_benchmark/benchmark_imap.md)
- [benchmark_activesync](../../../functions/tools/operations_benchmark/benchmark_activesync.md)
- [benchmark_smtp_data](../../../functions/tools/operations_benchmark/benchmark_smtp_data.md)
- [benchmark_outbound_retry](../../../functions/tools/operations_benchmark/benchmark_outbound_retry.md)
- [markdown_report](../../../functions/tools/operations_benchmark/markdown_report.md)
- [summary](../../../functions/tools/operations_benchmark/Measurement/summary.md)