---
type: Rust Function
title: queue_metrics
resource: LPE-CT/src/smtp.rs#L456-L484
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/queue_store/inspect_queue
  called_by:
  - functions/LPE-CT/src/http_routes/dashboard
  - functions/LPE-CT/src/http_routes/system_diagnostic_report
  - functions/LPE-CT/src/queue_metrics_count_runtime_spool_messages_by_state
---

# Signature

`pub(crate) fn queue_metrics( spool_dir: &Path, upstream_reachable: bool, ) -> Result<super::QueueMetrics>`

# Calls

- [inspect_queue](../../../../functions/LPE-CT/src/smtp/queue_store/inspect_queue.md)

# Called by

- [dashboard](../../../../functions/LPE-CT/src/http_routes/dashboard.md)
- [system_diagnostic_report](../../../../functions/LPE-CT/src/http_routes/system_diagnostic_report.md)
- [queue_metrics_count_runtime_spool_messages_by_state](../../../../functions/LPE-CT/src/queue_metrics_count_runtime_spool_messages_by_state.md)