---
type: Rust Function
title: mapi_outlook_view_metrics
resource: crates/lpe-exchange/src/mapi.rs#L271-L333
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/load
  called_by:
  - functions/crates/lpe-admin-api/src/observability/render_metrics
---

# Signature

`pub fn mapi_outlook_view_metrics() -> MapiOutlookViewMetrics`

# Calls

- [load](../../../../../functions/LPE-CT/web/app/load.md)

# Called by

- [render_metrics](../../../../../functions/crates/lpe-admin-api/src/observability/render_metrics.md)