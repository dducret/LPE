---
type: Rust Function
title: run_reporting_scheduler
resource: LPE-CT/src/main.rs#L867-L933
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/runtime_config_from_dashboard
  - functions/LPE-CT/src/reporting/enforce_retention
  - functions/LPE-CT/src/reporting/run_due_digest_generation
  - functions/LPE-CT/src/persist_state
  - functions/LPE-CT/src/sync_dashboard_to_postgres
  called_by:
  - functions/LPE-CT/src/main
---

# Signature

`async fn run_reporting_scheduler(state: AppState)`

# Calls

- [runtime_config_from_dashboard](../../../functions/LPE-CT/src/smtp/runtime_config_from_dashboard.md)
- [enforce_retention](../../../functions/LPE-CT/src/reporting/enforce_retention.md)
- [run_due_digest_generation](../../../functions/LPE-CT/src/reporting/run_due_digest_generation.md)
- [persist_state](../../../functions/LPE-CT/src/persist_state.md)
- [sync_dashboard_to_postgres](../../../functions/LPE-CT/src/sync_dashboard_to_postgres.md)

# Called by

- [main](../../../functions/LPE-CT/src/main.md)