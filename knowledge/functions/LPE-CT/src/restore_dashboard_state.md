---
type: Rust Function
title: restore_dashboard_state
resource: LPE-CT/src/main.rs#L967-L976
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/persist_state
  called_by:
  - functions/LPE-CT/src/http_routes/create_accepted_domain
  - functions/LPE-CT/src/http_routes/update_accepted_domain
  - functions/LPE-CT/src/http_routes/delete_accepted_domain
  - functions/LPE-CT/src/http_routes/import_accepted_domains
  - functions/LPE-CT/src/http_routes/test_accepted_domain
  - functions/LPE-CT/src/http_routes/update_policies
  - functions/LPE-CT/src/http_routes/update_reporting
---

# Signature

`fn restore_dashboard_state(state: &AppState, snapshot: &DashboardState) -> Result<(), ApiError>`

# Calls

- [persist_state](../../../functions/LPE-CT/src/persist_state.md)

# Called by

- [create_accepted_domain](../../../functions/LPE-CT/src/http_routes/create_accepted_domain.md)
- [update_accepted_domain](../../../functions/LPE-CT/src/http_routes/update_accepted_domain.md)
- [delete_accepted_domain](../../../functions/LPE-CT/src/http_routes/delete_accepted_domain.md)
- [import_accepted_domains](../../../functions/LPE-CT/src/http_routes/import_accepted_domains.md)
- [test_accepted_domain](../../../functions/LPE-CT/src/http_routes/test_accepted_domain.md)
- [update_policies](../../../functions/LPE-CT/src/http_routes/update_policies.md)
- [update_reporting](../../../functions/LPE-CT/src/http_routes/update_reporting.md)