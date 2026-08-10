---
type: Rust Function
title: append_dashboard_audit_event
resource: LPE-CT/src/main.rs#L978-L989
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/http_routes/create_accepted_domain
  - functions/LPE-CT/src/http_routes/update_accepted_domain
  - functions/LPE-CT/src/http_routes/delete_accepted_domain
  - functions/LPE-CT/src/http_routes/import_accepted_domains
  - functions/LPE-CT/src/http_routes/test_accepted_domain
  - functions/LPE-CT/src/http_routes/update_reporting
  - functions/LPE-CT/src/mutate_state
---

# Signature

`fn append_dashboard_audit_event(state: &mut DashboardState, actor: &str, action: &str)`

# Called by

- [create_accepted_domain](../../../functions/LPE-CT/src/http_routes/create_accepted_domain.md)
- [update_accepted_domain](../../../functions/LPE-CT/src/http_routes/update_accepted_domain.md)
- [delete_accepted_domain](../../../functions/LPE-CT/src/http_routes/delete_accepted_domain.md)
- [import_accepted_domains](../../../functions/LPE-CT/src/http_routes/import_accepted_domains.md)
- [test_accepted_domain](../../../functions/LPE-CT/src/http_routes/test_accepted_domain.md)
- [update_reporting](../../../functions/LPE-CT/src/http_routes/update_reporting.md)
- [mutate_state](../../../functions/LPE-CT/src/mutate_state.md)