---
type: Rust Function
title: sync_technical_store
resource: LPE-CT/src/main.rs#L938-L943
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/read_state
  - functions/LPE-CT/src/sync_dashboard_to_postgres
  called_by:
  - functions/LPE-CT/src/http_routes/create_accepted_domain
  - functions/LPE-CT/src/http_routes/update_accepted_domain
  - functions/LPE-CT/src/http_routes/delete_accepted_domain
  - functions/LPE-CT/src/http_routes/import_accepted_domains
  - functions/LPE-CT/src/http_routes/test_accepted_domain
  - functions/LPE-CT/src/http_routes/update_reporting
---

# Signature

`async fn sync_technical_store(state: &AppState) -> Result<(), ApiError>`

# Calls

- [read_state](../../../functions/LPE-CT/src/read_state.md)
- [sync_dashboard_to_postgres](../../../functions/LPE-CT/src/sync_dashboard_to_postgres.md)

# Called by

- [create_accepted_domain](../../../functions/LPE-CT/src/http_routes/create_accepted_domain.md)
- [update_accepted_domain](../../../functions/LPE-CT/src/http_routes/update_accepted_domain.md)
- [delete_accepted_domain](../../../functions/LPE-CT/src/http_routes/delete_accepted_domain.md)
- [import_accepted_domains](../../../functions/LPE-CT/src/http_routes/import_accepted_domains.md)
- [test_accepted_domain](../../../functions/LPE-CT/src/http_routes/test_accepted_domain.md)
- [update_reporting](../../../functions/LPE-CT/src/http_routes/update_reporting.md)