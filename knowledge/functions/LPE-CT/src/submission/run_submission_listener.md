---
type: Rust Function
title: run_submission_listener
resource: LPE-CT/src/submission.rs#L72-L99
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
  - functions/LPE-CT/src/submission/handle_submission_session
  called_by:
  - functions/LPE-CT/src/main
---

# Signature

`pub(crate) async fn run_submission_listener( bind_address: String, core_base_url: String, dashboard_store: Arc<Mutex<crate::DashboardState>>, ) -> Result<()>`

# Calls

- [build](../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)
- [handle_submission_session](../../../../functions/LPE-CT/src/submission/handle_submission_session.md)

# Called by

- [main](../../../../functions/LPE-CT/src/main.md)