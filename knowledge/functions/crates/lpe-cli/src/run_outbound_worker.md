---
type: Rust Function
title: run_outbound_worker
resource: crates/lpe-cli/src/main.rs#L150-L250
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
  - functions/crates/lpe-admin-api/src/readiness/ha_allows_active_work
  - functions/crates/lpe-admin-api/src/readiness/ha_current_role
  - functions/crates/lpe-storage/src/outbound/Storage/fetch_outbound_handoff_batch
  - functions/crates/lpe-admin-api/src/app/observe_outbound_worker_poll_failure
  - functions/crates/lpe-admin-api/src/app/observe_outbound_worker_poll
  - functions/crates/lpe-cli/src/dispatch_outbound_message
  called_by:
  - functions/crates/lpe-cli/src/main
  - functions/crates/lpe-cli/src/outbound_worker_retries_after_a_closed_pool_failure
---

# Signature

`async fn run_outbound_worker(storage: Storage) -> Result<()>`

# Calls

- [build](../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)
- [ha_allows_active_work](../../../../functions/crates/lpe-admin-api/src/readiness/ha_allows_active_work.md)
- [ha_current_role](../../../../functions/crates/lpe-admin-api/src/readiness/ha_current_role.md)
- [fetch_outbound_handoff_batch](../../../../functions/crates/lpe-storage/src/outbound/Storage/fetch_outbound_handoff_batch.md)
- [observe_outbound_worker_poll_failure](../../../../functions/crates/lpe-admin-api/src/app/observe_outbound_worker_poll_failure.md)
- [observe_outbound_worker_poll](../../../../functions/crates/lpe-admin-api/src/app/observe_outbound_worker_poll.md)
- [dispatch_outbound_message](../../../../functions/crates/lpe-cli/src/dispatch_outbound_message.md)

# Called by

- [main](../../../../functions/crates/lpe-cli/src/main.md)
- [outbound_worker_retries_after_a_closed_pool_failure](../../../../functions/crates/lpe-cli/src/outbound_worker_retries_after_a_closed_pool_failure.md)