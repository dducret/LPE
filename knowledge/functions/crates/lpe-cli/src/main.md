---
type: Rust Function
title: main
resource: crates/lpe-cli/src/main.rs#L23-L91
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/app/init_observability
  - functions/crates/lpe-cli/src/log_startup_fingerprint
  - functions/crates/lpe-cli/src/run_bootstrap_admin_command
  - functions/crates/lpe-storage/src/core/Storage/connect
  - functions/crates/lpe-cli/src/auto_bootstrap_admin_if_missing
  - functions/crates/lpe-cli/src/run_outbound_worker
  - functions/crates/lpe-cli/src/run_jmap_journal_purge_worker
---

# Signature

`async fn main() -> Result<()>`

# Calls

- [init_observability](../../../../functions/crates/lpe-admin-api/src/app/init_observability.md)
- [log_startup_fingerprint](../../../../functions/crates/lpe-cli/src/log_startup_fingerprint.md)
- [run_bootstrap_admin_command](../../../../functions/crates/lpe-cli/src/run_bootstrap_admin_command.md)
- [connect](../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [auto_bootstrap_admin_if_missing](../../../../functions/crates/lpe-cli/src/auto_bootstrap_admin_if_missing.md)
- [run_outbound_worker](../../../../functions/crates/lpe-cli/src/run_outbound_worker.md)
- [run_jmap_journal_purge_worker](../../../../functions/crates/lpe-cli/src/run_jmap_journal_purge_worker.md)