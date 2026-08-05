---
type: Rust Module
title: storage
resource: LPE-CT/src/storage.rs#L1-L1214
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-context-result
  - external/sqlx-postgres-pgpooloptions-types-json-pgpool-row
  - external/std-sync-oncelock
  - external/tokio-sync-oncecell
  member_of:
  - packages/LPE-CT
---

# Contains

- [LocalDbConfig](../../../classes/LPE-CT/src/storage/LocalDbConfig.md)
- [RecipientVerificationCacheEntry](../../../classes/LPE-CT/src/storage/RecipientVerificationCacheEntry.md)
- [RecipientVerificationCacheRecord](../../../classes/LPE-CT/src/storage/RecipientVerificationCacheRecord.md)
- [ensure_local_db_schema](../../../functions/LPE-CT/src/storage/ensure_local_db_schema.md)
- [load_dashboard_state](../../../functions/LPE-CT/src/storage/load_dashboard_state.md)
- [persist_dashboard_state](../../../functions/LPE-CT/src/storage/persist_dashboard_state.md)
- [sync_dashboard_configuration](../../../functions/LPE-CT/src/storage/sync_dashboard_configuration.md)
- [delete_stale_policy_address_rules](../../../functions/LPE-CT/src/storage/delete_stale_policy_address_rules.md)
- [delete_stale_attachment_policy_rules](../../../functions/LPE-CT/src/storage/delete_stale_attachment_policy_rules.md)
- [load_recipient_verification_cache_entry](../../../functions/LPE-CT/src/storage/load_recipient_verification_cache_entry.md)
- [persist_recipient_verification_cache_entry](../../../functions/LPE-CT/src/storage/persist_recipient_verification_cache_entry.md)
- [local_db_pool](../../../functions/LPE-CT/src/storage/local_db_pool.md)
- [ensure_pg_trgm_extension](../../../functions/LPE-CT/src/storage/ensure_pg_trgm_extension.md)

# Imports

- `anyhow::{anyhow, Context, Result}`
- `sqlx::{postgres::PgPoolOptions, types::Json, PgPool, Row}`
- `std::sync::OnceLock`
- `tokio::sync::OnceCell`

# Member of

- [lpe-ct](../../../packages/LPE-CT.md)