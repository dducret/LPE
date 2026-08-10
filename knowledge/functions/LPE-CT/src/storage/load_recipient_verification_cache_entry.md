---
type: Rust Function
title: load_recipient_verification_cache_entry
resource: LPE-CT/src/storage.rs#L1093-L1129
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/LPE-CT/src/transport_policy/verify_recipient_with_core
---

# Signature

`pub(crate) async fn load_recipient_verification_cache_entry( config: &LocalDbConfig, cache_key: &str, now_unix: u64, ) -> Result<Option<RecipientVerificationCacheRecord>>`

# Calls

- [query](../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [verify_recipient_with_core](../../../../functions/LPE-CT/src/transport_policy/verify_recipient_with_core.md)