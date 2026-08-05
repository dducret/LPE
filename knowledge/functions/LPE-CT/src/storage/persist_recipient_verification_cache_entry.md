---
type: Rust Function
title: persist_recipient_verification_cache_entry
resource: LPE-CT/src/storage.rs#L1131-L1166
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/LPE-CT/src/transport_policy/verify_recipient_with_core
---

# Signature

`pub(crate) async fn persist_recipient_verification_cache_entry( config: &LocalDbConfig, entry: &RecipientVerificationCacheEntry, ) -> Result<()>`

# Calls

- [query](../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [verify_recipient_with_core](../../../../functions/LPE-CT/src/transport_policy/verify_recipient_with_core.md)