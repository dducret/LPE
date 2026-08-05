---
type: Rust Function
title: save_reputation_store
resource: LPE-CT/src/smtp/reputation.rs#L117-L121
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/reputation/update_reputation
---

# Signature

`fn save_reputation_store(spool_dir: &Path, store: &ReputationStore) -> Result<()>`

# Called by

- [update_reputation](../../../../../functions/LPE-CT/src/smtp/reputation/update_reputation.md)