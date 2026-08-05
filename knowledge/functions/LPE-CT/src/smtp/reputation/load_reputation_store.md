---
type: Rust Function
title: load_reputation_store
resource: LPE-CT/src/smtp/reputation.rs#L109-L115
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/LPE-CT/src/smtp/reputation/load_reputation_score
  - functions/LPE-CT/src/smtp/reputation/update_reputation
---

# Signature

`fn load_reputation_store(spool_dir: &Path) -> Result<ReputationStore>`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [load_reputation_score](../../../../../functions/LPE-CT/src/smtp/reputation/load_reputation_score.md)
- [update_reputation](../../../../../functions/LPE-CT/src/smtp/reputation/update_reputation.md)