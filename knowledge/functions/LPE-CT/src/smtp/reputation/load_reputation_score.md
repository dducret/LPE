---
type: Rust Function
title: load_reputation_score
resource: LPE-CT/src/smtp/reputation.rs#L16-L40
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/reputation/reputation_key
  - functions/crates/lpe-activesync/src/tests/query
  - functions/LPE-CT/src/smtp/reputation/load_reputation_store
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy
  - functions/LPE-CT/src/smtp/tests/reputation_score_penalizes_quarantine_and_rejects
---

# Signature

`pub(in crate::smtp) async fn load_reputation_score( spool_dir: &Path, config: &RuntimeConfig, peer_ip: Option<IpAddr>, mail_from: &str, ) -> Result<i32>`

# Calls

- [reputation_key](../../../../../functions/LPE-CT/src/smtp/reputation/reputation_key.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [load_reputation_store](../../../../../functions/LPE-CT/src/smtp/reputation/load_reputation_store.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [evaluate_inbound_policy](../../../../../functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy.md)
- [reputation_score_penalizes_quarantine_and_rejects](../../../../../functions/LPE-CT/src/smtp/tests/reputation_score_penalizes_quarantine_and_rejects.md)