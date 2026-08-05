---
type: Rust Function
title: update_reputation
resource: LPE-CT/src/smtp/reputation.rs#L42-L88
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/reputation/reputation_key
  - functions/LPE-CT/src/smtp/parse_peer_ip
  - functions/crates/lpe-activesync/src/tests/query
  - functions/LPE-CT/src/smtp/reputation/load_reputation_store
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/LPE-CT/src/smtp/reputation/save_reputation_store
  called_by:
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
  - functions/LPE-CT/src/smtp/tests/reputation_score_penalizes_quarantine_and_rejects
---

# Signature

`pub(in crate::smtp) async fn update_reputation( spool_dir: &Path, config: &RuntimeConfig, message: &QueuedMessage, action: FilterAction, ) -> Result<()>`

# Calls

- [reputation_key](../../../../../functions/LPE-CT/src/smtp/reputation/reputation_key.md)
- [parse_peer_ip](../../../../../functions/LPE-CT/src/smtp/parse_peer_ip.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [load_reputation_store](../../../../../functions/LPE-CT/src/smtp/reputation/load_reputation_store.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [save_reputation_store](../../../../../functions/LPE-CT/src/smtp/reputation/save_reputation_store.md)

# Called by

- [receive_message_with_validator](../../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)
- [reputation_score_penalizes_quarantine_and_rejects](../../../../../functions/LPE-CT/src/smtp/tests/reputation_score_penalizes_quarantine_and_rejects.md)