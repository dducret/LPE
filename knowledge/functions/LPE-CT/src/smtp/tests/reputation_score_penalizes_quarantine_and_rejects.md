---
type: Rust Function
title: reputation_score_penalizes_quarantine_and_rejects
resource: LPE-CT/src/smtp/tests.rs#L2502-L2552
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/runtime_config
  - functions/LPE-CT/src/smtp/reputation/update_reputation
  - functions/LPE-CT/src/smtp/reputation/load_reputation_score
  - functions/LPE-CT/src/smtp/parse_peer_ip
---

# Signature

`async fn reputation_score_penalizes_quarantine_and_rejects()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [update_reputation](../../../../../functions/LPE-CT/src/smtp/reputation/update_reputation.md)
- [load_reputation_score](../../../../../functions/LPE-CT/src/smtp/reputation/load_reputation_score.md)
- [parse_peer_ip](../../../../../functions/LPE-CT/src/smtp/parse_peer_ip.md)