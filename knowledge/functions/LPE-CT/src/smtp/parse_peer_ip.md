---
type: Rust Function
title: parse_peer_ip
resource: LPE-CT/src/smtp.rs#L1170-L1175
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/reputation/update_reputation
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
  - functions/LPE-CT/src/smtp/tests/reputation_score_penalizes_quarantine_and_rejects
---

# Signature

`fn parse_peer_ip(peer: &str) -> Option<IpAddr>`

# Called by

- [update_reputation](../../../../functions/LPE-CT/src/smtp/reputation/update_reputation.md)
- [receive_message_with_validator](../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)
- [reputation_score_penalizes_quarantine_and_rejects](../../../../functions/LPE-CT/src/smtp/tests/reputation_score_penalizes_quarantine_and_rejects.md)