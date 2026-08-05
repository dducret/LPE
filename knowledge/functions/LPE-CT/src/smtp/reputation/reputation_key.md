---
type: Rust Function
title: reputation_key
resource: LPE-CT/src/smtp/reputation.rs#L90-L98
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/reputation/load_reputation_score
  - functions/LPE-CT/src/smtp/reputation/update_reputation
---

# Signature

`fn reputation_key(peer_ip: Option<IpAddr>, mail_from: &str) -> String`

# Called by

- [load_reputation_score](../../../../../functions/LPE-CT/src/smtp/reputation/load_reputation_score.md)
- [update_reputation](../../../../../functions/LPE-CT/src/smtp/reputation/update_reputation.md)