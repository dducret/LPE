---
type: Rust Function
title: ha_non_active_role_for_traffic
resource: LPE-CT/src/readiness.rs#L36-L41
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/http_routes/outbound_handoff
  - functions/LPE-CT/src/imaps_proxy/handle_imaps_session
  - functions/LPE-CT/src/smtp/session/handle_smtp_session
  - functions/LPE-CT/src/submission/handle_submission_session
---

# Signature

`pub(crate) fn ha_non_active_role_for_traffic() -> Result<Option<String>>`

# Called by

- [outbound_handoff](../../../../functions/LPE-CT/src/http_routes/outbound_handoff.md)
- [handle_imaps_session](../../../../functions/LPE-CT/src/imaps_proxy/handle_imaps_session.md)
- [handle_smtp_session](../../../../functions/LPE-CT/src/smtp/session/handle_smtp_session.md)
- [handle_submission_session](../../../../functions/LPE-CT/src/submission/handle_submission_session.md)