---
type: Rust Function
title: smtp_starttls_acceptor_for_paths
resource: LPE-CT/src/smtp/tls.rs#L115-L136
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/tests/smtp_starttls_upgrades_to_tls_after_ready_reply
  - functions/LPE-CT/src/smtp/tls/smtp_starttls_acceptor_from_store
---

# Signature

`pub(crate) fn smtp_starttls_acceptor_for_paths( cert_path: Option<String>, key_path: Option<String>, ) -> Result<Option<TlsAcceptor>>`

# Called by

- [smtp_starttls_upgrades_to_tls_after_ready_reply](../../../../../functions/LPE-CT/src/smtp/tests/smtp_starttls_upgrades_to_tls_after_ready_reply.md)
- [smtp_starttls_acceptor_from_store](../../../../../functions/LPE-CT/src/smtp/tls/smtp_starttls_acceptor_from_store.md)