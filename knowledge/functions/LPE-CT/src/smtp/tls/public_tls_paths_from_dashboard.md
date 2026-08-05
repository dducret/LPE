---
type: Rust Function
title: public_tls_paths_from_dashboard
resource: LPE-CT/src/smtp/tls.rs#L87-L113
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/tls/smtp_starttls_acceptor_from_store
---

# Signature

`fn public_tls_paths_from_dashboard( dashboard: &crate::DashboardState, ) -> (Option<String>, Option<String>)`

# Called by

- [smtp_starttls_acceptor_from_store](../../../../../functions/LPE-CT/src/smtp/tls/smtp_starttls_acceptor_from_store.md)