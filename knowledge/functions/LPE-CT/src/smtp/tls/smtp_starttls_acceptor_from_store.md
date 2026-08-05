---
type: Rust Function
title: smtp_starttls_acceptor_from_store
resource: LPE-CT/src/smtp/tls.rs#L75-L85
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/tls/public_tls_paths_from_dashboard
  - functions/LPE-CT/src/smtp/tls/smtp_starttls_acceptor_for_paths
  called_by:
  - functions/LPE-CT/src/smtp/run_smtp_listener
---

# Signature

`pub(super) fn smtp_starttls_acceptor_from_store( dashboard_store: &Arc<Mutex<crate::DashboardState>>, ) -> Result<Option<TlsAcceptor>>`

# Calls

- [public_tls_paths_from_dashboard](../../../../../functions/LPE-CT/src/smtp/tls/public_tls_paths_from_dashboard.md)
- [smtp_starttls_acceptor_for_paths](../../../../../functions/LPE-CT/src/smtp/tls/smtp_starttls_acceptor_for_paths.md)

# Called by

- [run_smtp_listener](../../../../../functions/LPE-CT/src/smtp/run_smtp_listener.md)