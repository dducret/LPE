---
type: Rust Function
title: run_smtp_command_loop
resource: LPE-CT/src/smtp/session.rs#L121-L162
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
  - functions/LPE-CT/src/smtp/protocol/write_smtp
  called_by:
  - functions/LPE-CT/src/smtp/session/handle_smtp_session
---

# Signature

`pub(in crate::smtp) async fn run_smtp_command_loop<R, W>( client: &reqwest::Client, reader: &mut BufReader<R>, writer: &mut W, dashboard_store: &Arc<Mutex<crate::DashboardState>>, spool_dir: &Path, peer: SocketAddr, mut transaction: SmtpTransaction, ) -> Result<()> where R: tokio::io::AsyncRead + Unpin, W: AsyncWrite + Unpin,`

# Calls

- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)
- [write_smtp](../../../../../functions/LPE-CT/src/smtp/protocol/write_smtp.md)

# Called by

- [handle_smtp_session](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_session.md)