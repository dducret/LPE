use anyhow::{anyhow, bail, Result};
use lpe_storage::AuditEntryInput;
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

use crate::{
    auth::{authenticate, require_auth},
    parse::{as_string, read_request, single_string_arg, Argument},
    store::ManageSieveStore,
};

#[derive(Clone)]
pub struct ManageSieveServer<S> {
    store: S,
}

impl<S: ManageSieveStore> ManageSieveServer<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn serve(self, listener: TcpListener) -> Result<()> {
        loop {
            let (stream, _) = listener.accept().await?;
            let store = self.store.clone();
            tokio::spawn(async move {
                let _ = handle_connection(store, stream).await;
            });
        }
    }
}

pub async fn serve(listener: TcpListener, store: impl ManageSieveStore) -> Result<()> {
    ManageSieveServer::new(store).serve(listener).await
}

async fn handle_connection<S: ManageSieveStore>(store: S, stream: TcpStream) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    writer
        .write_all(b"OK \"LPE ManageSieve ready\"\r\n")
        .await?;
    let mut authenticated = None;

    loop {
        let request = match read_request(&mut reader).await? {
            Some(request) => request,
            None => return Ok(()),
        };
        let command = request.command.to_ascii_uppercase();
        match command.as_str() {
            "CAPABILITY" => write_capability(&mut writer).await?,
            "AUTHENTICATE" => {
                authenticated = Some(authenticate(&store, &request.arguments).await?);
                writer
                    .write_all(b"OK \"authentication successful\"\r\n")
                    .await?;
            }
            "NOOP" => writer.write_all(b"OK\r\n").await?,
            "LOGOUT" => {
                writer.write_all(b"OK \"logout\"\r\n").await?;
                return Ok(());
            }
            "HAVESPACE" => {
                require_auth(&authenticated)?;
                handle_havespace(&mut writer, &request.arguments).await?;
            }
            "LISTSCRIPTS" => {
                let account = require_auth(&authenticated)?;
                let scripts = store.list_sieve_scripts(account.account_id).await?;
                for script in scripts {
                    if script.is_active {
                        writer
                            .write_all(format!("\"{}\" ACTIVE\r\n", script.name).as_bytes())
                            .await?;
                    } else {
                        writer
                            .write_all(format!("\"{}\"\r\n", script.name).as_bytes())
                            .await?;
                    }
                }
                writer.write_all(b"OK\r\n").await?;
            }
            "GETSCRIPT" => {
                let account = require_auth(&authenticated)?;
                let name = single_string_arg(&request.arguments)?;
                let script = store
                    .get_sieve_script(account.account_id, &name)
                    .await?
                    .ok_or_else(|| anyhow!("script not found"))?;
                writer
                    .write_all(format!("{{{}}}\r\n", script.content.len()).as_bytes())
                    .await?;
                writer.write_all(script.content.as_bytes()).await?;
                writer.write_all(b"\r\nOK\r\n").await?;
            }
            "PUTSCRIPT" => {
                let account = require_auth(&authenticated)?;
                if request.arguments.len() != 2 {
                    bail!("PUTSCRIPT expects name and script literal");
                }
                let name = as_string(&request.arguments[0])?;
                let content = as_string(&request.arguments[1])?;
                store
                    .put_sieve_script(
                        account.account_id,
                        &name,
                        &content,
                        false,
                        AuditEntryInput {
                            actor: account.email.clone(),
                            action: "mail.sieve.put-script".to_string(),
                            subject: name.clone(),
                        },
                    )
                    .await?;
                writer.write_all(b"OK\r\n").await?;
            }
            "CHECKSCRIPT" => {
                require_auth(&authenticated)?;
                let content = single_string_arg(&request.arguments)?;
                lpe_core::sieve::parse_script(&content)?;
                writer.write_all(b"OK\r\n").await?;
            }
            "SETACTIVE" => {
                let account = require_auth(&authenticated)?;
                let name = single_string_arg(&request.arguments)?;
                let active = if name.is_empty() {
                    None
                } else {
                    Some(name.clone())
                };
                store
                    .set_active_sieve_script(
                        account.account_id,
                        active.as_deref(),
                        AuditEntryInput {
                            actor: account.email.clone(),
                            action: "mail.sieve.set-active".to_string(),
                            subject: if name.is_empty() {
                                "<none>".to_string()
                            } else {
                                name
                            },
                        },
                    )
                    .await?;
                writer.write_all(b"OK\r\n").await?;
            }
            "DELETESCRIPT" => {
                let account = require_auth(&authenticated)?;
                let name = single_string_arg(&request.arguments)?;
                store
                    .delete_sieve_script(
                        account.account_id,
                        &name,
                        AuditEntryInput {
                            actor: account.email.clone(),
                            action: "mail.sieve.delete-script".to_string(),
                            subject: name.clone(),
                        },
                    )
                    .await?;
                writer.write_all(b"OK\r\n").await?;
            }
            "RENAMESCRIPT" => {
                let account = require_auth(&authenticated)?;
                if request.arguments.len() != 2 {
                    bail!("RENAMESCRIPT expects old and new names");
                }
                let old_name = as_string(&request.arguments[0])?;
                let new_name = as_string(&request.arguments[1])?;
                store
                    .rename_sieve_script(
                        account.account_id,
                        &old_name,
                        &new_name,
                        AuditEntryInput {
                            actor: account.email.clone(),
                            action: "mail.sieve.rename-script".to_string(),
                            subject: format!("{old_name}->{new_name}"),
                        },
                    )
                    .await?;
                writer.write_all(b"OK\r\n").await?;
            }
            _ => bail!("unsupported ManageSieve command"),
        }
    }
}

async fn write_capability<W: AsyncWriteExt + Unpin>(writer: &mut W) -> Result<()> {
    writer
        .write_all(
            concat!(
                "\"IMPLEMENTATION\" \"LPE ManageSieve\"\r\n",
                "\"SASL\" \"PLAIN XOAUTH2\"\r\n",
                "\"SIEVE\" \"fileinto discard redirect vacation\"\r\n",
                "\"VERSION\" \"1.0\"\r\n",
                "OK\r\n"
            )
            .as_bytes(),
        )
        .await?;
    Ok(())
}

async fn handle_havespace<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    arguments: &[Argument],
) -> Result<()> {
    if arguments.len() != 2 {
        bail!("HAVESPACE expects name and size");
    }
    let size = match &arguments[1] {
        Argument::Atom(value) => value.parse::<usize>()?,
        _ => bail!("HAVESPACE size must be numeric"),
    };
    if size > 64 * 1024 {
        writer.write_all(b"NO \"script too large\"\r\n").await?;
    } else {
        writer.write_all(b"OK\r\n").await?;
    }
    Ok(())
}
