use anyhow::{bail, Result};
use lpe_magika::Detector;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

use crate::{parse::split_two, MessageRefKind, Session};

impl<S: crate::store::ImapStore, D: Detector> Session<S, D> {
    pub(crate) async fn handle_uid<R, W>(
        &mut self,
        _reader: &mut BufReader<R>,
        writer: &mut W,
        tag: &str,
        arguments: &str,
    ) -> Result<bool>
    where
        R: AsyncReadExt + Unpin,
        W: AsyncWriteExt + Unpin,
    {
        let (command, rest) = split_two(arguments)?;
        match command.to_ascii_uppercase().as_str() {
            "FETCH" => {
                self.handle_fetch(tag, rest, writer, MessageRefKind::Uid)
                    .await
            }
            "STORE" => {
                self.handle_store(tag, rest, writer, MessageRefKind::Uid)
                    .await
            }
            "SEARCH" => {
                self.handle_search(tag, rest, writer, MessageRefKind::Uid)
                    .await
            }
            "COPY" => {
                self.handle_copy(tag, rest, writer, MessageRefKind::Uid)
                    .await
            }
            "MOVE" => {
                self.handle_move(tag, rest, writer, MessageRefKind::Uid)
                    .await
            }
            "EXPUNGE" => self.handle_uid_expunge(tag, writer).await,
            other => bail!("UID {} is not supported", other),
        }
    }

    async fn handle_uid_expunge<W>(&mut self, tag: &str, writer: &mut W) -> Result<bool>
    where
        W: AsyncWriteExt + Unpin,
    {
        self.require_selected()?;
        self.refresh_selected().await?;
        writer
            .write_all(format!("{tag} OK UID EXPUNGE completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }
}
