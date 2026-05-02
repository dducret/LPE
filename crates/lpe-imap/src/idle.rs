use anyhow::{bail, Result};
use lpe_magika::Detector;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    time::{timeout, Duration},
};

use crate::{render::render_selected_updates, Session};

impl<S: crate::store::ImapStore, D: Detector> Session<S, D> {
    pub(crate) async fn handle_idle<R, W>(
        &mut self,
        reader: &mut BufReader<R>,
        writer: &mut W,
        tag: &str,
    ) -> Result<bool>
    where
        R: AsyncReadExt + Unpin,
        W: AsyncWriteExt + Unpin,
    {
        self.require_auth()?;
        let mut previous = self.selected.clone();
        writer.write_all(b"+ idling\r\n").await?;
        writer.flush().await?;

        loop {
            let mut line = String::new();
            match timeout(Duration::from_secs(1), reader.read_line(&mut line)).await {
                Ok(Ok(0)) => return Ok(false),
                Ok(Ok(_)) => {
                    if line
                        .trim_end_matches(['\r', '\n'])
                        .eq_ignore_ascii_case("DONE")
                    {
                        break;
                    }
                    bail!("IDLE expects DONE to terminate");
                }
                Ok(Err(error)) => return Err(anyhow::Error::from(error)),
                Err(_) => {
                    if previous.is_some() {
                        self.refresh_selected().await?;
                        let current = self.selected.clone();
                        if let (Some(previous_selected), Some(current_selected)) =
                            (previous.as_ref(), current.as_ref())
                        {
                            let updates =
                                render_selected_updates(previous_selected, current_selected)?;
                            if !updates.is_empty() {
                                writer.write_all(updates.as_bytes()).await?;
                                writer.flush().await?;
                            }
                        }
                        previous = current;
                    }
                }
            }
        }

        writer
            .write_all(format!("{tag} OK IDLE completed\r\n").as_bytes())
            .await?;
        writer.flush().await?;
        Ok(true)
    }
}
