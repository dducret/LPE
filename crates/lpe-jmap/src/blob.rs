use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use lpe_storage::AuthenticatedAccount;

use crate::{error::set_error, JmapService};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BlobCopyArguments {
    account_id: String,
    from_account_id: String,
    blob_ids: Vec<String>,
}

impl<S: crate::store::JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) async fn handle_blob_copy(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: BlobCopyArguments = serde_json::from_value(arguments)?;
        let target_account = self
            .requested_account_access(account, Some(&arguments.account_id))
            .await?;
        let source_account = self
            .requested_account_access(account, Some(&arguments.from_account_id))
            .await?;
        let mut copied = Map::new();
        let mut not_copied = Map::new();

        for blob_id in arguments.blob_ids {
            match self.resolve_download_blob(&source_account, &blob_id).await {
                Ok(blob) => {
                    match self
                        .store
                        .save_jmap_upload_blob(
                            target_account.account_id,
                            &blob.media_type,
                            &blob.blob_bytes,
                        )
                        .await
                    {
                        Ok(copied_blob) => {
                            copied.insert(
                                blob_id,
                                json!({
                                    "accountId": target_account.account_id.to_string(),
                                    "blobId": format!("upload:{}", copied_blob.id),
                                    "type": copied_blob.media_type,
                                    "size": copied_blob.octet_size,
                                }),
                            );
                        }
                        Err(error) => {
                            not_copied.insert(blob_id, set_error(&error.to_string()));
                        }
                    }
                }
                Err(error) => {
                    not_copied.insert(blob_id, set_error(&error.to_string()));
                }
            }
        }

        Ok(json!({
            "fromAccountId": source_account.account_id.to_string(),
            "accountId": target_account.account_id.to_string(),
            "copied": Value::Object(copied),
            "notCopied": Value::Object(not_copied),
        }))
    }
}
