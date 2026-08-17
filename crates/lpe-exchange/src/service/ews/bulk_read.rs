use super::super::*;

const EWS_MARK_ALL_ITEMS_AS_READ_LIMIT: usize = 10_000;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn mark_all_items_as_read(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let FolderOperationTarget::Mailbox(folder_id) =
                parse_mark_all_items_as_read_target(request)?
            else {
                bail!("MarkAllItemsAsRead currently supports canonical mailbox folders only.");
            };
            if !self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?
                .iter()
                .any(|mailbox| mailbox.id == folder_id)
            {
                bail!("MarkAllItemsAsRead mailbox folder is not visible to the authenticated account.");
            }
            let read_flag = element_text(request, "ReadFlag")
                .map(|value| !value.eq_ignore_ascii_case("false"))
                .unwrap_or(true);
            self.store
                .mark_all_jmap_mailbox_messages_read(
                    principal.account_id,
                    folder_id,
                    !read_flag,
                    EWS_MARK_ALL_ITEMS_AS_READ_LIMIT,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "ews-mark-all-items-as-read".to_string(),
                        subject: folder_id.to_string(),
                    },
                )
                .await?;
            Ok(simple_operation_success_response("MarkAllItemsAsRead"))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "MarkAllItemsAsRead",
                "ErrorInvalidOperation",
                &error.to_string(),
            )
        }))
    }
}
