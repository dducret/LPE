use super::*;

pub(super) async fn hard_delete_recoverable_folder_contents<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<(Vec<u64>, bool), u32> {
    let items = snapshot.recoverable_items_for_folder(folder_id);
    if crate::mapi_store::recoverable_storage_folder(folder_id).is_none() {
        return Err(0x8004_010F);
    }
    let mut partial_completion = false;
    let mut changed_folder_ids = Vec::new();
    for item in items {
        if store
            .purge_recoverable_item(
                principal.account_id,
                item.canonical_id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-empty-recoverable-folder".to_string(),
                    subject: format!("recoverable:{}", item.canonical_id),
                },
            )
            .await
            .is_err()
        {
            partial_completion = true;
        } else {
            if changed_folder_ids.is_empty() {
                changed_folder_ids.push(folder_id);
            }
        }
    }
    Ok((changed_folder_ids, partial_completion))
}
