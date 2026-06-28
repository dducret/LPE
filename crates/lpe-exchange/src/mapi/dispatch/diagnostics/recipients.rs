use crate::mapi::session::PendingRecipientChange;

pub(in crate::mapi::dispatch) fn pending_recipient_upsert_count(
    changes: &[PendingRecipientChange],
) -> usize {
    changes
        .iter()
        .filter(|change| matches!(change, PendingRecipientChange::Upsert(_)))
        .count()
}

pub(in crate::mapi::dispatch) fn pending_recipient_delete_count(
    changes: &[PendingRecipientChange],
) -> usize {
    changes
        .iter()
        .filter(|change| matches!(change, PendingRecipientChange::Delete(_)))
        .count()
}

pub(in crate::mapi::dispatch) fn pending_recipient_types_summary(
    changes: &[PendingRecipientChange],
) -> String {
    changes
        .iter()
        .filter_map(|change| match change {
            PendingRecipientChange::Upsert(recipient) => Some(recipient.recipient_type.to_string()),
            PendingRecipientChange::Delete(_) => None,
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(in crate::mapi::dispatch) fn pending_recipient_row_ids_summary(
    changes: &[PendingRecipientChange],
) -> String {
    changes
        .iter()
        .map(|change| match change {
            PendingRecipientChange::Upsert(recipient) => recipient.row_id.to_string(),
            PendingRecipientChange::Delete(row_id) => format!("delete:{row_id}"),
        })
        .collect::<Vec<_>>()
        .join(",")
}
