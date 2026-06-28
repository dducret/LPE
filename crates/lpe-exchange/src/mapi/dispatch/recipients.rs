use super::*;

pub(super) fn submitted_recipients_from_pending(
    recipients: &[PendingRecipient],
) -> (
    Vec<SubmittedRecipientInput>,
    Vec<SubmittedRecipientInput>,
    Vec<SubmittedRecipientInput>,
) {
    let mut to = Vec::new();
    let mut cc = Vec::new();
    let mut bcc = Vec::new();
    for recipient in recipients {
        let value = SubmittedRecipientInput {
            address: recipient.address.clone(),
            display_name: recipient.display_name.clone(),
        };
        match recipient.recipient_type & 0x0F {
            0x02 => cc.push(value),
            0x03 => bcc.push(value),
            _ => to.push(value),
        }
    }
    (to, cc, bcc)
}

pub(super) fn pending_recipients_from_email(email: &JmapEmail) -> Vec<PendingRecipient> {
    message_recipients(email)
        .into_iter()
        .map(|recipient| PendingRecipient {
            row_id: recipient.order,
            recipient_type: recipient.recipient_type,
            address: recipient.address.address.clone(),
            display_name: recipient.address.display_name.clone(),
        })
        .collect()
}
