use super::super::*;

pub(in crate::service) fn message_response_in_target(
    mut email: JmapEmail,
    target: &JmapMailbox,
) -> JmapEmail {
    email.mailbox_id = target.id;
    email.mailbox_role = target.role.clone();
    email.mailbox_name = target.name.clone();
    if !email.mailbox_ids.contains(&target.id) {
        email.mailbox_ids.push(target.id);
    }
    email
}

pub(in crate::service) fn message_responses_in_target(
    emails: Vec<JmapEmail>,
    target: &JmapMailbox,
) -> Vec<JmapEmail> {
    emails
        .into_iter()
        .map(|email| message_response_in_target(email, target))
        .collect()
}
