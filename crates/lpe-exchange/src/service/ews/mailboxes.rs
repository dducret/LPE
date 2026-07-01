use super::super::*;

#[derive(Debug, Clone)]
pub(in crate::service) struct ParsedMailbox {
    pub(in crate::service) address: String,
    pub(in crate::service) display_name: Option<String>,
}

pub(in crate::service) fn parse_recipients(
    message: &str,
    collection_name: &str,
) -> Vec<SubmittedRecipientInput> {
    element_content(message, collection_name)
        .map(|collection| {
            element_contents(collection, "Mailbox")
                .into_iter()
                .filter_map(parse_mailbox)
                .map(|mailbox| SubmittedRecipientInput {
                    address: mailbox.address,
                    display_name: mailbox.display_name,
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(in crate::service) fn parse_first_mailbox(value: &str) -> Option<ParsedMailbox> {
    element_contents(value, "Mailbox")
        .into_iter()
        .find_map(parse_mailbox)
}

pub(in crate::service) fn parse_mailbox(value: &str) -> Option<ParsedMailbox> {
    let address = element_text(value, "EmailAddress")?;
    if address.trim().is_empty() {
        return None;
    }
    Some(ParsedMailbox {
        address: address.trim().to_string(),
        display_name: element_text(value, "Name").filter(|name| !name.trim().is_empty()),
    })
}

pub(in crate::service) fn requested_mailbox_emails(request: &str) -> Vec<String> {
    let mut emails = element_contents(request, "Mailbox")
        .into_iter()
        .filter_map(|mailbox| element_text(mailbox, "EmailAddress"))
        .map(|email| email.trim().to_ascii_lowercase())
        .filter(|email| !email.is_empty())
        .collect::<Vec<_>>();
    emails.sort();
    emails.dedup();
    emails
}
