use lpe_magika::parse_rfc822_header_value;

const OUTLOOK_TEST_SUBJECT: &str = "Microsoft Outlook Test Message";
const OUTLOOK_TEST_BODY: &str =
    "This is an email message sent automatically by Microsoft Outlook while testing the settings for your account.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutlookTestMessage {
    pub(crate) subject: String,
    pub(crate) from_header: String,
    pub(crate) mail_from: String,
    pub(crate) rcpt_to: Vec<String>,
}

pub(crate) fn classify_smtp_message(
    mail_from: &str,
    rcpt_to: &[String],
    raw_message: &[u8],
) -> Option<OutlookTestMessage> {
    let subject = parse_rfc822_header_value(raw_message, "subject")?;
    if !subject.trim().eq_ignore_ascii_case(OUTLOOK_TEST_SUBJECT) {
        return None;
    }

    let from_header = parse_rfc822_header_value(raw_message, "from").unwrap_or_default();
    let body = String::from_utf8_lossy(raw_message);
    if !body.contains(OUTLOOK_TEST_BODY) && !from_header.contains("Microsoft Outlook") {
        return None;
    }

    Some(OutlookTestMessage {
        subject,
        from_header,
        mail_from: mail_from.to_string(),
        rcpt_to: rcpt_to.to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::classify_smtp_message;

    #[test]
    fn detects_outlook_account_test_message() {
        let raw = b"From: Microsoft Outlook <test@l-p-e.ch>\r\nTo: test@l-p-e.ch\r\nSubject: Microsoft Outlook Test Message\r\n\r\nThis is an email message sent automatically by Microsoft Outlook while testing the settings for your account.\r\n";

        let detected =
            classify_smtp_message("test@l-p-e.ch", &["test@l-p-e.ch".to_string()], raw).unwrap();

        assert_eq!(detected.subject, "Microsoft Outlook Test Message");
        assert_eq!(detected.mail_from, "test@l-p-e.ch");
        assert_eq!(detected.rcpt_to, vec!["test@l-p-e.ch"]);
        assert!(detected.from_header.contains("Microsoft Outlook"));
    }

    #[test]
    fn ignores_unrelated_messages() {
        let raw = b"From: Sender <test@l-p-e.ch>\r\nSubject: Microsoft Outlook Test Message\r\n\r\nManual probe\r\n";

        assert!(
            classify_smtp_message("test@l-p-e.ch", &["test@l-p-e.ch".to_string()], raw).is_none()
        );
    }
}
