use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizationError {
    DomainNameRequired,
    InvalidDomainName,
    EmailMissingDomain,
    InvalidEmailLocalPart,
}

impl fmt::Display for NormalizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DomainNameRequired => formatter.write_str("domain name is required"),
            Self::InvalidDomainName => formatter.write_str("invalid domain name"),
            Self::EmailMissingDomain => formatter.write_str("email address must contain a domain"),
            Self::InvalidEmailLocalPart => {
                formatter.write_str("email address local part is invalid")
            }
        }
    }
}

impl std::error::Error for NormalizationError {}

pub fn normalize_domain_name(value: &str) -> Result<String, NormalizationError> {
    let trimmed = value.trim().trim_end_matches('.');
    if trimmed.is_empty() {
        return Err(NormalizationError::DomainNameRequired);
    }
    if trimmed
        .chars()
        .any(|ch| ch.is_control() || ch.is_whitespace())
    {
        return Err(NormalizationError::InvalidDomainName);
    }
    let ascii =
        idna::domain_to_ascii(trimmed).map_err(|_| NormalizationError::InvalidDomainName)?;
    let normalized = ascii.to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(NormalizationError::DomainNameRequired);
    }
    Ok(normalized)
}

pub fn normalize_mailbox_domain(value: &str) -> String {
    normalize_domain_name(value).unwrap_or_default()
}

pub fn normalize_email(value: &str) -> Result<String, NormalizationError> {
    let trimmed = value.trim();
    let Some((local_part, domain)) = trimmed.split_once('@') else {
        return Err(NormalizationError::EmailMissingDomain);
    };
    if local_part.trim().is_empty() || local_part.contains('@') {
        return Err(NormalizationError::InvalidEmailLocalPart);
    }
    let local_part = local_part.trim().to_lowercase();
    let domain = normalize_domain_name(domain)?;
    Ok(format!("{local_part}@{domain}"))
}

pub fn normalize_mailbox_email(value: &str) -> String {
    normalize_email(value).unwrap_or_default()
}

pub fn normalize_trimmed_lowercase(value: &str) -> String {
    value.trim().to_lowercase()
}

pub fn normalize_login_name(username: &str, hinted_user: Option<&str>) -> String {
    if username.contains('@') {
        normalize_trimmed_lowercase(username)
    } else {
        normalize_trimmed_lowercase(hinted_user.unwrap_or(username))
    }
}

pub fn normalize_calendar_email(value: &str) -> String {
    let trimmed = value.trim();
    trimmed
        .strip_prefix("mailto:")
        .unwrap_or(trimmed)
        .trim()
        .to_ascii_lowercase()
}

pub fn normalize_calendar_participation_status(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "accepted" => "accepted".to_string(),
        "declined" => "declined".to_string(),
        "tentative" => "tentative".to_string(),
        "delegated" => "delegated".to_string(),
        _ => "needs-action".to_string(),
    }
}

pub fn normalize_smtp_lookup_value(value: &str) -> String {
    let mut value = value.trim().trim_matches('\0').to_ascii_lowercase();
    if let Some(rest) = value.strip_prefix("=smtp:") {
        value = rest.to_string();
    } else if let Some(rest) = value.strip_prefix("smtp:") {
        value = rest.to_string();
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mailbox_email_normalizes_idna_domain_and_unicode_local_part() {
        assert_eq!(
            normalize_mailbox_email(" Jörg@Bücher.Example "),
            "jörg@xn--bcher-kva.example"
        );
    }

    #[test]
    fn login_name_uses_hint_for_unqualified_username() {
        assert_eq!(
            normalize_login_name("alice", Some(" Alice@Example.Test ")),
            "alice@example.test"
        );
        assert_eq!(
            normalize_login_name(" Bob@Example.Test ", None),
            "bob@example.test"
        );
    }

    #[test]
    fn calendar_email_strips_mailto_prefix() {
        assert_eq!(
            normalize_calendar_email(" mailto:Alice@Example.Test "),
            "alice@example.test"
        );
    }

    #[test]
    fn smtp_lookup_strips_transport_prefixes() {
        assert_eq!(
            normalize_smtp_lookup_value("\0=SMTP:Alice@Example.Test\0"),
            "alice@example.test"
        );
    }
}
