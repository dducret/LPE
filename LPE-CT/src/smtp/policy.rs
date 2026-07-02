use super::RuntimeConfig;

#[derive(Debug, Clone, Copy)]
pub(super) struct InboundDomainPolicy {
    pub(super) rbl_checks: bool,
    pub(super) spf_checks: bool,
    pub(super) greylisting: bool,
}

pub(super) fn domain_part(address: &str) -> Option<String> {
    address
        .rsplit_once('@')
        .map(|(_, domain)| domain.trim().to_ascii_lowercase())
        .filter(|domain| !domain.is_empty())
}

pub(super) fn normalized(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase())
}

pub(super) fn matches_domain(expected: Option<&str>, actual: Option<&str>) -> bool {
    match expected.map(|value| value.trim().to_ascii_lowercase()) {
        Some(expected) if !expected.is_empty() => actual == Some(expected.as_str()),
        _ => true,
    }
}

pub(super) fn matches_any_domain(expected: Option<&str>, actual: &[String]) -> bool {
    match expected.map(|value| value.trim().to_ascii_lowercase()) {
        Some(expected) if !expected.is_empty() => actual.iter().any(|value| value == &expected),
        _ => true,
    }
}

pub(super) fn inbound_domain_policy(
    config: &RuntimeConfig,
    rcpt_to: &[String],
) -> InboundDomainPolicy {
    if config.accepted_domains.is_empty() {
        return InboundDomainPolicy {
            rbl_checks: true,
            spf_checks: true,
            greylisting: true,
        };
    }

    let mut policy = InboundDomainPolicy {
        rbl_checks: false,
        spf_checks: false,
        greylisting: false,
    };
    let mut matched = false;

    for recipient in rcpt_to {
        let Some(domain) = recipient.rsplit_once('@').map(|(_, domain)| domain.trim()) else {
            return InboundDomainPolicy {
                rbl_checks: true,
                spf_checks: true,
                greylisting: true,
            };
        };
        let Some(accepted) = config
            .accepted_domains
            .iter()
            .find(|accepted| accepted.verified && accepted.domain.eq_ignore_ascii_case(domain))
        else {
            return InboundDomainPolicy {
                rbl_checks: true,
                spf_checks: true,
                greylisting: true,
            };
        };

        matched = true;
        policy.rbl_checks |= accepted.rbl_checks;
        policy.spf_checks |= accepted.spf_checks;
        policy.greylisting |= accepted.greylisting;
    }

    if matched {
        policy
    } else {
        InboundDomainPolicy {
            rbl_checks: true,
            spf_checks: true,
            greylisting: true,
        }
    }
}

pub(super) fn recipient_domain_is_accepted(config: &RuntimeConfig, recipient: &str) -> bool {
    if config.accepted_domains.is_empty() {
        return false;
    }
    let Some(domain) = domain_part(recipient) else {
        return false;
    };
    accepted_domain_is_verified(config, &domain)
}

pub(super) fn accepted_domain_is_verified(config: &RuntimeConfig, domain: &str) -> bool {
    config
        .accepted_domains
        .iter()
        .any(|accepted| accepted.verified && accepted.domain.eq_ignore_ascii_case(&domain))
}

pub(super) fn recipient_domain_accepts_null_reverse_path(
    config: &RuntimeConfig,
    recipient: &str,
) -> bool {
    if config.accepted_domains.is_empty() {
        return false;
    }
    let Some(domain) = domain_part(recipient) else {
        return false;
    };
    config.accepted_domains.iter().any(|accepted| {
        accepted.verified
            && accepted.accept_null_reverse_path
            && accepted.domain.eq_ignore_ascii_case(&domain)
    })
}
