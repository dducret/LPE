use anyhow::{anyhow, Result};
use email_auth::{
    dkim::DkimResult, dmarc::Disposition as DmarcDisposition, spf::SpfResult, EmailAuthenticator,
};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

use super::{DecisionTraceEntry, SystemDnsResolver};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(in crate::smtp) struct AuthSummary {
    pub(in crate::smtp) spf: String,
    pub(in crate::smtp) dkim: String,
    pub(in crate::smtp) dmarc: String,
}

#[derive(Debug, Clone)]
pub(in crate::smtp) struct AuthenticationAssessment {
    pub(in crate::smtp) spf: SpfDisposition,
    pub(in crate::smtp) dkim: DkimDisposition,
    pub(in crate::smtp) dkim_aligned: bool,
    pub(in crate::smtp) spf_aligned: bool,
    pub(in crate::smtp) dmarc: DmarcDisposition,
    pub(in crate::smtp) from_domain: String,
    pub(in crate::smtp) spf_domain: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::smtp) enum SpfDisposition {
    Pass,
    Fail,
    SoftFail,
    Neutral,
    None,
    TempError,
    PermError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::smtp) enum DkimDisposition {
    Pass,
    Fail,
    None,
    TempFail,
    PermFail,
}

impl AuthenticationAssessment {
    pub(in crate::smtp) fn has_temporary_failure(&self) -> bool {
        matches!(self.spf, SpfDisposition::TempError)
            || matches!(self.dkim, DkimDisposition::TempFail)
            || matches!(self.dmarc, DmarcDisposition::TempFail)
    }
}

pub(in crate::smtp) fn apply_authentication_scores(
    assessment: &AuthenticationAssessment,
    spam_score: &mut f32,
    security_score: &mut f32,
    decision_trace: &mut Vec<DecisionTraceEntry>,
) {
    match assessment.spf {
        SpfDisposition::SoftFail => {
            *spam_score += 1.5;
            decision_trace.push(DecisionTraceEntry {
                stage: "spf".to_string(),
                outcome: "softfail".to_string(),
                detail: "SPF softfail increases spam score without forcing a reject".to_string(),
            });
        }
        SpfDisposition::Fail => {
            *security_score += 2.5;
        }
        SpfDisposition::PermError => {
            *security_score += 1.5;
            decision_trace.push(DecisionTraceEntry {
                stage: "spf".to_string(),
                outcome: "permerror".to_string(),
                detail: "SPF record is malformed or exceeded processing limits".to_string(),
            });
        }
        SpfDisposition::TempError => {
            *security_score += 1.0;
        }
        _ => {}
    }

    match assessment.dkim {
        DkimDisposition::Fail => {
            *spam_score += 1.0;
            *security_score += 1.0;
        }
        DkimDisposition::PermFail => {
            *spam_score += 1.5;
            *security_score += 1.5;
            decision_trace.push(DecisionTraceEntry {
                stage: "dkim".to_string(),
                outcome: "permfail".to_string(),
                detail: "DKIM signature or key policy is structurally invalid".to_string(),
            });
        }
        DkimDisposition::TempFail => {
            *security_score += 1.0;
        }
        _ => {}
    }

    match assessment.dmarc {
        DmarcDisposition::Quarantine => {
            *spam_score += 3.0;
            *security_score += 1.0;
        }
        DmarcDisposition::Reject => {
            *security_score += 4.0;
        }
        DmarcDisposition::TempFail => {
            *security_score += 2.0;
        }
        _ => {}
    }

    if !assessment.spf_aligned {
        decision_trace.push(DecisionTraceEntry {
            stage: "spf-alignment".to_string(),
            outcome: "misaligned".to_string(),
            detail: format!(
                "RFC 5322 From domain {} is not aligned with SPF domain {}",
                assessment.from_domain, assessment.spf_domain
            ),
        });
    }
    if !assessment.dkim_aligned {
        decision_trace.push(DecisionTraceEntry {
            stage: "dkim-alignment".to_string(),
            outcome: "misaligned".to_string(),
            detail: format!(
                "no aligned DKIM signature passed for RFC 5322 From domain {}",
                assessment.from_domain
            ),
        });
    }
}

pub(in crate::smtp) fn spf_disposition(result: &SpfResult) -> SpfDisposition {
    match result {
        SpfResult::Pass => SpfDisposition::Pass,
        SpfResult::Fail { .. } => SpfDisposition::Fail,
        SpfResult::SoftFail => SpfDisposition::SoftFail,
        SpfResult::Neutral => SpfDisposition::Neutral,
        SpfResult::None => SpfDisposition::None,
        SpfResult::TempError => SpfDisposition::TempError,
        SpfResult::PermError => SpfDisposition::PermError,
    }
}

pub(in crate::smtp) fn dkim_disposition(results: &[DkimResult]) -> DkimDisposition {
    if results
        .iter()
        .any(|result| matches!(result, DkimResult::Pass { .. }))
    {
        DkimDisposition::Pass
    } else if results
        .iter()
        .any(|result| matches!(result, DkimResult::TempFail { .. }))
    {
        DkimDisposition::TempFail
    } else if results
        .iter()
        .any(|result| matches!(result, DkimResult::PermFail { .. }))
    {
        DkimDisposition::PermFail
    } else if results
        .iter()
        .any(|result| matches!(result, DkimResult::Fail { .. }))
    {
        DkimDisposition::Fail
    } else {
        DkimDisposition::None
    }
}

pub(in crate::smtp) fn summarize_spf(result: &SpfResult) -> String {
    match result {
        SpfResult::Pass => "pass".to_string(),
        SpfResult::Fail { explanation } => match explanation {
            Some(explanation) if !explanation.trim().is_empty() => {
                format!("fail ({})", explanation.trim())
            }
            _ => "fail".to_string(),
        },
        SpfResult::SoftFail => "softfail".to_string(),
        SpfResult::Neutral => "neutral".to_string(),
        SpfResult::None => "none".to_string(),
        SpfResult::TempError => "temperror".to_string(),
        SpfResult::PermError => "permerror".to_string(),
    }
}

pub(in crate::smtp) fn summarize_dkim(results: &[DkimResult], aligned: bool) -> String {
    match dkim_disposition(results) {
        DkimDisposition::Pass if aligned => "pass (aligned)".to_string(),
        DkimDisposition::Pass => "pass (unaligned)".to_string(),
        DkimDisposition::Fail => "fail".to_string(),
        DkimDisposition::TempFail => "temperror".to_string(),
        DkimDisposition::PermFail => "permerror".to_string(),
        DkimDisposition::None => "none".to_string(),
    }
}

pub(in crate::smtp) fn summarize_dmarc(result: DmarcDisposition) -> String {
    match result {
        DmarcDisposition::Pass => "pass".to_string(),
        DmarcDisposition::Quarantine => "quarantine".to_string(),
        DmarcDisposition::Reject => "reject".to_string(),
        DmarcDisposition::None => "none".to_string(),
        DmarcDisposition::TempFail => "temperror".to_string(),
    }
}

pub(in crate::smtp) async fn authenticate_message(
    client_ip: IpAddr,
    helo: &str,
    mail_from: &str,
    message_bytes: &[u8],
) -> Result<(
    AuthSummary,
    Vec<DecisionTraceEntry>,
    AuthenticationAssessment,
)> {
    let authenticator = EmailAuthenticator::new(SystemDnsResolver::new()?, "lpe-ct.local");
    let result = authenticator
        .authenticate(message_bytes, client_ip, helo, mail_from)
        .await
        .map_err(|error| anyhow!("authentication evaluation failed: {error}"))?;

    let spf = summarize_spf(&result.spf);
    let dkim = summarize_dkim(&result.dkim, result.dmarc.dkim_aligned);
    let dmarc = summarize_dmarc(result.dmarc.disposition);
    let assessment = AuthenticationAssessment {
        spf: spf_disposition(&result.spf),
        dkim: dkim_disposition(&result.dkim),
        dkim_aligned: result.dmarc.dkim_aligned,
        spf_aligned: result.dmarc.spf_aligned,
        dmarc: result.dmarc.disposition,
        from_domain: result.from_domain.clone(),
        spf_domain: result.spf_domain.clone(),
    };
    let mut trace = vec![
        DecisionTraceEntry {
            stage: "spf".to_string(),
            outcome: spf.clone(),
            detail: format!(
                "SPF evaluation for envelope sender {} from {} using domain {}",
                mail_from, client_ip, result.spf_domain
            ),
        },
        DecisionTraceEntry {
            stage: "dkim".to_string(),
            outcome: dkim.clone(),
            detail: format!(
                "DKIM verification executed on the RFC 5322 message (aligned={})",
                result.dmarc.dkim_aligned
            ),
        },
        DecisionTraceEntry {
            stage: "dmarc".to_string(),
            outcome: dmarc.clone(),
            detail: format!(
                "DMARC evaluation executed for RFC 5322 From domain {} (spf_aligned={}, dkim_aligned={})",
                result.from_domain, result.dmarc.spf_aligned, result.dmarc.dkim_aligned
            ),
        },
    ];

    if assessment.has_temporary_failure() {
        trace.push(DecisionTraceEntry {
            stage: "authentication".to_string(),
            outcome: "temperror".to_string(),
            detail: "one of SPF, DKIM, or DMARC encountered a temporary failure".to_string(),
        });
    }

    Ok((AuthSummary { spf, dkim, dmarc }, trace, assessment))
}
