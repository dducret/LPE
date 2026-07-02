use super::*;

pub(in crate::smtp) async fn evaluate_inbound_policy(
    spool_dir: &Path,
    config: &RuntimeConfig,
    peer_ip: Option<IpAddr>,
    helo: &str,
    mail_from: &str,
    rcpt_to: &[String],
    message_bytes: &[u8],
) -> Result<FilterVerdict> {
    let mut spam_score = 0.0;
    let mut security_score = 0.0;
    let mut decision_trace = Vec::new();
    let mut dnsbl = DnsblOutcome::default();
    let mut auth_summary = AuthSummary::default();
    let mut auth_assessment = None;
    let defer_reasons = Vec::new();
    let mut reject_reasons = Vec::new();
    let mut quarantine_reasons = Vec::new();
    let domain_policy = inbound_domain_policy(config, rcpt_to);
    let reputation_score = if config.reputation_enabled {
        load_reputation_score(spool_dir, config, peer_ip, mail_from).await?
    } else {
        0
    };

    if config.quarantine_enabled && should_quarantine(message_bytes) {
        let reasons = vec!["message matched local quarantine policy".to_string()];
        decision_trace.push(DecisionTraceEntry {
            stage: "manual-quarantine".to_string(),
            outcome: "quarantine".to_string(),
            detail: "message matched the explicit quarantine marker policy".to_string(),
        });
        return Ok(FilterVerdict {
            action: FilterAction::Quarantine,
            reason: Some(reasons.join("; ")),
            spam_score: config.spam_quarantine_threshold.max(1.0),
            security_score: 1.0,
            reputation_score,
            dnsbl_hits: dnsbl.hits,
            auth_summary,
            decision_trace,
        });
    }

    decision_trace.push(DecisionTraceEntry {
        stage: "pipeline".to_string(),
        outcome: "start".to_string(),
        detail: "running inbound edge pipeline: rbl/dns, bayespam, antivirus chain, final scoring"
            .to_string(),
    });

    if let Some(ip) = peer_ip {
        if config.greylisting_enabled && domain_policy.greylisting {
            match evaluate_greylisting(spool_dir, config, ip, mail_from, rcpt_to).await? {
                Some(reason) => {
                    decision_trace.push(DecisionTraceEntry {
                        stage: "greylisting".to_string(),
                        outcome: "defer".to_string(),
                        detail: reason.clone(),
                    });
                    spam_score += 1.5;
                    return Ok(FilterVerdict {
                        action: FilterAction::Defer,
                        reason: Some(reason),
                        spam_score,
                        security_score,
                        reputation_score,
                        dnsbl_hits: dnsbl.hits,
                        auth_summary,
                        decision_trace,
                    });
                }
                None => {
                    decision_trace.push(DecisionTraceEntry {
                        stage: "greylisting".to_string(),
                        outcome: "pass".to_string(),
                        detail: "triplet already aged through greylisting".to_string(),
                    });
                }
            }
        } else if config.greylisting_enabled {
            decision_trace.push(DecisionTraceEntry {
                stage: "greylisting".to_string(),
                outcome: "skipped".to_string(),
                detail: "greylisting disabled for the accepted recipient domain".to_string(),
            });
        }

        if config.dnsbl_enabled && domain_policy.rbl_checks {
            dnsbl = query_dnsbl(ip, &config.dnsbl_zones).await;
            if !dnsbl.hits.is_empty() {
                spam_score += 4.0 + dnsbl.hits.len() as f32;
                security_score += 2.0;
                decision_trace.push(DecisionTraceEntry {
                    stage: "rbl-dns-check".to_string(),
                    outcome: "listed".to_string(),
                    detail: format!("source IP listed on {}", dnsbl.hits.join(", ")),
                });
            } else {
                decision_trace.push(DecisionTraceEntry {
                    stage: "rbl-dns-check".to_string(),
                    outcome: "clear".to_string(),
                    detail: "source IP not listed on configured DNSBL zones".to_string(),
                });
            }
            if !dnsbl.tempfail_zones.is_empty() {
                security_score += 0.5;
                decision_trace.push(DecisionTraceEntry {
                    stage: "rbl-dns-check".to_string(),
                    outcome: "temperror".to_string(),
                    detail: format!(
                        "temporary DNS failure while querying {}",
                        dnsbl.tempfail_zones.join(", ")
                    ),
                });
            }
        } else if config.dnsbl_enabled {
            decision_trace.push(DecisionTraceEntry {
                stage: "rbl-dns-check".to_string(),
                outcome: "skipped".to_string(),
                detail: "RBL checks disabled for the accepted recipient domain".to_string(),
            });
        }

        if domain_policy.spf_checks {
            match authenticate_message(ip, helo, mail_from, message_bytes).await {
                Ok((summary, auth_trace, assessment)) => {
                    auth_summary = summary;
                    auth_assessment = Some(assessment.clone());
                    decision_trace.extend(auth_trace);
                    apply_authentication_scores(
                        &assessment,
                        &mut spam_score,
                        &mut security_score,
                        &mut decision_trace,
                    );
                }
                Err(error) => {
                    security_score += 1.0;
                    decision_trace.push(DecisionTraceEntry {
                        stage: "authentication".to_string(),
                        outcome: "temperror".to_string(),
                        detail: format!(
                            "authentication checks failed open with resolver error: {error}"
                        ),
                    });
                }
            }
        } else {
            decision_trace.push(DecisionTraceEntry {
                stage: "authentication".to_string(),
                outcome: "skipped".to_string(),
                detail: "SPF/DKIM/DMARC checks disabled for the accepted recipient domain"
                    .to_string(),
            });
        }
    } else {
        decision_trace.push(DecisionTraceEntry {
            stage: "authentication".to_string(),
            outcome: "skipped".to_string(),
            detail: "source peer IP could not be parsed for SPF, DKIM, and DMARC evaluation"
                .to_string(),
        });
    }

    let subject = parse_rfc822_header_value(message_bytes, "subject").unwrap_or_default();
    let visible_text = extract_visible_text(message_bytes)?;
    match score_bayespam(spool_dir, config, &subject, &visible_text, mail_from, helo).await? {
        Some(outcome) => {
            spam_score += outcome.contribution;
            decision_trace.push(DecisionTraceEntry {
                stage: "bayespam".to_string(),
                outcome: if outcome.probability >= 0.90 {
                    "spam"
                } else if outcome.probability >= 0.70 {
                    "suspect"
                } else {
                    "ham"
                }
                .to_string(),
                detail: format!(
                    "bayespam probability {:.3} using {} learned tokens (contribution={:.2})",
                    outcome.probability, outcome.matched_tokens, outcome.contribution
                ),
            });
        }
        None if config.bayespam_enabled => {
            decision_trace.push(DecisionTraceEntry {
                stage: "bayespam".to_string(),
                outcome: "skipped".to_string(),
                detail: "bayespam corpus is not trained enough for scoring yet".to_string(),
            });
        }
        None => {
            decision_trace.push(DecisionTraceEntry {
                stage: "bayespam".to_string(),
                outcome: "disabled".to_string(),
                detail: "bayespam disabled by local policy".to_string(),
            });
        }
    }
    let antivirus_verdict = evaluate_antivirus_policy(config, "inbound", message_bytes).await?;
    spam_score += antivirus_verdict.spam_score_delta;
    security_score += antivirus_verdict.security_score_delta;
    if antivirus_verdict.action == FilterAction::Quarantine {
        if let Some(reason) = antivirus_verdict.reason.clone() {
            quarantine_reasons.push(reason);
        }
    }
    decision_trace.extend(antivirus_verdict.decision_trace);

    if reputation_score < 0 {
        spam_score += (-reputation_score) as f32 * 0.35;
        security_score += (-reputation_score) as f32 * 0.10;
        decision_trace.push(DecisionTraceEntry {
            stage: "reputation".to_string(),
            outcome: "negative".to_string(),
            detail: format!("historical reputation score is {}", reputation_score),
        });
    } else {
        decision_trace.push(DecisionTraceEntry {
            stage: "reputation".to_string(),
            outcome: "neutral".to_string(),
            detail: format!("historical reputation score is {}", reputation_score),
        });
    }

    if config.reputation_enabled && reputation_score <= config.reputation_reject_threshold {
        reject_reasons.push(format!(
            "reputation score {} reached reject threshold {}",
            reputation_score, config.reputation_reject_threshold
        ));
        decision_trace.push(DecisionTraceEntry {
            stage: "reputation".to_string(),
            outcome: "reject".to_string(),
            detail: format!(
                "historical reputation score {} reached reject threshold {}",
                reputation_score, config.reputation_reject_threshold
            ),
        });
    } else if config.reputation_enabled
        && reputation_score <= config.reputation_quarantine_threshold
    {
        quarantine_reasons.push(format!(
            "reputation score {} reached quarantine threshold {}",
            reputation_score, config.reputation_quarantine_threshold
        ));
        decision_trace.push(DecisionTraceEntry {
            stage: "reputation".to_string(),
            outcome: "quarantine".to_string(),
            detail: format!(
                "historical reputation score {} reached quarantine threshold {}",
                reputation_score, config.reputation_quarantine_threshold
            ),
        });
    }

    let (action, reason) = finalize_policy_decision(
        config,
        auth_assessment.as_ref(),
        spam_score,
        security_score,
        reputation_score,
        &mut decision_trace,
        defer_reasons,
        reject_reasons,
        quarantine_reasons,
    );

    Ok(FilterVerdict {
        action,
        reason,
        spam_score,
        security_score,
        reputation_score,
        dnsbl_hits: dnsbl.hits,
        auth_summary,
        decision_trace,
    })
}

pub(in crate::smtp) fn finalize_policy_decision(
    config: &RuntimeConfig,
    auth_assessment: Option<&AuthenticationAssessment>,
    spam_score: f32,
    security_score: f32,
    reputation_score: i32,
    decision_trace: &mut Vec<DecisionTraceEntry>,
    mut defer_reasons: Vec<String>,
    mut reject_reasons: Vec<String>,
    mut quarantine_reasons: Vec<String>,
) -> (FilterAction, Option<String>) {
    if config.defer_on_auth_tempfail
        && auth_assessment.is_some_and(AuthenticationAssessment::has_temporary_failure)
    {
        defer_reasons.push("authentication dependency temporarily failed".to_string());
    }
    if config.require_dmarc_enforcement
        && auth_assessment.is_some_and(|assessment| assessment.dmarc == DmarcDisposition::Reject)
    {
        reject_reasons.push("DMARC policy requested reject".to_string());
    }
    if config.require_dmarc_enforcement
        && auth_assessment
            .is_some_and(|assessment| assessment.dmarc == DmarcDisposition::Quarantine)
    {
        quarantine_reasons.push("DMARC policy requested quarantine".to_string());
    }
    if config.require_spf
        && auth_assessment.is_some_and(|assessment| {
            assessment.spf == SpfDisposition::Fail && !assessment.dkim_aligned
        })
    {
        reject_reasons.push("SPF failed and no aligned DKIM signature passed".to_string());
    }
    if config.require_dkim_alignment
        && auth_assessment.is_some_and(|assessment| !assessment.dkim_aligned)
    {
        quarantine_reasons.push("aligned DKIM verification did not pass".to_string());
    }
    if spam_score >= config.spam_reject_threshold {
        reject_reasons.push(format!(
            "spam score {:.1} reached reject threshold {:.1}",
            spam_score, config.spam_reject_threshold
        ));
    } else if spam_score >= config.spam_quarantine_threshold {
        quarantine_reasons.push(format!(
            "spam score {:.1} reached quarantine threshold {:.1}",
            spam_score, config.spam_quarantine_threshold
        ));
    }

    decision_trace.push(DecisionTraceEntry {
        stage: "final-score".to_string(),
        outcome: "calculated".to_string(),
        detail: format!(
            "spam_score={spam_score:.1} security_score={security_score:.1} reputation_score={reputation_score}"
        ),
    });

    for reason in &defer_reasons {
        decision_trace.push(DecisionTraceEntry {
            stage: "policy-trigger".to_string(),
            outcome: "defer".to_string(),
            detail: reason.clone(),
        });
    }
    for reason in &reject_reasons {
        decision_trace.push(DecisionTraceEntry {
            stage: "policy-trigger".to_string(),
            outcome: "reject".to_string(),
            detail: reason.clone(),
        });
    }
    for reason in &quarantine_reasons {
        decision_trace.push(DecisionTraceEntry {
            stage: "policy-trigger".to_string(),
            outcome: "quarantine".to_string(),
            detail: reason.clone(),
        });
    }

    let (action, reasons) = if !defer_reasons.is_empty() {
        (FilterAction::Defer, defer_reasons)
    } else if !reject_reasons.is_empty() {
        (FilterAction::Reject, reject_reasons)
    } else if !quarantine_reasons.is_empty() {
        (FilterAction::Quarantine, quarantine_reasons)
    } else {
        (FilterAction::Accept, Vec::new())
    };

    let reason = if reasons.is_empty() {
        None
    } else {
        Some(reasons.join("; "))
    };

    decision_trace.push(DecisionTraceEntry {
        stage: "final-policy".to_string(),
        outcome: match action {
            FilterAction::Accept => "accept",
            FilterAction::Quarantine => "quarantine",
            FilterAction::Reject => "reject",
            FilterAction::Defer => "defer",
        }
        .to_string(),
        detail: reason.clone().unwrap_or_else(|| {
            format!(
                "message passed SMTP perimeter policy (spam_score={spam_score:.1}, security_score={security_score:.1})"
            )
        }),
    });

    (action, reason)
}

pub(in crate::smtp) fn apply_filter_verdict(message: &mut QueuedMessage, verdict: &FilterVerdict) {
    message.spam_score = verdict.spam_score;
    message.security_score = verdict.security_score;
    message.reputation_score = verdict.reputation_score;
    message.dnsbl_hits = verdict.dnsbl_hits.clone();
    message.auth_summary = verdict.auth_summary.clone();
    message
        .decision_trace
        .extend(verdict.decision_trace.clone());
    if let Some(reason) = &verdict.reason {
        message.relay_error = Some(reason.clone());
    }
}
