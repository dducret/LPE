use super::*;

#[derive(Debug, Clone)]
pub(crate) struct AntivirusProviderConfig {
    pub(in crate::smtp) id: String,
    pub(in crate::smtp) display_name: String,
    pub(in crate::smtp) command: String,
    pub(in crate::smtp) args: Vec<String>,
    pub(in crate::smtp) infected_markers: Vec<String>,
    pub(in crate::smtp) suspicious_markers: Vec<String>,
    pub(in crate::smtp) clean_markers: Vec<String>,
}

#[derive(Debug, Clone)]
pub(in crate::smtp) struct AntivirusVerdict {
    pub(in crate::smtp) action: FilterAction,
    pub(in crate::smtp) reason: Option<String>,
    pub(in crate::smtp) spam_score_delta: f32,
    pub(in crate::smtp) security_score_delta: f32,
    pub(in crate::smtp) decision_trace: Vec<DecisionTraceEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AntivirusProviderDecision {
    Clean,
    Suspicious,
    Infected,
}

#[derive(Debug, Clone)]
struct AntivirusScanTarget {
    root: PathBuf,
    attachment_count: usize,
}

#[derive(Debug, Clone)]
pub(in crate::smtp) struct AntivirusScanOutcome {
    pub(in crate::smtp) provider_id: String,
    pub(in crate::smtp) provider_name: String,
    pub(in crate::smtp) decision: AntivirusProviderDecision,
    pub(in crate::smtp) summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InboundMagikaOutcome {
    Accept,
    Quarantine(String),
    Reject(String),
}

pub(crate) fn load_antivirus_providers(provider_chain: &[String]) -> Vec<AntivirusProviderConfig> {
    provider_chain
        .iter()
        .filter_map(|provider_id| antivirus_provider_from_env(provider_id))
        .collect()
}

fn antivirus_provider_from_env(provider_id: &str) -> Option<AntivirusProviderConfig> {
    let normalized = provider_id.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }

    if normalized == "takeri" {
        return Some(AntivirusProviderConfig {
            id: normalized,
            display_name: "takeri".to_string(),
            command: env::var("LPE_CT_ANTIVIRUS_TAKERI_BIN")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "/opt/lpe-ct/bin/Shuhari-CyberForge-CLI".to_string()),
            args: env::var("LPE_CT_ANTIVIRUS_TAKERI_ARGS")
                .ok()
                .map(|value| parse_csv_env(&value))
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| vec!["takeri".to_string(), "scan".to_string()]),
            infected_markers: vec![
                "status: infected".to_string(),
                "infected files detected".to_string(),
                "infected files:".to_string(),
                "critical: malware detected".to_string(),
            ],
            suspicious_markers: vec![
                "status: suspicious".to_string(),
                "suspicious files:".to_string(),
            ],
            clean_markers: vec![
                "status: clean".to_string(),
                "no threats detected".to_string(),
            ],
        });
    }

    let env_key = normalized.replace('-', "_").to_ascii_uppercase();
    let command = env::var(format!("LPE_CT_ANTIVIRUS_{env_key}_BIN"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())?;
    let args = env::var(format!("LPE_CT_ANTIVIRUS_{env_key}_ARGS"))
        .ok()
        .map(|value| parse_csv_env(&value))
        .unwrap_or_default();
    let infected_markers = env::var(format!("LPE_CT_ANTIVIRUS_{env_key}_INFECTED_MARKERS"))
        .ok()
        .map(|value| parse_csv_env(&value))
        .unwrap_or_else(|| vec!["infected".to_string(), "malware".to_string()]);
    let suspicious_markers = env::var(format!("LPE_CT_ANTIVIRUS_{env_key}_SUSPICIOUS_MARKERS"))
        .ok()
        .map(|value| parse_csv_env(&value))
        .unwrap_or_else(|| vec!["suspicious".to_string()]);
    let clean_markers = env::var(format!("LPE_CT_ANTIVIRUS_{env_key}_CLEAN_MARKERS"))
        .ok()
        .map(|value| parse_csv_env(&value))
        .unwrap_or_else(|| vec!["clean".to_string(), "ok".to_string()]);

    Some(AntivirusProviderConfig {
        id: normalized.clone(),
        display_name: normalized,
        command,
        args,
        infected_markers,
        suspicious_markers,
        clean_markers,
    })
}

pub(crate) fn classify_inbound_message<D: Detector>(
    validator: &Validator<D>,
    message_bytes: &[u8],
) -> Result<InboundMagikaOutcome> {
    let attachments = collect_mime_attachment_parts(message_bytes)?;
    for attachment in attachments {
        let outcome = validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::LpeCtInboundSmtp,
                declared_mime: attachment.declared_mime.clone(),
                filename: attachment.filename.clone(),
                expected_kind: ExpectedKind::Any,
            },
            &attachment.bytes,
        )?;
        match outcome.policy_decision {
            PolicyDecision::Accept => {}
            PolicyDecision::Reject => {
                return Ok(InboundMagikaOutcome::Reject(format!(
                    "attachment {:?} rejected: {}",
                    attachment.filename, outcome.reason
                )));
            }
            PolicyDecision::Quarantine | PolicyDecision::Restrict => {
                return Ok(InboundMagikaOutcome::Quarantine(format!(
                    "attachment {:?} quarantined: {}",
                    attachment.filename, outcome.reason
                )));
            }
        }
    }
    Ok(InboundMagikaOutcome::Accept)
}

pub(in crate::smtp) async fn evaluate_antivirus_policy(
    config: &RuntimeConfig,
    direction: &str,
    message_bytes: &[u8],
) -> Result<AntivirusVerdict> {
    let mut decision_trace = Vec::new();
    if !config.antivirus_enabled {
        decision_trace.push(DecisionTraceEntry {
            stage: "virus-scan".to_string(),
            outcome: "disabled".to_string(),
            detail: "antivirus chain disabled by local policy".to_string(),
        });
        return Ok(AntivirusVerdict {
            action: FilterAction::Accept,
            reason: None,
            spam_score_delta: 0.0,
            security_score_delta: 0.0,
            decision_trace,
        });
    }

    if config.antivirus_provider_chain.is_empty() {
        let detail =
            "antivirus chain enabled but no providers are configured in LPE_CT_ANTIVIRUS_PROVIDER_CHAIN"
                .to_string();
        decision_trace.push(DecisionTraceEntry {
            stage: "virus-scan".to_string(),
            outcome: if config.antivirus_fail_closed {
                "quarantine"
            } else {
                "skipped"
            }
            .to_string(),
            detail: detail.clone(),
        });
        return Ok(AntivirusVerdict {
            action: if config.antivirus_fail_closed {
                FilterAction::Quarantine
            } else {
                FilterAction::Accept
            },
            reason: config.antivirus_fail_closed.then_some(detail),
            spam_score_delta: 0.0,
            security_score_delta: if config.antivirus_fail_closed {
                2.0
            } else {
                0.0
            },
            decision_trace,
        });
    }

    if config.antivirus_providers.is_empty() {
        let detail = format!(
            "antivirus chain references unsupported or incomplete providers: {}",
            config.antivirus_provider_chain.join(", ")
        );
        decision_trace.push(DecisionTraceEntry {
            stage: "virus-scan".to_string(),
            outcome: if config.antivirus_fail_closed {
                "quarantine"
            } else {
                "skipped"
            }
            .to_string(),
            detail: detail.clone(),
        });
        return Ok(AntivirusVerdict {
            action: if config.antivirus_fail_closed {
                FilterAction::Quarantine
            } else {
                FilterAction::Accept
            },
            reason: config.antivirus_fail_closed.then_some(detail),
            spam_score_delta: 0.0,
            security_score_delta: if config.antivirus_fail_closed {
                2.0
            } else {
                0.0
            },
            decision_trace,
        });
    }

    let target = prepare_antivirus_scan_target(direction, message_bytes)?;
    for provider in &config.antivirus_providers {
        match run_antivirus_provider(provider, &target).await {
            Ok(outcome) => {
                decision_trace.push(DecisionTraceEntry {
                    stage: "virus-scan".to_string(),
                    outcome: match outcome.decision {
                        AntivirusProviderDecision::Clean => "clean",
                        AntivirusProviderDecision::Suspicious => "suspicious",
                        AntivirusProviderDecision::Infected => "infected",
                    }
                    .to_string(),
                    detail: format!("{}: {}", outcome.provider_name, outcome.summary),
                });
                match outcome.decision {
                    AntivirusProviderDecision::Clean => {}
                    AntivirusProviderDecision::Suspicious => {
                        cleanup_antivirus_scan_target(&target);
                        return Ok(AntivirusVerdict {
                            action: FilterAction::Quarantine,
                            reason: Some(format!(
                                "antivirus provider {} flagged suspicious content",
                                outcome.provider_id
                            )),
                            spam_score_delta: 0.5,
                            security_score_delta: 4.0,
                            decision_trace,
                        });
                    }
                    AntivirusProviderDecision::Infected => {
                        cleanup_antivirus_scan_target(&target);
                        return Ok(AntivirusVerdict {
                            action: FilterAction::Quarantine,
                            reason: Some(format!(
                                "antivirus provider {} detected malware",
                                outcome.provider_id
                            )),
                            spam_score_delta: 1.0,
                            security_score_delta: 8.0,
                            decision_trace,
                        });
                    }
                }
            }
            Err(error) => {
                let detail = format!(
                    "{} execution failed for {} attachment artifact(s): {error}",
                    provider.display_name, target.attachment_count
                );
                decision_trace.push(DecisionTraceEntry {
                    stage: "virus-scan".to_string(),
                    outcome: if config.antivirus_fail_closed {
                        "quarantine"
                    } else {
                        "error"
                    }
                    .to_string(),
                    detail: detail.clone(),
                });
                if config.antivirus_fail_closed {
                    cleanup_antivirus_scan_target(&target);
                    return Ok(AntivirusVerdict {
                        action: FilterAction::Quarantine,
                        reason: Some(detail),
                        spam_score_delta: 0.0,
                        security_score_delta: 3.0,
                        decision_trace,
                    });
                }
            }
        }
    }

    cleanup_antivirus_scan_target(&target);
    Ok(AntivirusVerdict {
        action: FilterAction::Accept,
        reason: None,
        spam_score_delta: 0.0,
        security_score_delta: 0.0,
        decision_trace,
    })
}

fn prepare_antivirus_scan_target(
    direction: &str,
    message_bytes: &[u8],
) -> Result<AntivirusScanTarget> {
    let root = env::temp_dir().join(format!("lpe-ct-av-{}-{}", direction, uuid::Uuid::new_v4()));
    fs::create_dir_all(&root)
        .with_context(|| format!("unable to create antivirus scan target {}", root.display()))?;
    fs::write(root.join("message.eml"), message_bytes).with_context(|| {
        format!(
            "unable to write antivirus message artifact {}",
            root.display()
        )
    })?;

    let attachments = collect_mime_attachment_parts(message_bytes)?;
    for (index, attachment) in attachments.iter().enumerate() {
        let original_name = attachment.filename.as_deref().unwrap_or("attachment");
        let extension = attachment
            .filename
            .as_deref()
            .and_then(|filename| Path::new(filename).extension())
            .and_then(|value| value.to_str())
            .map(|value| format!(".{}", sanitize_attachment_component(value)))
            .unwrap_or_default();
        let file_name = Path::new(original_name)
            .file_stem()
            .and_then(|value| value.to_str())
            .map(sanitize_attachment_component)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| format!("attachment-{}", index + 1));
        fs::write(
            root.join(format!("{:02}-{}{}", index + 1, file_name, extension)),
            &attachment.bytes,
        )
        .with_context(|| {
            format!(
                "unable to write antivirus attachment artifact {}",
                root.display()
            )
        })?;
    }

    Ok(AntivirusScanTarget {
        root,
        attachment_count: attachments.len(),
    })
}

fn sanitize_attachment_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn cleanup_antivirus_scan_target(target: &AntivirusScanTarget) {
    let _ = fs::remove_dir_all(&target.root);
}

async fn run_antivirus_provider(
    provider: &AntivirusProviderConfig,
    target: &AntivirusScanTarget,
) -> Result<AntivirusScanOutcome> {
    let mut command = Command::new(&provider.command);
    let target_path = target.root.to_string_lossy().to_string();
    let mut path_explicitly_bound = false;
    for arg in &provider.args {
        if arg.contains("{path}") {
            path_explicitly_bound = true;
        }
        command.arg(arg.replace("{path}", &target_path));
    }
    if !path_explicitly_bound {
        command.arg(&target.root);
    }
    let output = command.output().await.with_context(|| {
        format!(
            "unable to execute antivirus provider {}",
            provider.display_name
        )
    })?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    parse_antivirus_output(provider, &stdout, &stderr, output.status.code())
}

pub(crate) fn parse_antivirus_output(
    provider: &AntivirusProviderConfig,
    stdout: &str,
    stderr: &str,
    exit_code: Option<i32>,
) -> Result<AntivirusScanOutcome> {
    let combined = format!("{stdout}\n{stderr}");
    let normalized = combined.to_ascii_lowercase();
    let infected = marker_matches(&normalized, &provider.infected_markers)
        || takeri_summary_count(&normalized, "infected files:") > 0;
    let suspicious = marker_matches(&normalized, &provider.suspicious_markers)
        || takeri_summary_count(&normalized, "suspicious files:") > 0;
    let clean = marker_matches(&normalized, &provider.clean_markers);

    let decision = if infected {
        AntivirusProviderDecision::Infected
    } else if suspicious {
        AntivirusProviderDecision::Suspicious
    } else if clean || exit_code == Some(0) {
        AntivirusProviderDecision::Clean
    } else {
        anyhow::bail!(
            "provider {} returned exit code {:?} without a parsable verdict",
            provider.display_name,
            exit_code
        );
    };

    let summary = combined
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("provider produced no output")
        .to_string();

    Ok(AntivirusScanOutcome {
        provider_id: provider.id.clone(),
        provider_name: provider.display_name.clone(),
        decision,
        summary,
    })
}

fn marker_matches(output: &str, markers: &[String]) -> bool {
    markers
        .iter()
        .map(|marker| marker.trim().to_ascii_lowercase())
        .filter(|marker| !marker.is_empty())
        .any(|marker| marker_has_positive_match(output, &marker))
}

fn marker_has_positive_match(output: &str, marker: &str) -> bool {
    let mut search_from = 0;
    while let Some(relative_index) = output[search_from..].find(marker) {
        let marker_start = search_from + relative_index;
        let marker_end = marker_start + marker.len();
        if !marker_match_is_explicitly_negative(output, marker_start, marker_end) {
            return true;
        }
        search_from = marker_end;
    }
    false
}

fn marker_match_is_explicitly_negative(
    output: &str,
    marker_start: usize,
    marker_end: usize,
) -> bool {
    let line_start = output[..marker_start]
        .rfind('\n')
        .map_or(0, |index| index + 1);
    let line_end = output[marker_end..]
        .find('\n')
        .map_or(output.len(), |index| marker_end + index);
    let before_marker = output[line_start..marker_start]
        .trim_end_matches(|ch: char| ch.is_whitespace() || matches!(ch, ':' | '=' | '-' | '>'));
    if before_marker.ends_with("no") || before_marker.ends_with("not") {
        return true;
    }

    let after_marker = output[marker_end..line_end].trim_start_matches(|ch: char| {
        ch.is_whitespace() || matches!(ch, ':' | '=' | '-' | '>' | '"' | '\'')
    });
    !after_marker.is_empty()
        && (after_marker.starts_with('0')
            || after_marker.starts_with("false")
            || after_marker.starts_with("no")
            || after_marker.starts_with("none")
            || after_marker.starts_with("not "))
}

fn takeri_summary_count(output: &str, prefix: &str) -> usize {
    output
        .lines()
        .find_map(|line| {
            let trimmed = line.trim();
            let normalized = trimmed.to_ascii_lowercase();
            normalized
                .strip_prefix(prefix)
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0)
}
