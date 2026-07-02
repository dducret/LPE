use super::*;

pub(in crate::smtp) async fn relay_message(
    config: &RuntimeConfig,
    message: &QueuedMessage,
    route: &TransportRouteDecision,
    attempt_count: u32,
    _last_attempt_error: Option<&str>,
) -> OutboundExecution {
    if config.mutual_tls_required {
        return OutboundExecution {
            status: TransportDeliveryStatus::Failed,
            detail: Some(
                "mutual TLS relay is configured but not implemented in LPE-CT v1".to_string(),
            ),
            remote_message_ref: None,
            retry: None,
            dsn: None,
            technical: Some(TransportTechnicalStatus {
                phase: "connect".to_string(),
                smtp_code: None,
                enhanced_code: None,
                remote_host: route.relay_target.clone(),
                detail: Some(
                    "mutual TLS relay is configured but not implemented in LPE-CT v1".to_string(),
                ),
            }),
            route: Some(TransportRouteDecision {
                rule_id: route.rule_id.clone(),
                relay_target: route.relay_target.clone(),
                queue: "held".to_string(),
            }),
            throttle: None,
        };
    }

    let mut targets = Vec::new();
    if let Some(target) = route.relay_target.clone() {
        targets.push(target);
    }
    for candidate in [&config.primary_upstream, &config.secondary_upstream] {
        let candidate = candidate.trim();
        if !candidate.is_empty() && !targets.iter().any(|existing| existing == candidate) {
            targets.push(candidate.to_string());
        }
    }

    if targets.is_empty() {
        return relay_message_direct_mx(config, message, route, attempt_count).await;
    }

    let mut last_error = None;
    for target in targets {
        match relay_message_to_target(
            &target,
            message,
            route,
            attempt_count,
            &config.outbound_ehlo_name,
        )
        .await
        {
            Ok(execution) => return execution,
            Err(error) => last_error = Some((target, error)),
        }
    }

    let (target, error) =
        last_error.unwrap_or_else(|| ("".to_string(), anyhow!("no SMTP target attempted")));
    let detail = error.to_string();
    let status = if is_permanent_relay_error(&detail) {
        TransportDeliveryStatus::Failed
    } else {
        TransportDeliveryStatus::Deferred
    };
    let retry = if status == TransportDeliveryStatus::Deferred {
        let retry_after = retry_after_seconds(DEFAULT_OUTBOUND_RETRY_AFTER_SECONDS, attempt_count);
        Some(TransportRetryAdvice {
            retry_after_seconds: retry_after,
            policy: "connect-backoff".to_string(),
            reason: Some(detail.clone()),
        })
    } else {
        None
    };
    let dsn = if status == TransportDeliveryStatus::Deferred {
        Some(TransportDsnReport {
            action: "delayed".to_string(),
            status: "4.4.1".to_string(),
            diagnostic_code: Some(format!("smtp; {detail}")),
            remote_mta: if target.is_empty() {
                None
            } else {
                Some(target.clone())
            },
        })
    } else {
        None
    };
    OutboundExecution {
        status: status.clone(),
        detail: Some(detail.clone()),
        remote_message_ref: None,
        retry,
        dsn,
        technical: Some(TransportTechnicalStatus {
            phase: "connect".to_string(),
            smtp_code: None,
            enhanced_code: None,
            remote_host: if target.is_empty() {
                route.relay_target.clone()
            } else {
                Some(target.clone())
            },
            detail: Some(detail),
        }),
        route: Some(TransportRouteDecision {
            rule_id: route.rule_id.clone(),
            relay_target: if target.is_empty() {
                route.relay_target.clone()
            } else {
                Some(target)
            },
            queue: default_queue_for_status(&status).to_string(),
        }),
        throttle: None,
    }
}

async fn relay_message_direct_mx(
    config: &RuntimeConfig,
    message: &QueuedMessage,
    route: &TransportRouteDecision,
    attempt_count: u32,
) -> OutboundExecution {
    let resolver = match SystemDnsResolver::new() {
        Ok(resolver) => resolver,
        Err(error) => {
            return direct_mx_failure(
                route,
                attempt_count,
                format!("unable to initialize DNS resolver for direct MX delivery: {error}"),
                None,
                false,
            );
        }
    };

    let mut recipients_by_domain = BTreeMap::<String, Vec<String>>::new();
    for recipient in &message.rcpt_to {
        let Some(domain) = domain_part(recipient) else {
            return direct_mx_failure(
                route,
                attempt_count,
                format!("recipient address has no domain: {recipient}"),
                None,
                true,
            );
        };
        recipients_by_domain
            .entry(domain)
            .or_default()
            .push(recipient.clone());
    }

    let mut relayed = Vec::new();
    let mut last_execution = None;
    let local_domains = recipients_by_domain
        .keys()
        .filter(|domain| accepted_domain_is_verified(config, domain))
        .cloned()
        .collect::<Vec<_>>();
    for domain in local_domains {
        let Some(recipients) = recipients_by_domain.remove(&domain) else {
            continue;
        };
        let execution = deliver_outbound_to_local_recipients(
            config,
            message,
            route,
            attempt_count,
            &recipients,
        )
        .await;
        if execution.status != TransportDeliveryStatus::Relayed {
            return execution;
        }
        relayed.push(format!("{domain} via local-core"));
        last_execution = Some(execution);
    }

    for (domain, recipients) in recipients_by_domain {
        let targets = match direct_mx_targets(&resolver, &domain).await {
            Ok(targets) => targets,
            Err(error) => {
                let detail = error.to_string();
                return direct_mx_failure(
                    route,
                    attempt_count,
                    detail.clone(),
                    Some(domain),
                    is_permanent_direct_mx_error(&detail),
                );
            }
        };

        let mut last_error = None;
        for target in targets {
            match relay_message_to_target_for_recipients(
                &target,
                message,
                route,
                attempt_count,
                &recipients,
                &config.outbound_ehlo_name,
            )
            .await
            {
                Ok(execution) if execution.status == TransportDeliveryStatus::Relayed => {
                    relayed.push(format!("{domain} via {target}"));
                    last_execution = Some(execution);
                    last_error = None;
                    break;
                }
                Ok(execution) => return execution,
                Err(error) => last_error = Some((target, error)),
            }
        }

        if let Some((target, error)) = last_error {
            return direct_mx_failure(
                route,
                attempt_count,
                error.to_string(),
                Some(format!("{domain} via {target}")),
                false,
            );
        }
    }

    let Some(mut execution) = last_execution else {
        return direct_mx_failure(
            route,
            attempt_count,
            "no outbound recipients available for direct MX delivery".to_string(),
            None,
            true,
        );
    };

    if relayed.len() > 1 {
        let has_local = relayed
            .iter()
            .any(|entry| entry.ends_with(" via local-core"));
        let relay_target = if has_local {
            "mixed-local-direct-mx"
        } else {
            "direct-mx"
        };
        execution.detail = Some(format!(
            "outbound delivery completed for {} recipient domain groups",
            relayed.len()
        ));
        execution.remote_message_ref = Some(relayed.join("; "));
        execution.route = Some(TransportRouteDecision {
            rule_id: route.rule_id.clone(),
            relay_target: Some(relay_target.to_string()),
            queue: "sent".to_string(),
        });
    }
    execution
}

async fn deliver_outbound_to_local_recipients(
    config: &RuntimeConfig,
    message: &QueuedMessage,
    route: &TransportRouteDecision,
    attempt_count: u32,
    recipients: &[String],
) -> OutboundExecution {
    let mut local_message = message.clone();
    local_message.rcpt_to = recipients.to_vec();
    match deliver_inbound_message(config, &local_message).await {
        Ok(delivery) => OutboundExecution {
            status: TransportDeliveryStatus::Relayed,
            detail: Some(format!(
                "delivered to local accepted domain through core delivery bridge: {} mailbox(es)",
                delivery.delivered_mailboxes.len()
            )),
            remote_message_ref: Some(format!("local-core:{}", message.id)),
            retry: None,
            dsn: None,
            technical: Some(TransportTechnicalStatus {
                phase: "local-delivery".to_string(),
                smtp_code: None,
                enhanced_code: Some("2.0.0".to_string()),
                remote_host: Some(config.core_delivery_base_url.clone()),
                detail: Some("delivered through LPE core final-delivery API".to_string()),
            }),
            route: Some(TransportRouteDecision {
                rule_id: route.rule_id.clone(),
                relay_target: Some("local-core".to_string()),
                queue: "sent".to_string(),
            }),
            throttle: None,
        },
        Err(error) => {
            let detail = format!("local core delivery failed: {error}");
            OutboundExecution {
                status: TransportDeliveryStatus::Deferred,
                detail: Some(detail.clone()),
                remote_message_ref: None,
                retry: Some(TransportRetryAdvice {
                    retry_after_seconds: retry_after_seconds(
                        DEFAULT_OUTBOUND_RETRY_AFTER_SECONDS,
                        attempt_count,
                    ),
                    policy: "local-core-delivery".to_string(),
                    reason: Some(detail.clone()),
                }),
                dsn: Some(TransportDsnReport {
                    action: "delayed".to_string(),
                    status: "4.4.1".to_string(),
                    diagnostic_code: Some(format!("smtp; {detail}")),
                    remote_mta: Some("local-core".to_string()),
                }),
                technical: Some(TransportTechnicalStatus {
                    phase: "local-delivery".to_string(),
                    smtp_code: None,
                    enhanced_code: Some("4.4.1".to_string()),
                    remote_host: Some(config.core_delivery_base_url.clone()),
                    detail: Some(detail.clone()),
                }),
                route: Some(TransportRouteDecision {
                    rule_id: route.rule_id.clone(),
                    relay_target: Some("local-core".to_string()),
                    queue: "deferred".to_string(),
                }),
                throttle: None,
            }
        }
    }
}

async fn direct_mx_targets(resolver: &SystemDnsResolver, domain: &str) -> Result<Vec<String>> {
    match resolver.query_mx(domain).await {
        Ok(mut records) if !records.is_empty() => {
            records.sort_by_key(|record| record.preference);
            let mut targets = Vec::new();
            for record in records {
                let exchange = record.exchange.trim().trim_end_matches('.');
                if exchange.is_empty() || exchange == "." {
                    anyhow::bail!(
                        "recipient domain {domain} publishes a null MX and does not accept mail"
                    );
                }
                targets.push(format!("{exchange}:25"));
            }
            Ok(targets)
        }
        Ok(_) | Err(DnsError::NoRecords) => Ok(vec![format!("{domain}:25")]),
        Err(DnsError::NxDomain) => anyhow::bail!("recipient domain {domain} does not exist"),
        Err(DnsError::TempFail) => {
            anyhow::bail!("temporary DNS failure while resolving MX for {domain}")
        }
    }
}

pub(in crate::smtp) fn sanitize_outbound_ehlo_name(value: &str) -> String {
    let normalized = value.trim().trim_end_matches('.').to_ascii_lowercase();
    if is_valid_ehlo_hostname(&normalized) {
        normalized
    } else {
        "lpe-ct.local".to_string()
    }
}

fn is_valid_ehlo_hostname(value: &str) -> bool {
    if value.is_empty() || value.len() > 253 || !value.contains('.') {
        return false;
    }
    value.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            && !label.starts_with('-')
            && !label.ends_with('-')
    })
}

async fn relay_message_to_target(
    target: &str,
    message: &QueuedMessage,
    route: &TransportRouteDecision,
    attempt_count: u32,
    ehlo_name: &str,
) -> Result<OutboundExecution> {
    relay_message_to_target_for_recipients(
        target,
        message,
        route,
        attempt_count,
        &message.rcpt_to,
        ehlo_name,
    )
    .await
}

async fn relay_message_to_target_for_recipients(
    target: &str,
    message: &QueuedMessage,
    route: &TransportRouteDecision,
    attempt_count: u32,
    recipients: &[String],
    ehlo_name: &str,
) -> Result<OutboundExecution> {
    let address = normalize_smtp_target(target);
    let stream = TcpStream::connect(&address)
        .await
        .with_context(|| format!("unable to connect to relay target {address}"))?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    expect_smtp(&mut reader, 220).await?;
    smtp_command(
        &mut reader,
        &mut writer,
        &format!("EHLO {}", sanitize_outbound_ehlo_name(ehlo_name)),
        250,
    )
    .await?;
    smtp_command(
        &mut reader,
        &mut writer,
        &format!("MAIL FROM:<{}>", message.mail_from),
        250,
    )
    .await?;
    for recipient in recipients {
        let reply = smtp_command_reply(
            &mut reader,
            &mut writer,
            &format!("RCPT TO:<{}>", recipient),
        )
        .await?;
        if !(reply.code == 250 || reply.code == 251) {
            let status = if reply.code >= 500 {
                TransportDeliveryStatus::Bounced
            } else {
                TransportDeliveryStatus::Deferred
            };
            let enhanced = parse_enhanced_status(&reply.message);
            return Ok(OutboundExecution {
                status: status.clone(),
                detail: Some(reply.message.clone()),
                remote_message_ref: None,
                retry: if status == TransportDeliveryStatus::Deferred {
                    let retry_after =
                        retry_after_seconds(DEFAULT_OUTBOUND_RETRY_AFTER_SECONDS, attempt_count);
                    Some(TransportRetryAdvice {
                        retry_after_seconds: retry_after,
                        policy: "remote-smtp".to_string(),
                        reason: Some(reply.message.clone()),
                    })
                } else {
                    None
                },
                dsn: Some(TransportDsnReport {
                    action: if status == TransportDeliveryStatus::Bounced {
                        "failed".to_string()
                    } else {
                        "delayed".to_string()
                    },
                    status: enhanced.clone().unwrap_or_else(|| {
                        if status == TransportDeliveryStatus::Bounced {
                            "5.1.1".to_string()
                        } else {
                            "4.4.1".to_string()
                        }
                    }),
                    diagnostic_code: Some(format!("smtp; {}", reply.message)),
                    remote_mta: Some(address.clone()),
                }),
                technical: Some(TransportTechnicalStatus {
                    phase: "rcpt-to".to_string(),
                    smtp_code: Some(reply.code),
                    enhanced_code: enhanced,
                    remote_host: Some(address.clone()),
                    detail: Some(reply.message.clone()),
                }),
                route: Some(TransportRouteDecision {
                    rule_id: route.rule_id.clone(),
                    relay_target: Some(target.to_string()),
                    queue: default_queue_for_status(&status).to_string(),
                }),
                throttle: None,
            });
        }
    }
    let data_reply = smtp_command_reply(&mut reader, &mut writer, "DATA").await?;
    if data_reply.code != 354 {
        let enhanced = parse_enhanced_status(&data_reply.message);
        return Ok(OutboundExecution {
            status: TransportDeliveryStatus::Deferred,
            detail: Some(data_reply.message.clone()),
            remote_message_ref: None,
            retry: {
                let retry_after =
                    retry_after_seconds(DEFAULT_OUTBOUND_RETRY_AFTER_SECONDS, attempt_count);
                Some(TransportRetryAdvice {
                    retry_after_seconds: retry_after,
                    policy: "remote-smtp".to_string(),
                    reason: Some(data_reply.message.clone()),
                })
            },
            dsn: Some(TransportDsnReport {
                action: "delayed".to_string(),
                status: enhanced.clone().unwrap_or_else(|| "4.3.0".to_string()),
                diagnostic_code: Some(format!("smtp; {}", data_reply.message)),
                remote_mta: Some(address.clone()),
            }),
            technical: Some(TransportTechnicalStatus {
                phase: "data".to_string(),
                smtp_code: Some(data_reply.code),
                enhanced_code: enhanced,
                remote_host: Some(address.clone()),
                detail: Some(data_reply.message.clone()),
            }),
            route: Some(TransportRouteDecision {
                rule_id: route.rule_id.clone(),
                relay_target: Some(target.to_string()),
                queue: "deferred".to_string(),
            }),
            throttle: None,
        });
    }
    writer.write_all(&message.data).await?;
    if !message.data.ends_with(b"\r\n") {
        writer.write_all(b"\r\n").await?;
    }
    writer.write_all(b".\r\n").await?;
    let final_reply = read_smtp_reply(&mut reader).await?;
    writer.write_all(b"QUIT\r\n").await?;
    if final_reply.code != 250 {
        let status = if final_reply.code >= 500 {
            TransportDeliveryStatus::Bounced
        } else {
            TransportDeliveryStatus::Deferred
        };
        let enhanced = parse_enhanced_status(&final_reply.message);
        return Ok(OutboundExecution {
            status: status.clone(),
            detail: Some(final_reply.message.clone()),
            remote_message_ref: None,
            retry: if status == TransportDeliveryStatus::Deferred {
                let retry_after =
                    retry_after_seconds(DEFAULT_OUTBOUND_RETRY_AFTER_SECONDS, attempt_count);
                Some(TransportRetryAdvice {
                    retry_after_seconds: retry_after,
                    policy: "remote-smtp".to_string(),
                    reason: Some(final_reply.message.clone()),
                })
            } else {
                None
            },
            dsn: Some(TransportDsnReport {
                action: if status == TransportDeliveryStatus::Bounced {
                    "failed".to_string()
                } else {
                    "delayed".to_string()
                },
                status: enhanced.clone().unwrap_or_else(|| {
                    if status == TransportDeliveryStatus::Bounced {
                        "5.0.0".to_string()
                    } else {
                        "4.0.0".to_string()
                    }
                }),
                diagnostic_code: Some(format!("smtp; {}", final_reply.message)),
                remote_mta: Some(address.clone()),
            }),
            technical: Some(TransportTechnicalStatus {
                phase: "final-response".to_string(),
                smtp_code: Some(final_reply.code),
                enhanced_code: enhanced,
                remote_host: Some(address.clone()),
                detail: Some(final_reply.message.clone()),
            }),
            route: Some(TransportRouteDecision {
                rule_id: route.rule_id.clone(),
                relay_target: Some(target.to_string()),
                queue: default_queue_for_status(&status).to_string(),
            }),
            throttle: None,
        });
    }

    Ok(OutboundExecution {
        status: TransportDeliveryStatus::Relayed,
        detail: None,
        remote_message_ref: Some(final_reply.message.clone()),
        retry: None,
        dsn: None,
        technical: Some(TransportTechnicalStatus {
            phase: "final-response".to_string(),
            smtp_code: Some(final_reply.code),
            enhanced_code: parse_enhanced_status(&final_reply.message),
            remote_host: Some(address.clone()),
            detail: Some(final_reply.message.clone()),
        }),
        route: Some(TransportRouteDecision {
            rule_id: route.rule_id.clone(),
            relay_target: Some(target.to_string()),
            queue: "sent".to_string(),
        }),
        throttle: None,
    })
}
