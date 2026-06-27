use super::*;

#[derive(Default)]
pub(in crate::smtp) struct SmtpTransaction {
    pub(in crate::smtp) helo: String,
    pub(in crate::smtp) mail_from: String,
    pub(in crate::smtp) mail_from_seen: bool,
    pub(in crate::smtp) rcpt_to: Vec<String>,
    pub(in crate::smtp) greeting_required: bool,
}

impl SmtpTransaction {
    fn after_starttls() -> Self {
        Self {
            greeting_required: true,
            ..Self::default()
        }
    }

    fn reset_message(&mut self) {
        self.mail_from.clear();
        self.mail_from_seen = false;
        self.rcpt_to.clear();
    }

    fn requires_greeting(&self) -> bool {
        self.greeting_required && self.helo.is_empty()
    }
}

pub(in crate::smtp) enum SmtpCommandOutcome {
    Continue,
    StartTls,
    Quit,
}

pub(in crate::smtp) async fn handle_smtp_session(
    stream: TcpStream,
    peer: SocketAddr,
    dashboard_store: Arc<Mutex<crate::DashboardState>>,
    spool_dir: PathBuf,
    starttls: Option<TlsAcceptor>,
) -> Result<()> {
    let client = reqwest::Client::new();
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    if let Some(role) = crate::ha_non_active_role_for_traffic()? {
        write_smtp(
            &mut writer,
            &format!("421 node role {role} is not accepting SMTP traffic"),
        )
        .await?;
        observability::record_smtp_session("ha-blocked");
        return Ok(());
    }

    let smtp_name = runtime_config_from_store(&dashboard_store)?.outbound_ehlo_name;
    write_smtp(&mut writer, &format!("220 {smtp_name} ESMTP ready")).await?;
    let mut transaction = SmtpTransaction::default();
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(());
        }

        let command = line.trim_end_matches(['\r', '\n']).to_string();
        match handle_smtp_command(
            &client,
            &mut reader,
            &mut writer,
            &dashboard_store,
            &spool_dir,
            peer,
            &mut transaction,
            &command,
            starttls.is_some(),
        )
        .await?
        {
            SmtpCommandOutcome::Continue => {}
            SmtpCommandOutcome::Quit => return Ok(()),
            SmtpCommandOutcome::StartTls => {
                let Some(starttls) = starttls.clone() else {
                    write_smtp(&mut writer, "454 TLS not available").await?;
                    continue;
                };
                write_smtp(&mut writer, "220 ready to start TLS").await?;
                let buffered = reader.buffer().to_vec();
                let stream = reader
                    .into_inner()
                    .reunite(writer)
                    .map_err(|_| anyhow!("unable to prepare SMTP stream for STARTTLS"))?;
                let stream = StartTlsStream::new(stream, buffered);
                let tls_stream = match starttls.accept(stream).await {
                    Ok(tls_stream) => tls_stream,
                    Err(error) => {
                        warn!(peer = %peer, error = %error, "smtp STARTTLS handshake failed");
                        return Err(error.into());
                    }
                };
                let (reader, mut writer) = tokio::io::split(tls_stream);
                let mut reader = BufReader::new(reader);
                run_smtp_command_loop(
                    &client,
                    &mut reader,
                    &mut writer,
                    &dashboard_store,
                    &spool_dir,
                    peer,
                    SmtpTransaction::after_starttls(),
                )
                .await?;
                return Ok(());
            }
        }
    }
}

pub(in crate::smtp) async fn run_smtp_command_loop<R, W>(
    client: &reqwest::Client,
    reader: &mut BufReader<R>,
    writer: &mut W,
    dashboard_store: &Arc<Mutex<crate::DashboardState>>,
    spool_dir: &Path,
    peer: SocketAddr,
    mut transaction: SmtpTransaction,
) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(());
        }

        let command = line.trim_end_matches(['\r', '\n']).to_string();
        match handle_smtp_command(
            client,
            reader,
            writer,
            dashboard_store,
            spool_dir,
            peer,
            &mut transaction,
            &command,
            false,
        )
        .await?
        {
            SmtpCommandOutcome::Continue => {}
            SmtpCommandOutcome::StartTls => {
                write_smtp(writer, "454 TLS already active").await?;
            }
            SmtpCommandOutcome::Quit => return Ok(()),
        }
    }
}

pub(in crate::smtp) async fn handle_smtp_command<R, W>(
    client: &reqwest::Client,
    reader: &mut BufReader<R>,
    writer: &mut W,
    dashboard_store: &Arc<Mutex<crate::DashboardState>>,
    spool_dir: &Path,
    peer: SocketAddr,
    transaction: &mut SmtpTransaction,
    command: &str,
    starttls_available: bool,
) -> Result<SmtpCommandOutcome>
where
    R: tokio::io::AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    if command.as_bytes().len() > MAX_SMTP_COMMAND_LINE_LEN {
        write_smtp(writer, "500 command line too long").await?;
        return Ok(SmtpCommandOutcome::Continue);
    }

    let upper = command.to_ascii_uppercase();
    if upper.starts_with("EHLO ") || upper.starts_with("HELO ") {
        let config = runtime_config_from_store(dashboard_store)?;
        transaction.helo = command[5.min(command.len())..].trim().to_string();
        transaction.greeting_required = false;
        transaction.reset_message();
        write_smtp(writer, &format!("250-{}", config.outbound_ehlo_name)).await?;
        if starttls_available {
            write_smtp(writer, "250-STARTTLS").await?;
        }
        write_smtp(
            writer,
            &format!(
                "250 SIZE {}",
                max_smtp_message_size_bytes(config.max_message_size_mb)
            ),
        )
        .await?;
    } else if upper == "STARTTLS" {
        if starttls_available {
            if transaction.helo.is_empty() {
                write_smtp(writer, "503 send EHLO or HELO first").await?;
                return Ok(SmtpCommandOutcome::Continue);
            }
            return Ok(SmtpCommandOutcome::StartTls);
        }
        write_smtp(writer, "454 TLS not available").await?;
    } else if upper == "AUTH" || upper.starts_with("AUTH ") {
        write_smtp(writer, "502 AUTH not available on public SMTP ingress").await?;
    } else if upper.starts_with("MAIL FROM:") {
        if transaction.requires_greeting() {
            write_smtp(writer, "503 send EHLO or HELO after STARTTLS first").await?;
            return Ok(SmtpCommandOutcome::Continue);
        }
        let config = runtime_config_from_store(dashboard_store)?;
        let candidate = match parse_smtp_path(
            command[10..].trim(),
            SmtpPathKind::MailFrom,
            max_smtp_message_size_bytes(config.max_message_size_mb),
        ) {
            Ok(parsed) => parsed.address,
            Err(error) => {
                write_smtp(writer, &smtp_path_error_reply("MAIL FROM", error)).await?;
                return Ok(SmtpCommandOutcome::Continue);
            }
        };
        if !candidate.is_empty() {
            if let transport_policy::AddressPolicyVerdict::Reject(reason) =
                transport_policy::evaluate_address_policy_with_config(
                    &config.address_policy,
                    transport_policy::AddressRole::Sender,
                    &candidate,
                )
            {
                write_smtp(writer, &format!("550 sender rejected ({reason})")).await?;
                return Ok(SmtpCommandOutcome::Continue);
            }
        }
        transaction.mail_from = candidate;
        transaction.mail_from_seen = true;
        transaction.rcpt_to.clear();
        write_smtp(writer, "250 sender accepted").await?;
    } else if upper.starts_with("RCPT TO:") {
        if transaction.requires_greeting() {
            write_smtp(writer, "503 send EHLO or HELO after STARTTLS first").await?;
            return Ok(SmtpCommandOutcome::Continue);
        }
        if !transaction.mail_from_seen {
            write_smtp(writer, "503 send MAIL FROM first").await?;
            return Ok(SmtpCommandOutcome::Continue);
        }
        let config = runtime_config_from_store(dashboard_store)?;
        let recipient = match parse_smtp_path(
            command[8..].trim(),
            SmtpPathKind::RcptTo,
            max_smtp_message_size_bytes(config.max_message_size_mb),
        ) {
            Ok(parsed) => parsed.address,
            Err(error) => {
                write_smtp(writer, &smtp_path_error_reply("RCPT TO", error)).await?;
                return Ok(SmtpCommandOutcome::Continue);
            }
        };
        if !recipient_domain_is_accepted(&config, &recipient) {
            write_smtp(
                writer,
                "550 recipient domain is not accepted by this sorting center",
            )
            .await?;
            return Ok(SmtpCommandOutcome::Continue);
        }
        if transaction.mail_from.is_empty()
            && !recipient_domain_accepts_null_reverse_path(&config, &recipient)
        {
            write_smtp(
                writer,
                "550 recipient domain does not accept null reverse-path",
            )
            .await?;
            return Ok(SmtpCommandOutcome::Continue);
        }
        if let transport_policy::AddressPolicyVerdict::Reject(reason) =
            transport_policy::evaluate_address_policy_with_config(
                &config.address_policy,
                transport_policy::AddressRole::Recipient,
                &recipient,
            )
        {
            write_smtp(writer, &format!("550 recipient rejected ({reason})")).await?;
            return Ok(SmtpCommandOutcome::Continue);
        }
        if transaction.rcpt_to.len() >= MAX_SMTP_RCPT_PER_TRANSACTION {
            write_smtp(writer, "452 too many recipients").await?;
            return Ok(SmtpCommandOutcome::Continue);
        }
        match transport_policy::verify_recipient_with_core(
            client,
            &config.recipient_verification,
            &config.core_delivery_base_url,
            Some(&transaction.mail_from),
            &recipient,
            Some(&transaction.helo),
            Some(&peer.to_string()),
            None,
        )
        .await
        {
            transport_policy::RecipientVerificationVerdict::Accept => {
                transaction.rcpt_to.push(recipient);
            }
            transport_policy::RecipientVerificationVerdict::Reject(reason) => {
                write_smtp(writer, &format!("550 recipient rejected ({reason})")).await?;
                return Ok(SmtpCommandOutcome::Continue);
            }
            transport_policy::RecipientVerificationVerdict::Defer(reason) => {
                write_smtp(
                    writer,
                    &format!("451 recipient verification unavailable ({reason})"),
                )
                .await?;
                return Ok(SmtpCommandOutcome::Continue);
            }
        }
        write_smtp(writer, "250 recipient accepted").await?;
    } else if upper == "DATA" {
        if transaction.requires_greeting() {
            write_smtp(writer, "503 send EHLO or HELO after STARTTLS first").await?;
            return Ok(SmtpCommandOutcome::Continue);
        }
        if !transaction.mail_from_seen || transaction.rcpt_to.is_empty() {
            write_smtp(writer, "503 sender and recipient required").await?;
            return Ok(SmtpCommandOutcome::Continue);
        }
        let config = runtime_config_from_store(dashboard_store)?;
        write_smtp(writer, "354 end with <CRLF>.<CRLF>").await?;
        let data = read_smtp_data(reader, config.max_message_size_mb).await?;
        let message = receive_message(
            spool_dir,
            &config,
            peer.to_string(),
            transaction.helo.clone(),
            transaction.mail_from.clone(),
            transaction.rcpt_to.clone(),
            data,
        )
        .await?;
        if message.status == "rejected" {
            write_smtp(writer, &rejected_smtp_reply(&message)).await?;
            return Ok(SmtpCommandOutcome::Quit);
        } else if message.status == "deferred" {
            write_smtp(writer, &deferred_smtp_reply(&message)).await?;
        } else if message.status == "quarantined" {
            write_smtp(writer, &format!("250 quarantined as {}", message.id)).await?;
            return Ok(SmtpCommandOutcome::Quit);
        } else {
            write_smtp(writer, &format!("250 queued as {}", message.id)).await?;
        }
        transaction.reset_message();
    } else if upper == "RSET" {
        transaction.reset_message();
        write_smtp(writer, "250 reset").await?;
    } else if upper == "NOOP" {
        write_smtp(writer, "250 ok").await?;
    } else if upper == "QUIT" {
        write_smtp(writer, "221 bye").await?;
        return Ok(SmtpCommandOutcome::Quit);
    } else {
        write_smtp(writer, "502 command not implemented").await?;
    }

    Ok(SmtpCommandOutcome::Continue)
}

pub(in crate::smtp) async fn receive_message(
    spool_dir: &Path,
    config: &RuntimeConfig,
    peer: String,
    helo: String,
    mail_from: String,
    rcpt_to: Vec<String>,
    data: Vec<u8>,
) -> Result<QueuedMessage> {
    receive_message_with_validator(
        &Validator::from_env(),
        spool_dir,
        config,
        peer,
        helo,
        mail_from,
        rcpt_to,
        data,
    )
    .await
}

pub(in crate::smtp) async fn receive_message_with_validator<D: Detector>(
    validator: &Validator<D>,
    spool_dir: &Path,
    config: &RuntimeConfig,
    peer: String,
    helo: String,
    mail_from: String,
    rcpt_to: Vec<String>,
    data: Vec<u8>,
) -> Result<QueuedMessage> {
    let mut message = QueuedMessage {
        id: message_id("in"),
        direction: "inbound".to_string(),
        received_at: current_timestamp(),
        peer,
        helo,
        mail_from,
        rcpt_to,
        status: "incoming".to_string(),
        relay_error: None,
        magika_summary: None,
        magika_decision: None,
        spam_score: 0.0,
        security_score: 0.0,
        reputation_score: 0,
        dnsbl_hits: Vec::new(),
        auth_summary: AuthSummary::default(),
        decision_trace: vec![DecisionTraceEntry {
            stage: "ingress".to_string(),
            outcome: "accepted".to_string(),
            detail: "message accepted by SMTP edge and persisted to the incoming spool".to_string(),
        }],
        remote_message_ref: None,
        technical_status: None,
        dsn: None,
        route: None,
        throttle: None,
        data,
    };

    persist_message(spool_dir, "incoming", &message).await?;
    message.decision_trace.push(DecisionTraceEntry {
        stage: "protocol".to_string(),
        outcome: "smtp-envelope".to_string(),
        detail: format!(
            "peer={} helo={} mail_from={} rcpt_count={}",
            message.peer,
            message.helo,
            message.mail_from,
            message.rcpt_to.len()
        ),
    });

    if config.drain_mode {
        message.status = "held".to_string();
        message.decision_trace.push(DecisionTraceEntry {
            stage: "drain-mode".to_string(),
            outcome: "held".to_string(),
            detail: "drain mode is enabled on the sorting center".to_string(),
        });
        move_message(spool_dir, &message, "incoming", "held").await?;
        let _ = append_transport_audit(spool_dir, config, "held", &message).await;
        return Ok(message);
    }

    match classify_inbound_message(validator, &message.data) {
        Ok(InboundMagikaOutcome::Accept) => {}
        Ok(InboundMagikaOutcome::Quarantine(reason)) => {
            observability::record_security_event("magika_quarantine");
            message.status = "quarantined".to_string();
            message.magika_decision = Some("quarantine".to_string());
            message.magika_summary = Some(reason);
            message.security_score += 5.0;
            message.decision_trace.push(DecisionTraceEntry {
                stage: "magika".to_string(),
                outcome: "quarantine".to_string(),
                detail: message.magika_summary.clone().unwrap_or_default(),
            });
            move_message(spool_dir, &message, "incoming", "quarantine").await?;
            persist_quarantine_metadata_or_warn(spool_dir, config, &message).await;
            let _ = append_transport_audit(spool_dir, config, "quarantine", &message).await;
            info!(
                trace_id = %message.id,
                status = %message.status,
                "inbound message quarantined by Magika"
            );
            return Ok(message);
        }
        Ok(InboundMagikaOutcome::Reject(reason)) => {
            observability::record_security_event("magika_reject");
            message.status = "rejected".to_string();
            message.magika_decision = Some("reject".to_string());
            message.magika_summary = Some(reason);
            message.security_score += 8.0;
            message.decision_trace.push(DecisionTraceEntry {
                stage: "magika".to_string(),
                outcome: "reject".to_string(),
                detail: message.magika_summary.clone().unwrap_or_default(),
            });
            move_message(spool_dir, &message, "incoming", "quarantine").await?;
            persist_quarantine_metadata_or_warn(spool_dir, config, &message).await;
            let _ = append_transport_audit(spool_dir, config, "quarantine", &message).await;
            info!(
                trace_id = %message.id,
                status = %message.status,
                "inbound message rejected by Magika"
            );
            return Ok(message);
        }
        Err(error) => {
            observability::record_security_event("magika_quarantine");
            message.status = "quarantined".to_string();
            message.magika_decision = Some("quarantine".to_string());
            message.magika_summary = Some(format!("Magika validation failed: {error}"));
            message.security_score += 4.0;
            message.decision_trace.push(DecisionTraceEntry {
                stage: "magika".to_string(),
                outcome: "quarantine".to_string(),
                detail: message.magika_summary.clone().unwrap_or_default(),
            });
            move_message(spool_dir, &message, "incoming", "quarantine").await?;
            persist_quarantine_metadata_or_warn(spool_dir, config, &message).await;
            let _ = append_transport_audit(spool_dir, config, "quarantine", &message).await;
            info!(
                trace_id = %message.id,
                status = %message.status,
                "inbound message quarantined after Magika failure"
            );
            return Ok(message);
        }
    }
    match transport_policy::evaluate_attachment_policy_with_config(
        &config.attachment_policy,
        validator,
        IngressContext::LpeCtInboundSmtp,
        &message.data,
    )? {
        transport_policy::AttachmentPolicyVerdict::Accept => {}
        transport_policy::AttachmentPolicyVerdict::Restrict(reason) => {
            observability::record_security_event("attachment_policy_quarantine");
            message.status = "quarantined".to_string();
            message.magika_decision = Some("quarantine".to_string());
            message.magika_summary = Some(reason.clone());
            message.security_score += 4.0;
            message.decision_trace.push(DecisionTraceEntry {
                stage: "attachment-policy".to_string(),
                outcome: "quarantine".to_string(),
                detail: reason,
            });
            move_message(spool_dir, &message, "incoming", "quarantine").await?;
            persist_quarantine_metadata_or_warn(spool_dir, config, &message).await;
            let _ = append_transport_audit(spool_dir, config, "quarantine", &message).await;
            return Ok(message);
        }
    }

    let verdict = evaluate_inbound_policy(
        spool_dir,
        config,
        parse_peer_ip(&message.peer),
        &message.helo,
        &message.mail_from,
        &message.rcpt_to,
        &message.data,
    )
    .await?;
    apply_filter_verdict(&mut message, &verdict);

    match verdict.action {
        FilterAction::Accept => match deliver_inbound_message(config, &message).await {
            Ok(_) => {
                message.status = "sent".to_string();
                message.decision_trace.push(DecisionTraceEntry {
                    stage: "core-delivery".to_string(),
                    outcome: "sent".to_string(),
                    detail: "message delivered to the core LPE inbound-delivery API".to_string(),
                });
                move_message(spool_dir, &message, "incoming", "sent").await?;
                let _ = append_transport_audit(spool_dir, config, "sent", &message).await;
                update_reputation(spool_dir, config, &message, FilterAction::Accept).await?;
                train_bayespam(spool_dir, config, &message, BayesLabel::Ham).await?;
                observability::record_smtp_session("delivered");
            }
            Err(error) => {
                message.status = if config.fallback_to_hold_queue {
                    "held".to_string()
                } else {
                    "deferred".to_string()
                };
                message.relay_error = Some(error.to_string());
                message.decision_trace.push(DecisionTraceEntry {
                    stage: "core-delivery".to_string(),
                    outcome: message.status.clone(),
                    detail: error.to_string(),
                });
                let destination = if config.fallback_to_hold_queue {
                    "held"
                } else {
                    "deferred"
                };
                move_message(spool_dir, &message, "incoming", destination).await?;
                let _ = append_transport_audit(spool_dir, config, destination, &message).await;
                update_reputation(spool_dir, config, &message, FilterAction::Defer).await?;
                observability::record_security_event("inbound_delivery_deferred");
                observability::record_smtp_session("deferred");
                warn!(
                    trace_id = %message.id,
                    status = %message.status,
                    error = %error,
                    "inbound final delivery deferred"
                );
            }
        },
        FilterAction::Quarantine => {
            observability::record_security_event("inbound_quarantine");
            message.status = "quarantined".to_string();
            move_message(spool_dir, &message, "incoming", "quarantine").await?;
            persist_quarantine_metadata_or_warn(spool_dir, config, &message).await;
            let _ = append_transport_audit(spool_dir, config, "quarantine", &message).await;
            update_reputation(spool_dir, config, &message, FilterAction::Quarantine).await?;
            train_bayespam(spool_dir, config, &message, BayesLabel::Spam).await?;
            observability::record_smtp_session("quarantined");
        }
        FilterAction::Reject => {
            observability::record_security_event("inbound_reject");
            message.status = "rejected".to_string();
            move_message(spool_dir, &message, "incoming", "quarantine").await?;
            persist_quarantine_metadata_or_warn(spool_dir, config, &message).await;
            let _ = append_transport_audit(spool_dir, config, "quarantine", &message).await;
            update_reputation(spool_dir, config, &message, FilterAction::Reject).await?;
            train_bayespam(spool_dir, config, &message, BayesLabel::Spam).await?;
            observability::record_smtp_session("rejected");
            warn!(
                trace_id = %message.id,
                reason = message.relay_error.as_deref().unwrap_or("perimeter policy reject"),
                "inbound message rejected by perimeter policy"
            );
        }
        FilterAction::Defer => {
            observability::record_security_event("inbound_defer");
            message.status = "deferred".to_string();
            move_message(spool_dir, &message, "incoming", "deferred").await?;
            let _ = append_transport_audit(spool_dir, config, "deferred", &message).await;
            update_reputation(spool_dir, config, &message, FilterAction::Defer).await?;
            observability::record_smtp_session("deferred");
            warn!(
                trace_id = %message.id,
                reason = message.relay_error.as_deref().unwrap_or("perimeter policy defer"),
                "inbound message deferred by perimeter policy"
            );
        }
    }

    info!(
        trace_id = %message.id,
        status = %message.status,
        peer = %message.peer,
        sender = %message.mail_from,
        recipient_count = message.rcpt_to.len(),
        "smtp message processed"
    );
    Ok(message)
}
