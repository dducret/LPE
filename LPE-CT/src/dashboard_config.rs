use super::*;

pub(crate) fn apply_env_overrides(state: &mut DashboardState) {
    if let Ok(value) = env::var("LPE_CT_NODE_NAME") {
        state.site.node_name = value;
    }
    if let Ok(value) = env::var("LPE_CT_CORE_DELIVERY_BASE_URL") {
        state.relay.core_delivery_base_url = value;
    }
    let public_tls_cert_path = env::var("LPE_CT_PUBLIC_TLS_CERT_PATH")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let public_tls_key_path = env::var("LPE_CT_PUBLIC_TLS_KEY_PATH")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if let (Some(cert_path), Some(key_path)) = (public_tls_cert_path, public_tls_key_path) {
        upsert_env_public_tls_profile(&mut state.network.public_tls, cert_path, key_path);
    }
    if let Ok(value) = env::var("LPE_CT_MUTUAL_TLS_REQUIRED") {
        state.relay.mutual_tls_required = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_FALLBACK_TO_HOLD_QUEUE") {
        state.relay.fallback_to_hold_queue = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_DRAIN_MODE") {
        state.policies.drain_mode = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_GREYLISTING_ENABLED") {
        state.policies.greylisting_enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_ANTIVIRUS_ENABLED") {
        state.policies.antivirus_enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_ANTIVIRUS_FAIL_CLOSED") {
        state.policies.antivirus_fail_closed = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_ANTIVIRUS_PROVIDER_CHAIN") {
        state.policies.antivirus_provider_chain = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_BAYESPAM_ENABLED") {
        state.policies.bayespam_enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_BAYESPAM_AUTO_LEARN") {
        state.policies.bayespam_auto_learn = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_BAYESPAM_SCORE_WEIGHT") {
        if let Ok(parsed) = value.parse::<f32>() {
            state.policies.bayespam_score_weight = parsed.max(0.0);
        }
    }
    if let Ok(value) = env::var("LPE_CT_BAYESPAM_MIN_TOKEN_LENGTH") {
        if let Ok(parsed) = value.parse::<u32>() {
            state.policies.bayespam_min_token_length = parsed.max(2);
        }
    }
    if let Ok(value) = env::var("LPE_CT_BAYESPAM_MAX_TOKENS") {
        if let Ok(parsed) = value.parse::<u32>() {
            state.policies.bayespam_max_tokens = parsed.max(16);
        }
    }
    if let Ok(value) = env::var("LPE_CT_REQUIRE_SPF") {
        state.policies.require_spf = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_REQUIRE_DKIM_ALIGNMENT") {
        state.policies.require_dkim_alignment = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_REQUIRE_DMARC_ENFORCEMENT") {
        state.policies.require_dmarc_enforcement = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_DEFER_ON_AUTH_TEMPFAIL") {
        state.policies.defer_on_auth_tempfail = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_DNSBL_ENABLED") {
        state.policies.dnsbl_enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_DNSBL_ZONES") {
        state.policies.dnsbl_zones = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_REPUTATION_ENABLED") {
        state.policies.reputation_enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_REPUTATION_QUARANTINE_THRESHOLD") {
        if let Ok(parsed) = value.parse::<i32>() {
            state.policies.reputation_quarantine_threshold = parsed;
        }
    }
    if let Ok(value) = env::var("LPE_CT_REPUTATION_REJECT_THRESHOLD") {
        if let Ok(parsed) = value.parse::<i32>() {
            state.policies.reputation_reject_threshold = parsed;
        }
    }
    if let Ok(value) = env::var("LPE_CT_SPAM_QUARANTINE_THRESHOLD") {
        if let Ok(parsed) = value.parse::<f32>() {
            state.policies.spam_quarantine_threshold = parsed.max(0.0);
        }
    }
    if let Ok(value) = env::var("LPE_CT_SPAM_REJECT_THRESHOLD") {
        if let Ok(parsed) = value.parse::<f32>() {
            state.policies.spam_reject_threshold =
                parsed.max(state.policies.spam_quarantine_threshold);
        }
    }
    if let Ok(value) = env::var("LPE_CT_MAX_MESSAGE_SIZE_MB") {
        if let Ok(parsed) = value.parse::<u32>() {
            state.policies.max_message_size_mb = parsed.max(1);
        }
    }
    if let Ok(value) = env::var("LPE_CT_POLICY_ALLOW_SENDERS") {
        state.policies.address_policy.allow_senders = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_POLICY_BLOCK_SENDERS") {
        state.policies.address_policy.block_senders = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_POLICY_ALLOW_RECIPIENTS") {
        state.policies.address_policy.allow_recipients = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_POLICY_BLOCK_RECIPIENTS") {
        state.policies.address_policy.block_recipients = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_RECIPIENT_VERIFICATION_ENABLED") {
        state.policies.recipient_verification.enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_RECIPIENT_VERIFICATION_FAIL_CLOSED") {
        state.policies.recipient_verification.fail_closed = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_RECIPIENT_VERIFICATION_CACHE_TTL_SECONDS") {
        if let Ok(parsed) = value.parse::<u32>() {
            state.policies.recipient_verification.cache_ttl_seconds = parsed.max(1);
        }
    }
    if let Ok(value) = env::var("LPE_CT_ATTACHMENT_ALLOW_EXTENSIONS") {
        state.policies.attachment_policy.allow_extensions = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_ATTACHMENT_BLOCK_EXTENSIONS") {
        state.policies.attachment_policy.block_extensions = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_ATTACHMENT_ALLOW_MIME_TYPES") {
        state.policies.attachment_policy.allow_mime_types = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_ATTACHMENT_BLOCK_MIME_TYPES") {
        state.policies.attachment_policy.block_mime_types = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_ATTACHMENT_ALLOW_DETECTED_TYPES") {
        state.policies.attachment_policy.allow_detected_types = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_ATTACHMENT_BLOCK_DETECTED_TYPES") {
        state.policies.attachment_policy.block_detected_types = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_OUTBOUND_DKIM_ENABLED") {
        state.policies.dkim.enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_OUTBOUND_DKIM_HEADERS") {
        state.policies.dkim.headers = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_OUTBOUND_DKIM_OVER_SIGN") {
        state.policies.dkim.over_sign = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_OUTBOUND_DKIM_EXPIRATION_SECONDS") {
        state.policies.dkim.expiration_seconds =
            value.parse::<u32>().ok().filter(|value| *value > 0);
    }
    if let Ok(value) = env::var("LPE_CT_OUTBOUND_DKIM_KEYS") {
        let domains = value
            .split(';')
            .filter_map(|entry| {
                let trimmed = entry.trim();
                if trimmed.is_empty() {
                    return None;
                }
                let mut parts = trimmed.split('|').map(str::trim);
                Some(DkimDomainConfig {
                    domain: parts.next()?.to_ascii_lowercase(),
                    selector: parts.next()?.to_string(),
                    private_key_path: parts.next()?.to_string(),
                    enabled: true,
                })
            })
            .collect::<Vec<_>>();
        if !domains.is_empty() {
            state.policies.dkim.domains = domains;
        }
    }
    state.local_data_stores.state_file_path = env::var("LPE_CT_STATE_FILE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| state.local_data_stores.state_file_path.clone());
    state.local_data_stores.spool_root = env::var("LPE_CT_SPOOL_DIR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| state.local_data_stores.spool_root.clone());
    if let Ok(value) = env::var("LPE_CT_LOCAL_DB_ENABLED") {
        state.local_data_stores.dedicated_postgres.enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_LOCAL_DB_LISTEN_ADDRESS") {
        let trimmed = value.trim();
        state.local_data_stores.dedicated_postgres.listen_address = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        };
    }
    if let Ok(value) = env::var("LPE_CT_LOCAL_DB_NETWORK_SCOPE") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            state.local_data_stores.dedicated_postgres.network_scope = trimmed.to_string();
        }
    }
    if let Ok(value) = env::var("LPE_CT_LOCAL_DB_PURPOSES") {
        let parsed = parse_csv(&value);
        if !parsed.is_empty() {
            state.local_data_stores.dedicated_postgres.purposes = parsed;
        }
    }
    normalize_public_tls_settings(&mut state.network.public_tls);
}

fn upsert_env_public_tls_profile(
    settings: &mut PublicTlsSettings,
    cert_path: String,
    key_path: String,
) {
    let profile = PublicTlsProfile {
        id: ENV_PUBLIC_TLS_PROFILE_ID.to_string(),
        name: "Environment public TLS".to_string(),
        cert_path,
        key_path,
        created_at: current_timestamp(),
    };
    if let Some(existing) = settings
        .profiles
        .iter_mut()
        .find(|existing| existing.id == ENV_PUBLIC_TLS_PROFILE_ID)
    {
        *existing = profile;
    } else {
        settings.profiles.push(profile);
    }
    settings.active_profile_id = Some(ENV_PUBLIC_TLS_PROFILE_ID.to_string());
}

pub(crate) fn normalize_public_tls_settings(settings: &mut PublicTlsSettings) {
    settings.profiles.retain(|profile| {
        !profile.id.trim().is_empty()
            && !profile.cert_path.trim().is_empty()
            && !profile.key_path.trim().is_empty()
    });
    settings.profiles.sort_by(|left, right| {
        left.name
            .to_ascii_lowercase()
            .cmp(&right.name.to_ascii_lowercase())
            .then_with(|| left.id.cmp(&right.id))
    });
    if let Some(active_id) = settings.active_profile_id.as_deref() {
        if !settings
            .profiles
            .iter()
            .any(|profile| profile.id == active_id)
        {
            settings.active_profile_id = None;
        }
    }
}

pub(crate) fn normalize_policy_settings(policies: &mut PolicySettings) {
    let mut antivirus_provider_chain = policies
        .antivirus_provider_chain
        .iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    let mut seen = std::collections::BTreeSet::new();
    antivirus_provider_chain.retain(|value| seen.insert(value.clone()));
    if policies.antivirus_enabled && antivirus_provider_chain.is_empty() {
        antivirus_provider_chain = default_antivirus_provider_chain();
    }
    policies.antivirus_provider_chain = antivirus_provider_chain;
    if policies.bayespam_min_token_length < 2 {
        policies.bayespam_min_token_length = 2;
    }
    if policies.bayespam_max_tokens < 16 {
        policies.bayespam_max_tokens = 16;
    }
    if policies.bayespam_score_weight < 0.0 {
        policies.bayespam_score_weight = 0.0;
    }
    if policies.reputation_reject_threshold > policies.reputation_quarantine_threshold {
        policies.reputation_reject_threshold = policies.reputation_quarantine_threshold;
    }
    if policies.spam_reject_threshold < policies.spam_quarantine_threshold {
        policies.spam_reject_threshold = policies.spam_quarantine_threshold;
    }
    normalize_csv_rules(&mut policies.address_policy.allow_senders);
    normalize_csv_rules(&mut policies.address_policy.block_senders);
    normalize_csv_rules(&mut policies.address_policy.allow_recipients);
    normalize_csv_rules(&mut policies.address_policy.block_recipients);
    policies.recipient_verification.cache_ttl_seconds =
        policies.recipient_verification.cache_ttl_seconds.max(1);
    normalize_attachment_extension_rules(&mut policies.attachment_policy.allow_extensions);
    normalize_attachment_extension_rules(&mut policies.attachment_policy.block_extensions);
    normalize_csv_rules(&mut policies.attachment_policy.allow_mime_types);
    normalize_csv_rules(&mut policies.attachment_policy.block_mime_types);
    normalize_csv_rules(&mut policies.attachment_policy.allow_detected_types);
    normalize_csv_rules(&mut policies.attachment_policy.block_detected_types);
    normalize_csv_rules(&mut policies.dkim.headers);
    if policies.dkim.headers.is_empty() {
        policies.dkim.headers = default_dkim_headers();
    }
    if policies.dkim.headers.iter().all(|value| value != "sender") {
        policies.dkim.headers.push("sender".to_string());
    }
    let mut seen_domains = std::collections::BTreeSet::new();
    policies.dkim.domains.retain_mut(|domain| {
        domain.domain = domain.domain.trim().to_ascii_lowercase();
        domain.selector = domain.selector.trim().to_string();
        domain.private_key_path = domain.private_key_path.trim().to_string();
        !domain.domain.is_empty()
            && !domain.selector.is_empty()
            && !domain.private_key_path.is_empty()
            && seen_domains.insert(domain.domain.clone())
    });
}

pub(crate) fn validate_relay_settings(settings: &mut RelaySettings) -> Result<(), ApiError> {
    settings.outbound_ehlo_name = normalize_outbound_ehlo_name(&settings.outbound_ehlo_name);
    if !is_valid_domain_name(&settings.outbound_ehlo_name) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "outbound EHLO name must be a fully-qualified hostname such as mx.example.com",
        ));
    }
    settings.primary_upstream = settings.primary_upstream.trim().to_string();
    settings.secondary_upstream = settings.secondary_upstream.trim().to_string();
    settings.core_delivery_base_url = settings.core_delivery_base_url.trim().to_string();
    settings.sync_interval_seconds = settings.sync_interval_seconds.max(1);
    Ok(())
}

pub(crate) fn normalize_relay_settings(settings: &mut RelaySettings, site: &SiteProfile) {
    if validate_relay_settings(settings).is_err() {
        settings.outbound_ehlo_name = default_outbound_ehlo_name_for_site(site);
        let _ = validate_relay_settings(settings);
    }
}

pub(crate) fn accepted_domain_from_input(
    input: AcceptedDomainInput,
    existing_id: Option<String>,
) -> Result<AcceptedDomain, ApiError> {
    let domain = normalize_domain_name(&input.domain);
    if !is_valid_domain_name(&domain) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "accepted domain is invalid",
        ));
    }
    let destination_server = input.destination_server.trim().to_string();
    if destination_server.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "destination server is required",
        ));
    }
    let verification_type = normalize_verification_type(&input.verification_type)?;
    Ok(AcceptedDomain {
        id: existing_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
        domain,
        destination_server,
        verification_type,
        rbl_checks: input.rbl_checks,
        spf_checks: input.spf_checks,
        greylisting: input.greylisting,
        accept_null_reverse_path: input.accept_null_reverse_path,
        verified: input.verified,
    })
}

pub(crate) fn normalize_accepted_domains(domains: &mut Vec<AcceptedDomain>) {
    let mut seen = std::collections::BTreeSet::new();
    domains.retain_mut(|domain| {
        if domain.id.trim().is_empty() {
            domain.id = Uuid::new_v4().to_string();
        }
        domain.domain = normalize_domain_name(&domain.domain);
        domain.destination_server = domain.destination_server.trim().to_string();
        domain.verification_type = normalize_verification_type(&domain.verification_type)
            .unwrap_or_else(|_| "none".to_string());
        is_valid_domain_name(&domain.domain)
            && !domain.destination_server.is_empty()
            && seen.insert(domain.domain.clone())
    });
    domains.sort_by(|left, right| left.domain.cmp(&right.domain));
}

fn normalize_domain_name(value: &str) -> String {
    value.trim().trim_start_matches('@').to_ascii_lowercase()
}

fn normalize_outbound_ehlo_name(value: &str) -> String {
    value.trim().trim_end_matches('.').to_ascii_lowercase()
}

fn is_valid_domain_name(value: &str) -> bool {
    let value = value.trim();
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

pub(crate) fn normalize_verification_type(value: &str) -> Result<String, ApiError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "none" => Ok("none".to_string()),
        "dynamic" => Ok("dynamic".to_string()),
        "ldap" => Ok("ldap".to_string()),
        "allowed" => Ok("allowed".to_string()),
        _ => Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "verification type must be one of none, dynamic, ldap, or allowed",
        )),
    }
}

#[derive(Debug)]
pub(crate) struct LpeCoreDeliveryProbe {
    pub(crate) verified: bool,
    pub(crate) checked_url: String,
    pub(crate) detail: String,
}

#[derive(Debug)]
pub(crate) struct LpeRecipientBridgeProbe {
    pub(crate) reachable: bool,
    pub(crate) recipient_verified: bool,
    pub(crate) checked_url: String,
    pub(crate) detail: String,
}

pub(crate) async fn probe_lpe_core_delivery(
    core_delivery_base_url: &str,
) -> Result<LpeCoreDeliveryProbe, ApiError> {
    let checked_url = lpe_health_probe_url(core_delivery_base_url)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(1_500))
        .build()
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    let response = match client.get(&checked_url).send().await {
        Ok(response) => response,
        Err(error) => {
            return Ok(LpeCoreDeliveryProbe {
                verified: false,
                checked_url,
                detail: format!("core LPE delivery API is unreachable: {error}"),
            });
        }
    };
    let status = response.status();
    if !status.is_success() {
        return Ok(LpeCoreDeliveryProbe {
            verified: false,
            checked_url,
            detail: format!("core LPE delivery API health check returned HTTP {status}"),
        });
    }
    let health = match response.json::<LpeHealthProbeResponse>().await {
        Ok(health) => health,
        Err(error) => {
            return Ok(LpeCoreDeliveryProbe {
                verified: false,
                checked_url,
                detail: format!("core LPE delivery API did not return LPE health JSON: {error}"),
            });
        }
    };
    let is_lpe = health.service.as_deref() == Some("lpe-admin-api")
        && health.status.as_deref() == Some("ok");
    Ok(LpeCoreDeliveryProbe {
        verified: is_lpe,
        checked_url,
        detail: if is_lpe {
            "core LPE delivery API is reachable".to_string()
        } else {
            format!(
                "core LPE delivery API health response is not an LPE server signature (service={}, status={})",
                health.service.unwrap_or_else(|| "missing".to_string()),
                health.status.unwrap_or_else(|| "missing".to_string())
            )
        },
    })
}

pub(crate) fn lpe_health_probe_url(core_delivery_base_url: &str) -> Result<String, ApiError> {
    let trimmed = core_delivery_base_url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "core delivery base URL is required",
        ));
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Ok(format!("{trimmed}/health/live"));
    }
    Ok(format!("http://{trimmed}/health/live"))
}

pub(crate) async fn probe_lpe_recipient_bridge(
    core_delivery_base_url: &str,
    domain: &str,
) -> Result<LpeRecipientBridgeProbe, ApiError> {
    let checked_url = lpe_bridge_probe_url(core_delivery_base_url)?;
    let recipient = format!("postmaster@{}", domain.trim().trim_start_matches('@'));
    let request = RecipientVerificationRequest {
        trace_id: format!("lpe-ct-domain-test-{}", Uuid::new_v4()),
        direction: "smtp-inbound".to_string(),
        sender: Some("postmaster@lpe-ct.local".to_string()),
        recipient,
        helo: Some("lpe-ct-domain-test".to_string()),
        peer: None,
        account_id: None,
    };
    let integration_secret = match integration_shared_secret() {
        Ok(secret) => secret,
        Err(error) => {
            return Ok(LpeRecipientBridgeProbe {
                reachable: false,
                recipient_verified: false,
                checked_url,
                detail: format!("integration secret is not usable for bridge testing: {error}"),
            });
        }
    };
    let signed = SignedIntegrationHeaders::sign(
        &integration_secret,
        "POST",
        "/internal/lpe-ct/recipient-verification",
        &request,
    )
    .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(1_500))
        .build()
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let response = match client
        .post(&checked_url)
        .header(INTEGRATION_KEY_HEADER, signed.integration_key)
        .header(INTEGRATION_TIMESTAMP_HEADER, signed.timestamp)
        .header(INTEGRATION_NONCE_HEADER, signed.nonce)
        .header(INTEGRATION_SIGNATURE_HEADER, signed.signature)
        .header("x-trace-id", &request.trace_id)
        .json(&request)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            return Ok(LpeRecipientBridgeProbe {
                reachable: false,
                recipient_verified: false,
                checked_url,
                detail: format!("signed recipient-verification bridge is unreachable: {error}"),
            });
        }
    };
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Ok(LpeRecipientBridgeProbe {
            reachable: false,
            recipient_verified: false,
            checked_url,
            detail: format!("signed recipient-verification bridge returned HTTP {status}: {body}"),
        });
    }
    let body = match response.json::<RecipientVerificationResponse>().await {
        Ok(body) => body,
        Err(error) => {
            return Ok(LpeRecipientBridgeProbe {
                reachable: false,
                recipient_verified: false,
                checked_url,
                detail: format!(
                    "signed recipient-verification bridge returned invalid JSON: {error}"
                ),
            });
        }
    };
    Ok(LpeRecipientBridgeProbe {
        reachable: true,
        recipient_verified: body.verified,
        checked_url,
        detail: if body.verified {
            "test recipient is accepted".to_string()
        } else {
            body.detail
                .unwrap_or_else(|| "bridge reachable; test recipient is not accepted".to_string())
        },
    })
}

pub(crate) fn lpe_bridge_probe_url(core_delivery_base_url: &str) -> Result<String, ApiError> {
    let trimmed = core_delivery_base_url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "core delivery base URL is required",
        ));
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Ok(format!("{trimmed}/internal/lpe-ct/recipient-verification"));
    }
    Ok(format!(
        "http://{trimmed}/internal/lpe-ct/recipient-verification"
    ))
}

pub(crate) fn normalize_local_data_stores(local_data_stores: &mut LocalDataStoresSettings) {
    if local_data_stores.state_file_path.trim().is_empty() {
        local_data_stores.state_file_path = "/var/lib/lpe-ct/state.json".to_string();
    }
    if local_data_stores.spool_root.trim().is_empty() {
        local_data_stores.spool_root = "/var/spool/lpe-ct".to_string();
    }
    if local_data_stores.spool_queues.is_empty() {
        local_data_stores.spool_queues = default_spool_queues();
    }
    if local_data_stores.policy_artifacts.is_empty() {
        local_data_stores.policy_artifacts = default_policy_artifacts();
    }
    if local_data_stores.forbidden_canonical_data.is_empty() {
        local_data_stores.forbidden_canonical_data = default_forbidden_canonical_data();
    }
    local_data_stores.dedicated_postgres.network_scope =
        normalize_local_db_network_scope(&local_data_stores.dedicated_postgres.network_scope);
    if local_data_stores.dedicated_postgres.enabled
        && local_data_stores
            .dedicated_postgres
            .listen_address
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
    {
        local_data_stores.dedicated_postgres.listen_address =
            Some(default_local_db_listen_address());
    }
    if local_data_stores.dedicated_postgres.purposes.is_empty() {
        local_data_stores.dedicated_postgres.purposes = default_local_db_purposes();
    }
    local_data_stores.dedicated_postgres.purposes.sort();
    local_data_stores.dedicated_postgres.purposes.dedup();
}

fn parse_bool(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn normalize_csv_rules(values: &mut Vec<String>) {
    let mut seen = std::collections::BTreeSet::new();
    *values = values
        .iter()
        .map(|value| value.trim().trim_start_matches('@').to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .filter(|value| seen.insert(value.clone()))
        .collect();
}

fn normalize_attachment_extension_rules(values: &mut Vec<String>) {
    let mut seen = std::collections::BTreeSet::new();
    *values = values
        .iter()
        .map(|value| value.trim().trim_start_matches('.').to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .filter(|value| seen.insert(value.clone()))
        .collect();
}

fn env_value(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().trim_matches('_').to_string())
        .filter(|value| !value.is_empty())
}

fn required_trimmed_env(name: &str) -> Result<String> {
    env_value(name).ok_or_else(|| anyhow::anyhow!("{name} must be set"))
}

fn local_hostname() -> String {
    env_value("HOSTNAME")
        .or_else(|| env_value("COMPUTERNAME"))
        .unwrap_or_else(|| "localhost".to_string())
}

pub(crate) fn ensure_management_bootstrap(state: &mut DashboardState) -> Result<()> {
    if state.management_auth.admin_email.trim().is_empty()
        || state.management_auth.password_hash.trim().is_empty()
    {
        let admin_email = required_trimmed_env("LPE_CT_BOOTSTRAP_ADMIN_EMAIL")?.to_lowercase();
        let password = required_trimmed_env("LPE_CT_BOOTSTRAP_ADMIN_PASSWORD")?;
        if is_known_weak_secret(password.trim()) {
            anyhow::bail!(
                "LPE_CT_BOOTSTRAP_ADMIN_PASSWORD uses a forbidden weak placeholder value"
            );
        }
        state.management_auth = ManagementAuthState {
            admin_email: admin_email.clone(),
            password_hash: hash_password(password.trim())?,
        };
        state.audit.insert(
            0,
            AuditEvent {
                timestamp: current_timestamp(),
                actor: "system".to_string(),
                action: "seed-management-admin".to_string(),
                details: format!("Bootstrap LPE-CT management admin prepared for {admin_email}"),
            },
        );
        state.audit.truncate(12);
    }

    Ok(())
}

fn parse_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

pub(crate) fn default_state() -> DashboardState {
    let node_name = env_value("LPE_CT_NODE_NAME")
        .or_else(|| env_value("LPE_CT_SERVER_NAME"))
        .unwrap_or_else(local_hostname);
    let management_fqdn = env_value("LPE_CT_MANAGEMENT_FQDN")
        .or_else(|| env_value("LPE_CT_SERVER_NAME"))
        .unwrap_or_else(|| node_name.clone());
    let published_mx = env_value("LPE_CT_PUBLISHED_MX").unwrap_or_else(|| management_fqdn.clone());
    DashboardState {
        site: SiteProfile {
            node_name,
            role: "dmz-sorting-center".to_string(),
            region: env_value("LPE_CT_REGION").unwrap_or_default(),
            dmz_zone: env_value("LPE_CT_DMZ_ZONE").unwrap_or_default(),
            published_mx,
            management_fqdn,
            public_smtp_bind: env_value("LPE_CT_SMTP_BIND_ADDRESS")
                .unwrap_or_else(|| "0.0.0.0:25".to_string()),
            management_bind: env_value("LPE_CT_BIND_ADDRESS")
                .unwrap_or_else(|| "127.0.0.1:8380".to_string()),
        },
        relay: RelaySettings {
            primary_upstream: String::new(),
            secondary_upstream: String::new(),
            outbound_ehlo_name: default_outbound_ehlo_name(),
            core_delivery_base_url: default_core_delivery_base_url(),
            mutual_tls_required: false,
            fallback_to_hold_queue: false,
            sync_interval_seconds: 30,
            lan_dependency_note: "Only relay and management flows to the LAN are allowed."
                .to_string(),
        },
        accepted_domains: Vec::new(),
        routing: RoutingSettings { rules: Vec::new() },
        throttling: ThrottlingSettings {
            enabled: true,
            rules: vec![ThrottleRule {
                id: "per-recipient-domain".to_string(),
                scope: "recipient-domain".to_string(),
                recipient_domain: None,
                sender_domain: None,
                max_messages: 20,
                window_seconds: 60,
                retry_after_seconds: 120,
            }],
        },
        network: NetworkSettings {
            allowed_management_cidrs: env_value("LPE_CT_ALLOWED_MANAGEMENT_CIDRS")
                .map(|value| parse_csv(&value))
                .unwrap_or_default(),
            allowed_upstream_cidrs: env_value("LPE_CT_ALLOWED_UPSTREAM_CIDRS")
                .map(|value| parse_csv(&value))
                .unwrap_or_default(),
            outbound_smart_hosts: Vec::new(),
            public_listener_enabled: true,
            submission_listener_enabled: false,
            proxy_protocol_enabled: false,
            max_concurrent_sessions: 250,
            public_tls: PublicTlsSettings::default(),
        },
        local_data_stores: LocalDataStoresSettings {
            state_file_path: env_value("LPE_CT_STATE_FILE")
                .unwrap_or_else(|| "/var/lib/lpe-ct/state.json".to_string()),
            spool_root: env_value("LPE_CT_SPOOL_DIR")
                .unwrap_or_else(|| "/var/spool/lpe-ct".to_string()),
            spool_queues: default_spool_queues(),
            policy_artifacts: default_policy_artifacts(),
            forbidden_canonical_data: default_forbidden_canonical_data(),
            dedicated_postgres: LocalPostgresStore {
                enabled: true,
                purposes: default_local_db_purposes(),
                listen_address: Some(default_local_db_listen_address()),
                network_scope: default_local_db_network_scope(),
                public_exposure_forbidden: true,
                notes: default_local_db_notes(),
            },
        },
        policies: PolicySettings {
            drain_mode: false,
            quarantine_enabled: true,
            greylisting_enabled: true,
            antivirus_enabled: default_antivirus_enabled(),
            antivirus_fail_closed: default_antivirus_fail_closed(),
            antivirus_provider_chain: default_antivirus_provider_chain(),
            bayespam_enabled: default_bayespam_enabled(),
            bayespam_auto_learn: default_bayespam_auto_learn(),
            bayespam_score_weight: default_bayespam_score_weight(),
            bayespam_min_token_length: default_bayespam_min_token_length(),
            bayespam_max_tokens: default_bayespam_max_tokens(),
            require_spf: true,
            require_dkim_alignment: false,
            require_dmarc_enforcement: true,
            defer_on_auth_tempfail: default_defer_on_auth_tempfail(),
            dnsbl_enabled: default_dnsbl_enabled(),
            dnsbl_zones: default_dnsbl_zones(),
            reputation_enabled: default_reputation_enabled(),
            reputation_quarantine_threshold: default_reputation_quarantine_threshold(),
            reputation_reject_threshold: default_reputation_reject_threshold(),
            spam_quarantine_threshold: default_spam_quarantine_threshold(),
            spam_reject_threshold: default_spam_reject_threshold(),
            attachment_text_scan_enabled: true,
            max_message_size_mb: 64,
            address_policy: AddressPolicySettings {
                allow_senders: Vec::new(),
                block_senders: Vec::new(),
                allow_recipients: Vec::new(),
                block_recipients: Vec::new(),
            },
            recipient_verification: default_recipient_verification_settings(),
            attachment_policy: AttachmentPolicySettings {
                allow_extensions: Vec::new(),
                block_extensions: Vec::new(),
                allow_mime_types: Vec::new(),
                block_mime_types: Vec::new(),
                allow_detected_types: Vec::new(),
                block_detected_types: Vec::new(),
            },
            dkim: default_dkim_settings(),
        },
        reporting: reporting::default_reporting_settings(),
        updates: UpdateSettings {
            channel: "stable".to_string(),
            auto_download: false,
            maintenance_window: "Sun 02:30".to_string(),
            last_applied_release: "bootstrap".to_string(),
            update_source: env_value("LPE_CT_UPDATE_SOURCE")
                .unwrap_or_else(|| "git checkout".to_string()),
        },
        queues: QueueMetrics {
            inbound_messages: 0,
            incoming_messages: 0,
            active_messages: 0,
            deferred_messages: 0,
            quarantined_messages: 0,
            held_messages: 0,
            corrupt_messages: 0,
            delivery_attempts_last_hour: 0,
            upstream_reachable: true,
        },
        management_auth: ManagementAuthState {
            admin_email: String::new(),
            password_hash: String::new(),
        },
        audit: Vec::new(),
    }
}

pub(crate) fn default_core_delivery_base_url() -> String {
    env_value("LPE_CT_CORE_DELIVERY_BASE_URL")
        .unwrap_or_else(|| "http://127.0.0.1:8080".to_string())
}

pub(crate) fn default_outbound_ehlo_name() -> String {
    [
        "LPE_CT_PUBLISHED_MX",
        "LPE_CT_MANAGEMENT_FQDN",
        "LPE_CT_SERVER_NAME",
    ]
    .into_iter()
    .filter_map(env_value)
    .map(|value| normalize_outbound_ehlo_name(&value))
    .find(|value| is_valid_domain_name(value))
    .unwrap_or_else(|| "lpe-ct.local".to_string())
}

fn default_outbound_ehlo_name_for_site(site: &SiteProfile) -> String {
    [&site.published_mx, &site.management_fqdn]
        .into_iter()
        .map(|value| normalize_outbound_ehlo_name(value))
        .find(|value| is_valid_domain_name(value))
        .unwrap_or_else(default_outbound_ehlo_name)
}

pub(crate) fn default_recipient_verification_cache_ttl_seconds() -> u32 {
    300
}

pub(crate) fn default_recipient_verification_settings() -> RecipientVerificationSettings {
    RecipientVerificationSettings {
        enabled: false,
        fail_closed: true,
        cache_ttl_seconds: default_recipient_verification_cache_ttl_seconds(),
    }
}

pub(crate) fn default_dkim_headers() -> Vec<String> {
    vec![
        "from".to_string(),
        "sender".to_string(),
        "to".to_string(),
        "cc".to_string(),
        "subject".to_string(),
        "mime-version".to_string(),
        "content-type".to_string(),
        "message-id".to_string(),
    ]
}

pub(crate) fn default_dkim_settings() -> DkimSettings {
    DkimSettings {
        enabled: false,
        headers: default_dkim_headers(),
        over_sign: true,
        expiration_seconds: Some(3600),
        domains: Vec::new(),
    }
}

pub(crate) fn submission_listener_is_configured(bind_address: &Option<String>) -> bool {
    bind_address
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some()
        && env::var("LPE_CT_SUBMISSION_TLS_CERT_PATH")
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
        && env::var("LPE_CT_SUBMISSION_TLS_KEY_PATH")
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
}

pub(crate) fn default_true() -> bool {
    true
}

pub(crate) fn default_spool_queues() -> Vec<String> {
    smtp::SPOOL_QUEUES
        .iter()
        .map(|value| (*value).to_string())
        .collect()
}

pub(crate) fn default_policy_artifacts() -> Vec<String> {
    smtp::POLICY_ARTIFACTS
        .iter()
        .map(|value| (*value).to_string())
        .collect()
}

pub(crate) fn default_forbidden_canonical_data() -> Vec<String> {
    [
        "mailboxes",
        "inbox",
        "sent",
        "drafts",
        "outbox",
        "contacts",
        "calendars",
        "tasks",
        "rights",
        "tenant-administration",
        "canonical-search",
        "bcc-business-storage",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub(crate) fn default_local_db_purposes() -> Vec<String> {
    [
        "bayesian",
        "reputation",
        "greylisting",
        "quarantine-metadata",
        "cluster-coordination",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub(crate) fn default_local_db_network_scope() -> String {
    "host-local".to_string()
}

pub(crate) fn default_local_db_listen_address() -> String {
    "127.0.0.1:5432".to_string()
}

pub(crate) fn default_local_db_notes() -> String {
    "Dedicated LPE-CT PostgreSQL is the default technical state store and may hold only perimeter-owned technical state."
        .to_string()
}

fn normalize_local_db_network_scope(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "host-local" | "private-backend" | "lpe-ct-cluster" => value.trim().to_ascii_lowercase(),
        _ => default_local_db_network_scope(),
    }
}

pub(crate) fn default_dnsbl_enabled() -> bool {
    true
}

pub(crate) fn default_antivirus_enabled() -> bool {
    false
}

pub(crate) fn default_antivirus_fail_closed() -> bool {
    true
}

pub(crate) fn default_antivirus_provider_chain() -> Vec<String> {
    vec!["takeri".to_string()]
}

pub(crate) fn default_bayespam_enabled() -> bool {
    true
}

pub(crate) fn default_bayespam_auto_learn() -> bool {
    true
}

pub(crate) fn default_bayespam_score_weight() -> f32 {
    6.0
}

pub(crate) fn default_bayespam_min_token_length() -> u32 {
    3
}

pub(crate) fn default_bayespam_max_tokens() -> u32 {
    256
}

pub(crate) fn default_defer_on_auth_tempfail() -> bool {
    true
}

pub(crate) fn default_dnsbl_zones() -> Vec<String> {
    vec!["zen.spamhaus.org".to_string(), "bl.spamcop.net".to_string()]
}

pub(crate) fn default_reputation_enabled() -> bool {
    true
}

pub(crate) fn default_reputation_quarantine_threshold() -> i32 {
    -4
}

pub(crate) fn default_reputation_reject_threshold() -> i32 {
    -8
}

pub(crate) fn default_spam_quarantine_threshold() -> f32 {
    5.0
}

pub(crate) fn default_spam_reject_threshold() -> f32 {
    9.0
}
