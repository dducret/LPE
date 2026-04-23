use crate::{
    hash_password,
    types::{BootstrapAdminRequest, BootstrapAdminResponse},
    MIN_ADMIN_PASSWORD_LEN, MIN_INTEGRATION_SECRET_LEN,
};
use lpe_storage::{AdminCredentialInput, AuditEntryInput, NewServerAdministrator, Storage};
use std::env;

pub fn bootstrap_admin_request_from_env() -> anyhow::Result<BootstrapAdminRequest> {
    let email = required_env("LPE_BOOTSTRAP_ADMIN_EMAIL")?;
    let display_name = env::var("LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME")
        .unwrap_or_else(|_| "Bootstrap Administrator".to_string())
        .trim()
        .to_string();
    let password = required_env("LPE_BOOTSTRAP_ADMIN_PASSWORD")?;

    validate_bootstrap_admin_request(&email, &display_name, &password)?;

    Ok(BootstrapAdminRequest {
        email,
        display_name,
        password,
    })
}

pub fn bootstrap_admin_request_from_env_or_defaults() -> anyhow::Result<BootstrapAdminRequest> {
    let email = required_env("LPE_BOOTSTRAP_ADMIN_EMAIL")?;
    let display_name = env::var("LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME")
        .unwrap_or_else(|_| "Bootstrap Administrator".to_string())
        .trim()
        .to_string();
    let password = required_env("LPE_BOOTSTRAP_ADMIN_PASSWORD")?;

    validate_bootstrap_admin_request(&email, &display_name, &password)?;

    Ok(BootstrapAdminRequest {
        email,
        display_name,
        password,
    })
}

pub async fn bootstrap_admin(
    storage: &Storage,
    request: BootstrapAdminRequest,
) -> anyhow::Result<BootstrapAdminResponse> {
    validate_bootstrap_admin_request(&request.email, &request.display_name, &request.password)?;

    if storage.has_admin_bootstrap_state().await? {
        anyhow::bail!("bootstrap administrator already exists");
    }

    let email = request.email.trim().to_lowercase();
    let display_name = request.display_name.trim().to_string();
    storage
        .create_server_administrator(
            NewServerAdministrator {
                domain_id: None,
                email: email.clone(),
                display_name: display_name.clone(),
                role: "server-admin".to_string(),
                rights_summary:
                    "server, domains, accounts, aliases, admins, policies, security, ai, antispam, pst, audit, mail"
                        .to_string(),
                permissions: vec!["*".to_string()],
            },
            AuditEntryInput {
                actor: "bootstrap-cli".to_string(),
                action: "create-bootstrap-admin".to_string(),
                subject: email.clone(),
            },
        )
        .await?;

    storage
        .upsert_admin_credential(
            AdminCredentialInput {
                email: email.clone(),
                password_hash: hash_password(&request.password)?,
            },
            AuditEntryInput {
                actor: "bootstrap-cli".to_string(),
                action: "set-bootstrap-password".to_string(),
                subject: email.clone(),
            },
        )
        .await?;

    Ok(BootstrapAdminResponse {
        email,
        display_name,
    })
}

pub fn integration_shared_secret() -> anyhow::Result<String> {
    let secret = required_env("LPE_INTEGRATION_SHARED_SECRET")?;
    validate_shared_secret("LPE_INTEGRATION_SHARED_SECRET", &secret)?;
    Ok(secret)
}

fn required_env(name: &str) -> anyhow::Result<String> {
    let value = env::var(name)
        .map_err(|_| anyhow::anyhow!("{name} must be set"))?
        .trim()
        .to_string();
    if value.is_empty() {
        anyhow::bail!("{name} must not be empty");
    }
    Ok(value)
}

fn validate_bootstrap_admin_request(
    email: &str,
    display_name: &str,
    password: &str,
) -> anyhow::Result<()> {
    if !email.contains('@') {
        anyhow::bail!("bootstrap admin email must contain '@'");
    }
    if display_name.trim().is_empty() {
        anyhow::bail!("bootstrap admin display name must not be empty");
    }
    validate_admin_password(password)?;
    Ok(())
}

fn validate_admin_password(password: &str) -> anyhow::Result<()> {
    let trimmed = password.trim();
    if trimmed.len() < MIN_ADMIN_PASSWORD_LEN {
        anyhow::bail!(
            "bootstrap admin password must contain at least {MIN_ADMIN_PASSWORD_LEN} characters"
        );
    }
    if is_known_weak_secret(trimmed) {
        anyhow::bail!("bootstrap admin password uses a forbidden weak placeholder value");
    }
    Ok(())
}

fn validate_shared_secret(name: &str, secret: &str) -> anyhow::Result<()> {
    let trimmed = secret.trim();
    if trimmed.len() < MIN_INTEGRATION_SECRET_LEN {
        anyhow::bail!("{name} must contain at least {MIN_INTEGRATION_SECRET_LEN} characters");
    }
    if is_known_weak_secret(trimmed) {
        anyhow::bail!("{name} uses a forbidden weak placeholder value");
    }
    Ok(())
}

fn is_known_weak_secret(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "change-me"
            | "changeme"
            | "secret"
            | "shared-secret"
            | "integration-test"
            | "password"
            | "admin"
            | "default"
            | "test"
            | "example"
    )
}
