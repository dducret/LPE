use axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{delete, get, post, put},
    Router,
};
use lpe_storage::Storage;

mod access;
mod account_oidc;
mod admin_auth;
mod bootstrap;
mod client_auth;
mod client_config;
mod console;
mod delegation;
mod health;
mod http;
mod integration;
mod observability;
mod oidc;
mod pst;
mod readiness;
mod security;
mod sieve;
mod totp;
mod types;
mod util;
mod workspace;

pub use crate::bootstrap::{
    bootstrap_admin, bootstrap_admin_request_from_env,
    bootstrap_admin_request_from_env_or_defaults, integration_shared_secret,
};

pub(crate) use crate::access::{require_account, require_admin};
pub(crate) use crate::readiness::{
    build_readiness_response, check_optional_http_dependency, ha_activation_check, lpe_ct_base_url,
    readiness_failed, readiness_ok,
};
pub use crate::readiness::{ha_allows_active_work, ha_current_role};
use crate::{
    admin_auth::{
        admin_auth_factors, enroll_totp, login, logout, me, oidc_callback, oidc_metadata,
        oidc_start, revoke_admin_factor, verify_totp_factor,
    },
    client_auth::{
        account_auth_factors, client_login, client_logout, client_me, client_oidc_callback,
        client_oidc_metadata, client_oidc_start, create_account_app_password,
        create_client_oauth_access_token, enroll_account_totp, list_account_app_passwords,
        revoke_account_app_password, revoke_account_factor, verify_account_totp_factor,
    },
    console::{
        attachment_support, create_account, create_alias, create_domain, create_filter_rule,
        create_mailbox, create_pst_transfer_job, create_server_administrator, dashboard,
        local_ai_health, mail_flow, run_pst_jobs, search_email_trace, update_account,
        update_antispam_settings, update_domain, update_local_ai_settings,
        update_security_settings, update_server_settings, upload_pst_import,
    },
    delegation::{
        delete_collaboration_grant, delete_mailbox_delegation_grant,
        delete_sender_delegation_grant, delete_task_list_grant, get_mailbox_delegation,
        list_collaboration_overview, upsert_collaboration_grant, upsert_mailbox_delegation_grant,
        upsert_sender_delegation_grant, upsert_task_list_grant,
    },
    health::{health, health_live, health_ready},
    http::bad_request_error,
    integration::{
        accept_smtp_submission, authenticate_smtp_submission, deliver_inbound_message,
        verify_lpe_ct_recipient,
    },
    pst::pst_upload_max_bytes,
    security::hash_password,
    sieve::{
        delete_sieve_script, get_sieve_overview, get_sieve_script, put_sieve_script,
        rename_sieve_script, set_active_sieve_script,
    },
    util::{parse_collaboration_kind, parse_sender_delegation_right},
    workspace::{
        client_workspace, delete_client_task, delete_draft_message, get_client_task,
        list_client_task_lists, list_client_tasks, save_draft_message, submit_message,
        upsert_client_contact, upsert_client_event, upsert_client_task,
    },
};

pub(crate) const MIN_ADMIN_PASSWORD_LEN: usize = 12;
pub(crate) const MIN_INTEGRATION_SECRET_LEN: usize = 32;

pub fn router(storage: Storage) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/health/live", get(health_live))
        .route("/health/ready", get(health_ready))
        .route("/metrics", get(observability::metrics_endpoint))
        .route("/auth/login", post(login))
        .route("/auth/logout", post(logout))
        .route("/auth/me", get(me))
        .route("/auth/factors", get(admin_auth_factors))
        .route("/auth/factors/totp/enroll", post(enroll_totp))
        .route("/auth/factors/totp/verify", post(verify_totp_factor))
        .route("/auth/factors/{factor_id}", delete(revoke_admin_factor))
        .route("/auth/oidc/metadata", get(oidc_metadata))
        .route("/auth/oidc/start", get(oidc_start))
        .route("/auth/oidc/callback", get(oidc_callback))
        .route("/mail/auth/login", post(client_login))
        .route("/mail/auth/logout", post(client_logout))
        .route("/mail/auth/me", get(client_me))
        .route("/mail/auth/factors", get(account_auth_factors))
        .route("/mail/auth/factors/totp/enroll", post(enroll_account_totp))
        .route(
            "/mail/auth/factors/totp/verify",
            post(verify_account_totp_factor),
        )
        .route(
            "/mail/auth/factors/{factor_id}",
            delete(revoke_account_factor),
        )
        .route(
            "/mail/auth/app-passwords",
            get(list_account_app_passwords).post(create_account_app_password),
        )
        .route(
            "/mail/auth/app-passwords/{app_password_id}",
            delete(revoke_account_app_password),
        )
        .route(
            "/mail/auth/oauth/access-token",
            post(create_client_oauth_access_token),
        )
        .route("/mail/auth/oidc/metadata", get(client_oidc_metadata))
        .route("/mail/auth/oidc/start", get(client_oidc_start))
        .route("/mail/auth/oidc/callback", get(client_oidc_callback))
        .route("/mail/workspace", get(client_workspace))
        .route(
            "/mail/tasks",
            get(list_client_tasks).post(upsert_client_task),
        )
        .route("/mail/task-lists", get(list_client_task_lists))
        .route(
            "/mail/tasks/{task_id}",
            get(get_client_task).delete(delete_client_task),
        )
        .route("/health/local-ai", get(local_ai_health))
        .route("/capabilities/attachments", get(attachment_support))
        .route("/console/dashboard", get(dashboard))
        .route("/console/accounts", post(create_account))
        .route("/console/accounts/{account_id}", put(update_account))
        .route("/console/mailboxes", post(create_mailbox))
        .route("/console/mailboxes/pst-jobs", post(create_pst_transfer_job))
        .route(
            "/console/mailboxes/{mailbox_id}/pst-upload",
            post(upload_pst_import),
        )
        .route("/console/domains", post(create_domain))
        .route("/console/domains/{domain_id}", put(update_domain))
        .route("/console/aliases", post(create_alias))
        .route("/console/admins", post(create_server_administrator))
        .route("/console/antispam/rules", post(create_filter_rule))
        .route("/console/mail-flow", get(mail_flow))
        .route(
            "/console/mailboxes/pst-jobs/run-pending",
            post(run_pst_jobs),
        )
        .route("/mail/messages/submit", post(submit_message))
        .route("/mail/messages/draft", post(save_draft_message))
        .route(
            "/internal/lpe-ct/inbound-deliveries",
            post(deliver_inbound_message),
        )
        .route(
            "/internal/lpe-ct/submission-auth",
            post(authenticate_smtp_submission),
        )
        .route(
            "/internal/lpe-ct/recipient-verification",
            post(verify_lpe_ct_recipient),
        )
        .route("/internal/lpe-ct/submissions", post(accept_smtp_submission))
        .route(
            "/mail/messages/{message_id}/draft",
            delete(delete_draft_message),
        )
        .route("/mail/contacts", post(upsert_client_contact))
        .route("/mail/calendar/events", post(upsert_client_event))
        .route(
            "/mail/shares",
            get(list_collaboration_overview).put(upsert_collaboration_grant),
        )
        .route(
            "/mail/shares/{kind}/{grantee_account_id}",
            delete(delete_collaboration_grant),
        )
        .route(
            "/mail/task-lists/{task_list_id}/shares",
            put(upsert_task_list_grant),
        )
        .route(
            "/mail/task-lists/{task_list_id}/shares/{grantee_account_id}",
            delete(delete_task_list_grant),
        )
        .route("/mail/delegation", get(get_mailbox_delegation))
        .route(
            "/mail/delegation/mailboxes",
            put(upsert_mailbox_delegation_grant),
        )
        .route(
            "/mail/delegation/mailboxes/{grantee_account_id}",
            delete(delete_mailbox_delegation_grant),
        )
        .route(
            "/mail/delegation/sender",
            put(upsert_sender_delegation_grant),
        )
        .route(
            "/mail/delegation/sender/{sender_right}/{grantee_account_id}",
            delete(delete_sender_delegation_grant),
        )
        .route(
            "/mail/sieve",
            get(get_sieve_overview).post(put_sieve_script),
        )
        .route("/mail/sieve/rename", post(rename_sieve_script))
        .route("/mail/sieve/active", put(set_active_sieve_script))
        .route(
            "/mail/sieve/{name}",
            get(get_sieve_script).delete(delete_sieve_script),
        )
        .route(
            "/console/audit/email-trace-search",
            post(search_email_trace),
        )
        .route("/console/settings/server", put(update_server_settings))
        .route("/console/settings/security", put(update_security_settings))
        .route("/console/settings/local-ai", put(update_local_ai_settings))
        .route("/console/settings/antispam", put(update_antispam_settings))
        .merge(client_config::router())
        .nest("/jmap", lpe_jmap::router())
        .merge(lpe_activesync::router())
        .merge(lpe_dav::router())
        .layer(middleware::from_fn(observability::observe_http))
        .layer(DefaultBodyLimit::max(pst_upload_max_bytes()))
        .with_state(storage)
}

pub fn init_observability(service_name: &str) {
    observability::init_tracing(service_name);
}

pub fn observe_outbound_worker_poll(batch_size: usize) {
    observability::record_outbound_worker_poll(batch_size);
}

pub fn observe_outbound_worker_dispatch(status: &str) {
    observability::record_outbound_dispatch(status);
}

#[cfg(test)]
mod tests {
    use super::{
        bootstrap_admin_request_from_env, bootstrap_admin_request_from_env_or_defaults,
        ha_activation_check, ha_allows_active_work, integration_shared_secret,
    };
    use crate::integration::{
        build_smtp_submission_input_for_owner, merge_smtp_bcc_recipients,
        parse_smtp_submission_sender,
    };
    use crate::pst::validate_uploaded_pst_file_with_validator;
    use lpe_domain::SmtpSubmissionRequest;
    use lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator};
    use lpe_mail_auth::AccountPrincipal;
    use lpe_storage::{
        mail::parse_rfc822_message, SubmissionAccountIdentity, SubmittedRecipientInput,
    };
    use std::{
        fs,
        path::PathBuf,
        sync::{Mutex, MutexGuard},
        time::{SystemTime, UNIX_EPOCH},
    };
    use uuid::Uuid;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn env_lock() -> MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[derive(Debug, Clone)]
    struct FakeDetector {
        detection: MagikaDetection,
    }

    impl Detector for FakeDetector {
        fn detect(&self, _source: DetectionSource<'_>) -> anyhow::Result<MagikaDetection> {
            Ok(self.detection.clone())
        }
    }

    fn temp_file(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!("lpe-pst-upload-{suffix}"));
        fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn pst_upload_validation_accepts_valid_pst_like_file() {
        let path = temp_file("mailbox.pst");
        fs::write(&path, b"LPE-PST-V1\n").unwrap();
        let validator = Validator::new(
            FakeDetector {
                detection: MagikaDetection {
                    label: "pst".to_string(),
                    mime_type: "application/vnd.ms-outlook".to_string(),
                    description: "pst".to_string(),
                    group: "archive".to_string(),
                    extensions: vec!["pst".to_string()],
                    score: Some(0.99),
                },
            },
            0.80,
        );

        validate_uploaded_pst_file_with_validator(
            &validator,
            &path,
            "mailbox.pst",
            Some("application/vnd.ms-outlook"),
        )
        .unwrap();

        assert!(path.exists());
        assert!(path.with_extension("pst.magika.json").exists());
    }

    #[test]
    fn pst_upload_validation_rejects_extension_and_type_mismatch() {
        let path = temp_file("mailbox.pdf");
        fs::write(&path, b"not a pst").unwrap();
        let validator = Validator::new(
            FakeDetector {
                detection: MagikaDetection {
                    label: "pdf".to_string(),
                    mime_type: "application/pdf".to_string(),
                    description: "pdf".to_string(),
                    group: "document".to_string(),
                    extensions: vec!["pdf".to_string()],
                    score: Some(0.99),
                },
            },
            0.80,
        );

        let error = validate_uploaded_pst_file_with_validator(
            &validator,
            &path,
            "mailbox.pdf",
            Some("application/pdf"),
        )
        .unwrap_err();

        assert!(error.to_string().contains("PST upload blocked"));
        assert!(!path.exists());
    }

    #[test]
    #[ignore = "env-sensitive"]
    fn integration_secret_rejects_missing_or_weak_values() {
        let _guard = env_lock();
        std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
        assert!(integration_shared_secret().is_err());

        std::env::set_var("LPE_INTEGRATION_SHARED_SECRET", "change-me");
        assert!(integration_shared_secret().is_err());

        std::env::set_var(
            "LPE_INTEGRATION_SHARED_SECRET",
            "0123456789abcdef0123456789abcdef",
        );
        assert_eq!(
            integration_shared_secret().unwrap(),
            "0123456789abcdef0123456789abcdef"
        );
        std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
    }

    #[test]
    #[ignore = "env-sensitive"]
    fn bootstrap_request_requires_explicit_strong_password() {
        let _guard = env_lock();
        std::env::set_var("LPE_BOOTSTRAP_ADMIN_EMAIL", "admin@example.test");
        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_PASSWORD");
        assert!(bootstrap_admin_request_from_env().is_err());

        std::env::set_var("LPE_BOOTSTRAP_ADMIN_PASSWORD", "change-me");
        assert!(bootstrap_admin_request_from_env().is_err());

        std::env::set_var(
            "LPE_BOOTSTRAP_ADMIN_PASSWORD",
            "Very-Strong-Bootstrap-Password-2026",
        );
        let request = bootstrap_admin_request_from_env().unwrap();
        assert_eq!(request.email, "admin@example.test");
        assert_eq!(request.display_name, "Bootstrap Administrator");

        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_EMAIL");
        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_PASSWORD");
        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME");
    }

    #[test]
    #[ignore = "env-sensitive"]
    fn bootstrap_auto_request_requires_explicit_bootstrap_credentials() {
        let _guard = env_lock();
        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_EMAIL");
        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_PASSWORD");
        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME");

        assert!(bootstrap_admin_request_from_env_or_defaults().is_err());

        std::env::set_var("LPE_BOOTSTRAP_ADMIN_EMAIL", "root@tenant.example");
        std::env::set_var(
            "LPE_BOOTSTRAP_ADMIN_PASSWORD",
            "Very-Strong-Bootstrap-Password-2026",
        );
        let request = bootstrap_admin_request_from_env_or_defaults().unwrap();
        assert_eq!(request.email, "root@tenant.example");
        assert_eq!(request.password, "Very-Strong-Bootstrap-Password-2026");

        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_EMAIL");
        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_PASSWORD");
    }

    #[test]
    #[ignore = "env-sensitive"]
    fn ha_role_check_accepts_only_active_role() {
        let _guard = env_lock();
        let role_file = temp_file("ha-role");

        std::env::set_var("LPE_HA_ROLE_FILE", &role_file);

        fs::write(&role_file, b"active\n").unwrap();
        let active = ha_activation_check();
        assert_eq!(active.status, "ok");

        fs::write(&role_file, b"standby\n").unwrap();
        let standby = ha_activation_check();
        assert_eq!(standby.status, "failed");
        assert!(standby.detail.contains("standby"));

        fs::write(&role_file, b"broken\n").unwrap();
        let invalid = ha_activation_check();
        assert_eq!(invalid.status, "failed");
        assert!(invalid.detail.contains("unsupported role"));

        std::env::remove_var("LPE_HA_ROLE_FILE");
    }

    #[test]
    #[ignore = "env-sensitive"]
    fn ha_active_work_follows_role_file() {
        let _guard = env_lock();
        let role_file = temp_file("ha-active-work");

        std::env::remove_var("LPE_HA_ROLE_FILE");
        assert!(ha_allows_active_work().unwrap());

        std::env::set_var("LPE_HA_ROLE_FILE", &role_file);
        fs::write(&role_file, b"active\n").unwrap();
        assert!(ha_allows_active_work().unwrap());

        fs::write(&role_file, b"maintenance\n").unwrap();
        assert!(!ha_allows_active_work().unwrap());

        std::env::remove_var("LPE_HA_ROLE_FILE");
    }

    #[test]
    fn smtp_submission_derives_envelope_only_recipients_as_bcc() {
        let raw = concat!(
            "From: Alice <alice@example.test>\r\n",
            "To: Bob <bob@example.test>\r\n",
            "Bcc: Hidden <hidden@example.test>\r\n",
            "Subject: Hi\r\n",
            "\r\n",
            "Body\r\n"
        );

        let bcc = merge_smtp_bcc_recipients(
            raw.as_bytes(),
            &[
                "bob@example.test".to_string(),
                "hidden@example.test".to_string(),
                "blind2@example.test".to_string(),
            ],
            &[lpe_storage::SubmittedRecipientInput {
                address: "bob@example.test".to_string(),
                display_name: Some("Bob".to_string()),
            }],
            &[],
        );

        assert_eq!(bcc.len(), 2);
        assert_eq!(bcc[0].address, "hidden@example.test");
        assert_eq!(bcc[1].address, "blind2@example.test");
    }

    #[test]
    fn smtp_submission_builds_canonical_submit_input() {
        let principal = AccountPrincipal {
            tenant_id: "tenant-a".to_string(),
            account_id: Uuid::nil(),
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
        };
        let request = SmtpSubmissionRequest {
            trace_id: "trace-1".to_string(),
            helo: "laptop.example.test".to_string(),
            peer: "203.0.113.55:41234".to_string(),
            account_id: Uuid::nil(),
            account_email: "alice@example.test".to_string(),
            account_display_name: "Alice".to_string(),
            mail_from: "alice@example.test".to_string(),
            rcpt_to: vec![
                "bob@example.test".to_string(),
                "blind@example.test".to_string(),
            ],
            raw_message: concat!(
                "From: Alice <alice@example.test>\r\n",
                "To: Bob <bob@example.test>\r\n",
                "Subject: Hello\r\n",
                "\r\n",
                "Body\r\n"
            )
            .as_bytes()
            .to_vec(),
        };
        let parsed = parse_rfc822_message(&request.raw_message).unwrap();
        let sender = parse_smtp_submission_sender(
            &request.raw_message,
            &principal.email,
            &principal.email,
            &principal.email,
        )
        .unwrap();
        let input = build_smtp_submission_input_for_owner(
            &principal,
            &SubmissionAccountIdentity {
                account_id: principal.account_id,
                email: principal.email.clone(),
                display_name: principal.display_name.clone(),
            },
            &request,
            parsed,
            vec![SubmittedRecipientInput {
                address: "bob@example.test".to_string(),
                display_name: Some("Bob".to_string()),
            }],
            Vec::new(),
            vec![SubmittedRecipientInput {
                address: "blind@example.test".to_string(),
                display_name: None,
            }],
            sender,
        );

        assert_eq!(input.source, "smtp-submission");
        assert_eq!(input.from_address, "alice@example.test");
        assert_eq!(input.to.len(), 1);
        assert_eq!(input.to[0].address, "bob@example.test");
        assert_eq!(input.bcc.len(), 1);
        assert_eq!(input.bcc[0].address, "blind@example.test");
    }

    #[test]
    fn smtp_submission_builds_send_as_input_for_delegated_mailbox() {
        let principal = AccountPrincipal {
            tenant_id: "tenant-a".to_string(),
            account_id: Uuid::new_v4(),
            email: "delegate@example.test".to_string(),
            display_name: "Delegate".to_string(),
        };
        let owner = SubmissionAccountIdentity {
            account_id: Uuid::new_v4(),
            email: "shared@example.test".to_string(),
            display_name: "Shared Mailbox".to_string(),
        };
        let request = SmtpSubmissionRequest {
            trace_id: "trace-2".to_string(),
            helo: "laptop.example.test".to_string(),
            peer: "203.0.113.55:41234".to_string(),
            account_id: principal.account_id,
            account_email: principal.email.clone(),
            account_display_name: principal.display_name.clone(),
            mail_from: "shared@example.test".to_string(),
            rcpt_to: vec!["bob@example.test".to_string()],
            raw_message: concat!(
                "From: Shared Mailbox <shared@example.test>\r\n",
                "To: Bob <bob@example.test>\r\n",
                "Subject: Hello\r\n",
                "\r\n",
                "Body\r\n"
            )
            .as_bytes()
            .to_vec(),
        };

        let parsed = parse_rfc822_message(&request.raw_message).unwrap();
        let sender = parse_smtp_submission_sender(
            &request.raw_message,
            &owner.email,
            &principal.email,
            &owner.email,
        )
        .unwrap();
        let input = build_smtp_submission_input_for_owner(
            &principal,
            &owner,
            &request,
            parsed,
            vec![SubmittedRecipientInput {
                address: "bob@example.test".to_string(),
                display_name: Some("Bob".to_string()),
            }],
            Vec::new(),
            Vec::new(),
            sender,
        );

        assert_eq!(input.account_id, owner.account_id);
        assert_eq!(input.submitted_by_account_id, principal.account_id);
        assert_eq!(input.from_address, owner.email);
        assert_eq!(input.sender_address, None);
    }

    #[test]
    fn smtp_submission_builds_send_on_behalf_input_for_delegated_mailbox() {
        let principal = AccountPrincipal {
            tenant_id: "tenant-a".to_string(),
            account_id: Uuid::new_v4(),
            email: "delegate@example.test".to_string(),
            display_name: "Delegate".to_string(),
        };
        let owner = SubmissionAccountIdentity {
            account_id: Uuid::new_v4(),
            email: "shared@example.test".to_string(),
            display_name: "Shared Mailbox".to_string(),
        };
        let request = SmtpSubmissionRequest {
            trace_id: "trace-3".to_string(),
            helo: "laptop.example.test".to_string(),
            peer: "203.0.113.55:41234".to_string(),
            account_id: principal.account_id,
            account_email: principal.email.clone(),
            account_display_name: principal.display_name.clone(),
            mail_from: "delegate@example.test".to_string(),
            rcpt_to: vec!["bob@example.test".to_string()],
            raw_message: concat!(
                "From: Shared Mailbox <shared@example.test>\r\n",
                "Sender: Delegate <delegate@example.test>\r\n",
                "To: Bob <bob@example.test>\r\n",
                "Subject: Hello\r\n",
                "\r\n",
                "Body\r\n"
            )
            .as_bytes()
            .to_vec(),
        };

        let parsed = parse_rfc822_message(&request.raw_message).unwrap();
        let sender = parse_smtp_submission_sender(
            &request.raw_message,
            &owner.email,
            &principal.email,
            &owner.email,
        )
        .unwrap();
        let input = build_smtp_submission_input_for_owner(
            &principal,
            &owner,
            &request,
            parsed,
            vec![SubmittedRecipientInput {
                address: "bob@example.test".to_string(),
                display_name: Some("Bob".to_string()),
            }],
            Vec::new(),
            Vec::new(),
            sender,
        );

        assert_eq!(input.account_id, owner.account_id);
        assert_eq!(input.submitted_by_account_id, principal.account_id);
        assert_eq!(input.from_address, owner.email);
        assert_eq!(
            input.sender_address.as_deref(),
            Some("delegate@example.test")
        );
        assert_eq!(input.sender_display.as_deref(), Some("Delegate"));
    }
}
