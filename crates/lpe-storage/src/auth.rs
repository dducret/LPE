use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    normalize_admin_session_auth_method, normalize_email, permission_summary,
    permissions_from_storage, AccountAppPasswordRow, AccountAuthFactorRow, AccountLoginRow,
    AdminAuthFactorRow, AdminLoginRow, AuditEntryInput, AuthenticatedAccountRow,
    AuthenticatedAdminRow, PLATFORM_TENANT_ID, Storage,
};

#[derive(Debug, Clone, Serialize)]
pub struct AuthenticatedAdmin {
    pub tenant_id: String,
    pub email: String,
    pub display_name: String,
    pub role: String,
    pub domain_id: Option<Uuid>,
    pub domain_name: String,
    pub rights_summary: String,
    pub permissions: Vec<String>,
    pub auth_method: String,
    pub expires_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdminAuthFactor {
    pub id: Uuid,
    pub factor_type: String,
    pub status: String,
    pub created_at: String,
    pub verified_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AdminCredentialInput {
    pub email: String,
    pub password_hash: String,
}

#[derive(Debug, Clone)]
pub struct AccountCredentialInput {
    pub email: String,
    pub password_hash: String,
}

#[derive(Debug, Clone)]
pub struct AdminLogin {
    pub tenant_id: String,
    pub email: String,
    pub password_hash: String,
    pub status: String,
    pub display_name: String,
    pub role: String,
    pub domain_id: Option<Uuid>,
    pub domain_name: String,
    pub rights_summary: String,
    pub permissions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminOidcClaims {
    pub issuer_url: String,
    pub subject: String,
    pub email: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountOidcClaims {
    pub issuer_url: String,
    pub subject: String,
    pub email: String,
    pub display_name: String,
}

#[derive(Debug, Clone)]
pub struct NewAdminAuthFactor {
    pub admin_email: String,
    pub factor_type: String,
    pub secret_ciphertext: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountAuthFactor {
    pub id: Uuid,
    pub factor_type: String,
    pub status: String,
    pub created_at: String,
    pub verified_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewAccountAuthFactor {
    pub account_email: String,
    pub factor_type: String,
    pub secret_ciphertext: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountAppPassword {
    pub id: Uuid,
    pub label: String,
    pub status: String,
    pub created_at: String,
    pub last_used_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StoredAccountAppPassword {
    pub id: Uuid,
    pub password_hash: String,
}

#[derive(Debug, Clone)]
pub struct AccountLogin {
    pub tenant_id: String,
    pub account_id: Uuid,
    pub email: String,
    pub password_hash: String,
    pub status: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthenticatedAccount {
    pub tenant_id: String,
    pub account_id: Uuid,
    pub email: String,
    pub display_name: String,
    pub expires_at: String,
}

impl Storage {
    pub async fn upsert_admin_credential(
        &self,
        input: AdminCredentialInput,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let email = normalize_email(&input.email);
        let tenant_id = self.tenant_id_for_admin_email(&email).await?;
        if email.is_empty() || input.password_hash.trim().is_empty() {
            bail!("admin credential email and password hash are required");
        }

        sqlx::query(
            r#"
            INSERT INTO admin_credentials (email, tenant_id, password_hash, status)
            VALUES ($1, $2, $3, 'active')
            ON CONFLICT (tenant_id, email) DO UPDATE SET
                password_hash = EXCLUDED.password_hash,
                status = 'active',
                updated_at = NOW()
            "#,
        )
        .bind(email)
        .bind(&tenant_id)
        .bind(input.password_hash)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn ensure_admin_credential_stub(&self, email: &str) -> Result<()> {
        let email = normalize_email(email);
        let tenant_id = self.tenant_id_for_admin_email(&email).await?;
        if email.is_empty() {
            bail!("admin credential email is required");
        }

        sqlx::query(
            r#"
            INSERT INTO admin_credentials (email, tenant_id, password_hash, status)
            VALUES ($1, $2, 'federated-only', 'active')
            ON CONFLICT (tenant_id, email) DO UPDATE SET
                status = 'active',
                updated_at = NOW()
            "#,
        )
        .bind(email)
        .bind(&tenant_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn find_admin_oidc_identity(
        &self,
        issuer_url: &str,
        subject: &str,
    ) -> Result<Option<String>> {
        let tenant_id = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM admin_oidc_identities
            WHERE issuer_url = $1 AND subject = $2
            LIMIT 1
            "#,
        )
        .bind(issuer_url.trim())
        .bind(subject.trim())
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or_else(|| PLATFORM_TENANT_ID.to_string());
        let email = sqlx::query_scalar::<_, String>(
            r#"
            SELECT admin_email
            FROM admin_oidc_identities
            WHERE tenant_id = $1 AND issuer_url = $2 AND subject = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(issuer_url.trim())
        .bind(subject.trim())
        .fetch_optional(&self.pool)
        .await?;

        Ok(email)
    }

    pub async fn upsert_admin_oidc_identity(&self, claims: &AdminOidcClaims) -> Result<()> {
        let tenant_id = self.tenant_id_for_admin_email(&claims.email).await?;
        sqlx::query(
            r#"
            INSERT INTO admin_oidc_identities (
                tenant_id, issuer_url, subject, admin_email, created_at, last_login_at
            )
            VALUES ($1, $2, $3, $4, NOW(), NOW())
            ON CONFLICT (tenant_id, issuer_url, subject) DO UPDATE SET
                admin_email = EXCLUDED.admin_email,
                last_login_at = NOW()
            "#,
        )
        .bind(&tenant_id)
        .bind(claims.issuer_url.trim())
        .bind(claims.subject.trim())
        .bind(normalize_email(&claims.email))
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn create_admin_auth_factor(&self, input: NewAdminAuthFactor) -> Result<Uuid> {
        let admin_email = normalize_email(&input.admin_email);
        let tenant_id = self.tenant_id_for_admin_email(&admin_email).await?;
        let factor_id = Uuid::new_v4();

        sqlx::query(
            r#"
            INSERT INTO admin_auth_factors (
                id, tenant_id, admin_email, factor_type, status, secret_ciphertext
            )
            VALUES ($1, $2, $3, $4, 'pending', $5)
            "#,
        )
        .bind(factor_id)
        .bind(&tenant_id)
        .bind(admin_email)
        .bind(input.factor_type.trim().to_lowercase())
        .bind(input.secret_ciphertext)
        .execute(&self.pool)
        .await?;

        Ok(factor_id)
    }

    pub async fn fetch_admin_auth_factors(
        &self,
        admin_email: &str,
    ) -> Result<Vec<AdminAuthFactor>> {
        let admin_email = normalize_email(admin_email);
        let tenant_id = self.tenant_id_for_admin_email(&admin_email).await?;
        let rows = sqlx::query_as::<_, AdminAuthFactorRow>(
            r#"
            SELECT
                id,
                factor_type,
                status,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                CASE
                    WHEN verified_at IS NULL THEN NULL
                    ELSE to_char(verified_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS verified_at,
                secret_ciphertext
            FROM admin_auth_factors
            WHERE tenant_id = $1 AND lower(admin_email) = lower($2)
            ORDER BY created_at DESC
            "#,
        )
        .bind(&tenant_id)
        .bind(admin_email)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| AdminAuthFactor {
                id: row.id,
                factor_type: row.factor_type,
                status: row.status,
                created_at: row.created_at,
                verified_at: row.verified_at,
            })
            .collect())
    }

    pub async fn fetch_admin_totp_secret(
        &self,
        admin_email: &str,
    ) -> Result<Option<(Uuid, String)>> {
        let admin_email = normalize_email(admin_email);
        let tenant_id = self.tenant_id_for_admin_email(&admin_email).await?;
        let row = sqlx::query_as::<_, AdminAuthFactorRow>(
            r#"
            SELECT
                id,
                factor_type,
                status,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                CASE
                    WHEN verified_at IS NULL THEN NULL
                    ELSE to_char(verified_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS verified_at,
                secret_ciphertext
            FROM admin_auth_factors
            WHERE tenant_id = $1
              AND lower(admin_email) = lower($2)
              AND factor_type = 'totp'
              AND status = 'active'
            ORDER BY verified_at DESC NULLS LAST, created_at DESC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(admin_email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.and_then(|row| row.secret_ciphertext.map(|secret| (row.id, secret))))
    }

    pub async fn fetch_pending_admin_factor_secret(
        &self,
        admin_email: &str,
        factor_id: Uuid,
    ) -> Result<Option<String>> {
        let admin_email = normalize_email(admin_email);
        let tenant_id = self.tenant_id_for_admin_email(&admin_email).await?;
        sqlx::query_scalar(
            r#"
            SELECT secret_ciphertext
            FROM admin_auth_factors
            WHERE tenant_id = $1
              AND lower(admin_email) = lower($2)
              AND id = $3
              AND status = 'pending'
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(admin_email)
        .bind(factor_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn activate_admin_auth_factor(
        &self,
        admin_email: &str,
        factor_id: Uuid,
    ) -> Result<bool> {
        let admin_email = normalize_email(admin_email);
        let tenant_id = self.tenant_id_for_admin_email(&admin_email).await?;
        let updated = sqlx::query(
            r#"
            UPDATE admin_auth_factors
            SET status = 'active', verified_at = NOW()
            WHERE tenant_id = $1
              AND lower(admin_email) = lower($2)
              AND id = $3
              AND status = 'pending'
            "#,
        )
        .bind(&tenant_id)
        .bind(admin_email)
        .bind(factor_id)
        .execute(&self.pool)
        .await?;

        Ok(updated.rows_affected() > 0)
    }

    pub async fn revoke_admin_auth_factor(
        &self,
        admin_email: &str,
        factor_id: Uuid,
    ) -> Result<bool> {
        let admin_email = normalize_email(admin_email);
        let tenant_id = self.tenant_id_for_admin_email(&admin_email).await?;
        let updated = sqlx::query(
            r#"
            UPDATE admin_auth_factors
            SET status = 'revoked'
            WHERE tenant_id = $1
              AND lower(admin_email) = lower($2)
              AND id = $3
              AND status IN ('pending', 'active')
            "#,
        )
        .bind(&tenant_id)
        .bind(admin_email)
        .bind(factor_id)
        .execute(&self.pool)
        .await?;

        Ok(updated.rows_affected() > 0)
    }

    pub async fn find_account_oidc_identity(
        &self,
        issuer_url: &str,
        subject: &str,
    ) -> Result<Option<String>> {
        let tenant_id = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM account_oidc_identities
            WHERE issuer_url = $1 AND subject = $2
            LIMIT 1
            "#,
        )
        .bind(issuer_url.trim())
        .bind(subject.trim())
        .fetch_optional(&self.pool)
        .await?;
        let Some(tenant_id) = tenant_id else {
            return Ok(None);
        };

        sqlx::query_scalar::<_, String>(
            r#"
            SELECT account_email
            FROM account_oidc_identities
            WHERE tenant_id = $1 AND issuer_url = $2 AND subject = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(issuer_url.trim())
        .bind(subject.trim())
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn upsert_account_oidc_identity(&self, claims: &AccountOidcClaims) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_email(&claims.email).await?;
        sqlx::query(
            r#"
            INSERT INTO account_oidc_identities (
                tenant_id, issuer_url, subject, account_email, created_at, last_login_at
            )
            VALUES ($1, $2, $3, $4, NOW(), NOW())
            ON CONFLICT (tenant_id, issuer_url, subject) DO UPDATE SET
                account_email = EXCLUDED.account_email,
                last_login_at = NOW()
            "#,
        )
        .bind(&tenant_id)
        .bind(claims.issuer_url.trim())
        .bind(claims.subject.trim())
        .bind(normalize_email(&claims.email))
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn create_account_auth_factor(&self, input: NewAccountAuthFactor) -> Result<Uuid> {
        let account_email = normalize_email(&input.account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let factor_id = Uuid::new_v4();

        sqlx::query(
            r#"
            INSERT INTO account_auth_factors (
                id, tenant_id, account_email, factor_type, status, secret_ciphertext
            )
            VALUES ($1, $2, $3, $4, 'pending', $5)
            "#,
        )
        .bind(factor_id)
        .bind(&tenant_id)
        .bind(account_email)
        .bind(input.factor_type.trim().to_lowercase())
        .bind(input.secret_ciphertext)
        .execute(&self.pool)
        .await?;

        Ok(factor_id)
    }

    pub async fn fetch_account_auth_factors(
        &self,
        account_email: &str,
    ) -> Result<Vec<AccountAuthFactor>> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let rows = sqlx::query_as::<_, AccountAuthFactorRow>(
            r#"
            SELECT
                id,
                factor_type,
                status,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                CASE
                    WHEN verified_at IS NULL THEN NULL
                    ELSE to_char(verified_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS verified_at,
                secret_ciphertext
            FROM account_auth_factors
            WHERE tenant_id = $1 AND lower(account_email) = lower($2)
            ORDER BY created_at DESC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| AccountAuthFactor {
                id: row.id,
                factor_type: row.factor_type,
                status: row.status,
                created_at: row.created_at,
                verified_at: row.verified_at,
            })
            .collect())
    }

    pub async fn fetch_account_totp_secret(
        &self,
        account_email: &str,
    ) -> Result<Option<(Uuid, String)>> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let row = sqlx::query_as::<_, AccountAuthFactorRow>(
            r#"
            SELECT
                id,
                factor_type,
                status,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                CASE
                    WHEN verified_at IS NULL THEN NULL
                    ELSE to_char(verified_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS verified_at,
                secret_ciphertext
            FROM account_auth_factors
            WHERE tenant_id = $1
              AND lower(account_email) = lower($2)
              AND factor_type = 'totp'
              AND status = 'active'
            ORDER BY verified_at DESC NULLS LAST, created_at DESC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.and_then(|row| row.secret_ciphertext.map(|secret| (row.id, secret))))
    }

    pub async fn fetch_pending_account_factor_secret(
        &self,
        account_email: &str,
        factor_id: Uuid,
    ) -> Result<Option<String>> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        sqlx::query_scalar(
            r#"
            SELECT secret_ciphertext
            FROM account_auth_factors
            WHERE tenant_id = $1
              AND lower(account_email) = lower($2)
              AND id = $3
              AND status = 'pending'
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .bind(factor_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn activate_account_auth_factor(
        &self,
        account_email: &str,
        factor_id: Uuid,
    ) -> Result<bool> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let updated = sqlx::query(
            r#"
            UPDATE account_auth_factors
            SET status = 'active', verified_at = NOW()
            WHERE tenant_id = $1
              AND lower(account_email) = lower($2)
              AND id = $3
              AND status = 'pending'
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .bind(factor_id)
        .execute(&self.pool)
        .await?;

        Ok(updated.rows_affected() > 0)
    }

    pub async fn revoke_account_auth_factor(
        &self,
        account_email: &str,
        factor_id: Uuid,
    ) -> Result<bool> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let updated = sqlx::query(
            r#"
            UPDATE account_auth_factors
            SET status = 'revoked'
            WHERE tenant_id = $1
              AND lower(account_email) = lower($2)
              AND id = $3
              AND status IN ('pending', 'active')
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .bind(factor_id)
        .execute(&self.pool)
        .await?;

        Ok(updated.rows_affected() > 0)
    }

    pub async fn create_account_app_password(
        &self,
        account_email: &str,
        label: &str,
        password_hash: &str,
    ) -> Result<Uuid> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO account_app_passwords (
                id, tenant_id, account_email, label, password_hash, status
            )
            VALUES ($1, $2, $3, $4, $5, 'active')
            "#,
        )
        .bind(id)
        .bind(&tenant_id)
        .bind(account_email)
        .bind(label.trim())
        .bind(password_hash.trim())
        .execute(&self.pool)
        .await?;

        Ok(id)
    }

    pub async fn list_account_app_passwords(
        &self,
        account_email: &str,
    ) -> Result<Vec<AccountAppPassword>> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let rows = sqlx::query_as::<_, AccountAppPasswordRow>(
            r#"
            SELECT
                id,
                label,
                status,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                CASE
                    WHEN last_used_at IS NULL THEN NULL
                    ELSE to_char(last_used_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS last_used_at,
                NULL AS password_hash
            FROM account_app_passwords
            WHERE tenant_id = $1 AND lower(account_email) = lower($2)
            ORDER BY created_at DESC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| AccountAppPassword {
                id: row.id,
                label: row.label,
                status: row.status,
                created_at: row.created_at,
                last_used_at: row.last_used_at,
            })
            .collect())
    }

    pub async fn fetch_active_account_app_passwords(
        &self,
        account_email: &str,
    ) -> Result<Vec<StoredAccountAppPassword>> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let rows = sqlx::query_as::<_, AccountAppPasswordRow>(
            r#"
            SELECT
                id,
                label,
                status,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                CASE
                    WHEN last_used_at IS NULL THEN NULL
                    ELSE to_char(last_used_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS last_used_at,
                password_hash
            FROM account_app_passwords
            WHERE tenant_id = $1
              AND lower(account_email) = lower($2)
              AND status = 'active'
            ORDER BY created_at DESC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .filter_map(|row| {
                row.password_hash
                    .map(|password_hash| StoredAccountAppPassword {
                        id: row.id,
                        password_hash,
                    })
            })
            .collect())
    }

    pub async fn touch_account_app_password(
        &self,
        account_email: &str,
        app_password_id: Uuid,
    ) -> Result<()> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        sqlx::query(
            r#"
            UPDATE account_app_passwords
            SET last_used_at = NOW()
            WHERE tenant_id = $1
              AND lower(account_email) = lower($2)
              AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .bind(app_password_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn revoke_account_app_password(
        &self,
        account_email: &str,
        app_password_id: Uuid,
    ) -> Result<bool> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let updated = sqlx::query(
            r#"
            UPDATE account_app_passwords
            SET status = 'disabled'
            WHERE tenant_id = $1
              AND lower(account_email) = lower($2)
              AND id = $3
              AND status = 'active'
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .bind(app_password_id)
        .execute(&self.pool)
        .await?;

        Ok(updated.rows_affected() > 0)
    }

    pub async fn upsert_account_credential(
        &self,
        input: AccountCredentialInput,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let email = normalize_email(&input.email);
        let tenant_id = self.tenant_id_for_account_email(&email).await?;
        if email.is_empty() || input.password_hash.trim().is_empty() {
            bail!("account credential email and password hash are required");
        }

        let account_exists = sqlx::query(
            r#"
            SELECT 1
            FROM accounts
            WHERE tenant_id = $1 AND lower(primary_email) = lower($2)
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(&email)
        .fetch_optional(&mut *tx)
        .await?;

        if account_exists.is_none() {
            bail!("account not found");
        }

        sqlx::query(
            r#"
            INSERT INTO account_credentials (account_email, tenant_id, password_hash, status)
            VALUES ($1, $2, $3, 'active')
            ON CONFLICT (tenant_id, account_email) DO UPDATE SET
                password_hash = EXCLUDED.password_hash,
                status = 'active',
                updated_at = NOW()
            "#,
        )
        .bind(email)
        .bind(&tenant_id)
        .bind(input.password_hash)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn has_admin_bootstrap_state(&self) -> Result<bool> {
        let credentials_exist = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM admin_credentials
                WHERE tenant_id = $1
            )
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_one(&self.pool)
        .await?;
        if credentials_exist {
            return Ok(true);
        }

        let administrators_exist = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM server_administrators
                WHERE tenant_id = $1
            )
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_one(&self.pool)
        .await?;

        Ok(administrators_exist)
    }

    pub async fn fetch_admin_login(&self, email: &str) -> Result<Option<AdminLogin>> {
        let email = normalize_email(email);
        let tenant_id = self.tenant_id_for_admin_email(&email).await?;
        let row = sqlx::query_as::<_, AdminLoginRow>(
            r#"
            SELECT
                ac.tenant_id,
                ac.email,
                ac.password_hash,
                ac.status,
                sa.display_name,
                sa.role,
                sa.domain_id,
                d.name AS domain_name,
                sa.rights_summary,
                sa.permissions_json
            FROM admin_credentials ac
            LEFT JOIN server_administrators sa
                ON sa.tenant_id = ac.tenant_id AND lower(sa.email) = lower(ac.email)
            LEFT JOIN domains d ON d.id = sa.domain_id
            WHERE ac.tenant_id = $1 AND lower(ac.email) = lower($2)
            ORDER BY sa.created_at ASC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| {
            let role = row.role.unwrap_or_else(|| "server-admin".to_string());
            let permissions = permissions_from_storage(
                &role,
                row.rights_summary.as_deref(),
                row.permissions_json.as_deref(),
            );
            AdminLogin {
                tenant_id: row.tenant_id,
                email: row.email,
                password_hash: row.password_hash,
                status: row.status,
                display_name: row
                    .display_name
                    .unwrap_or_else(|| "LPE Administrator".to_string()),
                role,
                domain_id: row.domain_id,
                domain_name: row.domain_name.unwrap_or_else(|| "All domains".to_string()),
                rights_summary: permission_summary(&permissions),
                permissions,
            }
        }))
    }

    pub async fn create_admin_session(
        &self,
        token: &str,
        tenant_id: &str,
        email: &str,
        session_timeout_minutes: u32,
        auth_method: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO admin_sessions (id, tenant_id, token, admin_email, auth_method, expires_at)
            VALUES ($1, $2, $3, $4, $5, NOW() + ($6::TEXT || ' minutes')::INTERVAL)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(token)
        .bind(normalize_email(email))
        .bind(normalize_admin_session_auth_method(auth_method))
        .bind(session_timeout_minutes.max(5) as i32)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn fetch_account_login(&self, email: &str) -> Result<Option<AccountLogin>> {
        let email = normalize_email(email);
        let tenant_id = self.tenant_id_for_account_email(&email).await?;
        let row = sqlx::query_as::<_, AccountLoginRow>(
            r#"
            SELECT
                a.tenant_id,
                a.id AS account_id,
                ac.account_email AS email,
                ac.password_hash,
                ac.status,
                a.display_name
            FROM account_credentials ac
            JOIN accounts a
              ON a.tenant_id = ac.tenant_id
             AND lower(a.primary_email) = lower(ac.account_email)
            WHERE ac.tenant_id = $1 AND lower(ac.account_email) = lower($2)
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| AccountLogin {
            tenant_id: row.tenant_id,
            account_id: row.account_id,
            email: row.email,
            password_hash: row.password_hash,
            status: row.status,
            display_name: row.display_name,
        }))
    }

    pub async fn create_account_session(
        &self,
        token: &str,
        tenant_id: &str,
        account_email: &str,
        session_timeout_minutes: u32,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO account_sessions (id, tenant_id, token, account_email, expires_at)
            VALUES ($1, $2, $3, $4, NOW() + ($5::TEXT || ' minutes')::INTERVAL)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(token)
        .bind(normalize_email(account_email))
        .bind(session_timeout_minutes.max(5) as i32)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn fetch_admin_session(&self, token: &str) -> Result<Option<AuthenticatedAdmin>> {
        let Some(tenant_id) = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM admin_sessions
            WHERE token = $1
            LIMIT 1
            "#,
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let row = sqlx::query_as::<_, AuthenticatedAdminRow>(
            r#"
            SELECT
                s.tenant_id,
                ac.email,
                sa.display_name,
                sa.role,
                sa.domain_id,
                d.name AS domain_name,
                sa.rights_summary,
                sa.permissions_json,
                s.auth_method,
                to_char(s.expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS expires_at
            FROM admin_sessions s
            JOIN admin_credentials ac
              ON ac.tenant_id = s.tenant_id
             AND ac.email = s.admin_email
            LEFT JOIN server_administrators sa
                ON sa.tenant_id = s.tenant_id AND lower(sa.email) = lower(s.admin_email)
            LEFT JOIN domains d ON d.id = sa.domain_id
            WHERE s.tenant_id = $1
              AND s.token = $2
              AND s.expires_at > NOW()
              AND ac.status = 'active'
            ORDER BY sa.created_at ASC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| {
            let role = row.role.unwrap_or_else(|| "server-admin".to_string());
            let permissions = permissions_from_storage(
                &role,
                row.rights_summary.as_deref(),
                row.permissions_json.as_deref(),
            );
            AuthenticatedAdmin {
                tenant_id: row.tenant_id,
                email: row.email,
                display_name: row
                    .display_name
                    .unwrap_or_else(|| "LPE Administrator".to_string()),
                role,
                domain_id: row.domain_id,
                domain_name: row.domain_name.unwrap_or_else(|| "All domains".to_string()),
                rights_summary: permission_summary(&permissions),
                permissions,
                auth_method: row.auth_method,
                expires_at: row.expires_at,
            }
        }))
    }

    pub async fn delete_admin_session(&self, token: &str) -> Result<()> {
        let tenant_id = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM admin_sessions
            WHERE token = $1
            LIMIT 1
            "#,
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or_else(|| PLATFORM_TENANT_ID.to_string());
        sqlx::query(
            r#"
            DELETE FROM admin_sessions
            WHERE tenant_id = $1 AND token = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(token)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn fetch_account_session(&self, token: &str) -> Result<Option<AuthenticatedAccount>> {
        let Some(tenant_id) = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM account_sessions
            WHERE token = $1
            LIMIT 1
            "#,
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let row = sqlx::query_as::<_, AuthenticatedAccountRow>(
            r#"
            SELECT
                s.tenant_id,
                a.id AS account_id,
                ac.account_email AS email,
                a.display_name,
                to_char(s.expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS expires_at
            FROM account_sessions s
            JOIN account_credentials ac
              ON ac.tenant_id = s.tenant_id
             AND ac.account_email = s.account_email
            JOIN accounts a
              ON a.tenant_id = s.tenant_id
             AND lower(a.primary_email) = lower(s.account_email)
            WHERE s.tenant_id = $1
              AND s.token = $2
              AND s.expires_at > NOW()
              AND ac.status = 'active'
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| AuthenticatedAccount {
            tenant_id: row.tenant_id,
            account_id: row.account_id,
            email: row.email,
            display_name: row.display_name,
            expires_at: row.expires_at,
        }))
    }

    pub async fn delete_account_session(&self, token: &str) -> Result<()> {
        let tenant_id = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM account_sessions
            WHERE token = $1
            LIMIT 1
            "#,
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or_else(|| PLATFORM_TENANT_ID.to_string());
        sqlx::query(
            r#"
            DELETE FROM account_sessions
            WHERE tenant_id = $1 AND token = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(token)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
