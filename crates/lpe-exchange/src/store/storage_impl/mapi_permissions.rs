macro_rules! store_impl_mapi_permissions {
    () => {
        fn fetch_mapi_folder_permissions<'a>(
            &'a self,
            account_id: Uuid,
            mailbox_ids: &'a [Uuid],
        ) -> StoreFuture<'a, Vec<MapiFolderPermission>> {
            Box::pin(async move {
                let row = sqlx::query(
                    r#"
                    SELECT tenant_id, primary_email, display_name
                    FROM accounts
                    WHERE id = $1
                    LIMIT 1
                    "#,
                )
                .bind(account_id)
                .fetch_optional(self.pool())
                .await?
                .ok_or_else(|| anyhow::anyhow!("account not found"))?;
                let principal = lpe_mail_auth::AccountPrincipal {
                    tenant_id: row.get("tenant_id"),
                    account_id,
                    email: row.get("primary_email"),
                    display_name: row.get("display_name"),
                    quota_mb: None,
                    quota_used_octets: None,
                };
                let mut permissions = mailbox_ids
                    .iter()
                    .copied()
                    .map(|mailbox_id| owner_permission(mailbox_id, &principal))
                    .collect::<Vec<_>>();
                if mailbox_ids.is_empty() {
                    return Ok(permissions);
                }

                let rows = sqlx::query(
                    r#"
                    SELECT
                        g.mailbox_id,
                        g.grantee_account_id,
                        grantee.display_name,
                        g.may_read,
                        g.may_write,
                        g.may_delete,
                        g.may_share
                    FROM mailbox_delegation_grants g
                    JOIN accounts grantee
                      ON grantee.tenant_id = g.tenant_id
                     AND grantee.id = g.grantee_account_id
                    WHERE g.tenant_id = $1
                      AND g.mailbox_id = ANY($2)
                    ORDER BY lower(grantee.primary_email) ASC
                    "#,
                )
                .bind(principal.tenant_id)
                .bind(mailbox_ids)
                .fetch_all(self.pool())
                .await?;

                permissions.extend(rows.into_iter().map(|row| MapiFolderPermission {
                    mailbox_id: row.get("mailbox_id"),
                    member_account_id: Some(row.get("grantee_account_id")),
                    member_name: row.get("display_name"),
                    rights: rights_from_grant(
                        row.get("may_read"),
                        row.get("may_write"),
                        row.get("may_delete"),
                        row.get("may_share"),
                    ),
                }));
                Ok(permissions)
            })
        }

        fn set_mapi_folder_permission<'a>(
            &'a self,
            owner_account_id: Uuid,
            mailbox_id: Uuid,
            grantee_account_id: Uuid,
            may_read: bool,
            may_write: bool,
            may_delete: bool,
            may_share: bool,
            audit: AuditEntryInput,
        ) -> StoreFuture<'a, ()> {
            Box::pin(async move {
                self.set_mailbox_folder_delegation_grant(
                    MailboxFolderDelegationGrantInput {
                        owner_account_id,
                        mailbox_id,
                        grantee_account_id,
                        may_read,
                        may_write,
                        may_delete,
                        may_share,
                    },
                    audit,
                )
                .await
            })
        }

        fn set_mapi_calendar_permission<'a>(
            &'a self,
            owner_account_id: Uuid,
            grantee_account_id: Uuid,
            may_read: bool,
            may_write: bool,
            may_delete: bool,
            may_share: bool,
            audit: AuditEntryInput,
        ) -> StoreFuture<'a, ()> {
            Box::pin(async move {
                if !may_read {
                    return self
                        .delete_collaboration_grant(
                            owner_account_id,
                            CollaborationResourceKind::Calendar,
                            grantee_account_id,
                            audit,
                        )
                        .await;
                }
                let tenant_id = mapi_tenant_id_for_account(self, owner_account_id).await?;
                let grantee_email = sqlx::query_scalar::<_, String>(
                    r#"
                    SELECT primary_email
                    FROM accounts
                    WHERE tenant_id = $1
                      AND id = $2
                    LIMIT 1
                    "#,
                )
                .bind(tenant_id)
                .bind(grantee_account_id)
                .fetch_optional(self.pool())
                .await?
                .ok_or_else(|| anyhow::anyhow!("calendar permission grantee account not found"))?;
                self.upsert_collaboration_grant(
                    CollaborationGrantInput {
                        kind: CollaborationResourceKind::Calendar,
                        owner_account_id,
                        grantee_email,
                        calendar_id: None,
                        may_read,
                        may_write,
                        may_delete,
                        may_share,
                    },
                    audit,
                )
                .await
                .map(|_| ())
            })
        }

        fn set_mapi_calendar_collection_permission<'a>(
            &'a self,
            owner_account_id: Uuid,
            calendar_collection_id: &'a str,
            grantee_account_id: Uuid,
            may_read: bool,
            may_write: bool,
            may_delete: bool,
            may_share: bool,
            audit: AuditEntryInput,
        ) -> StoreFuture<'a, ()> {
            Box::pin(async move {
                self.set_calendar_collection_grant(
                    owner_account_id,
                    calendar_collection_id,
                    grantee_account_id,
                    may_read,
                    may_write,
                    may_delete,
                    may_share,
                    audit,
                )
                .await
            })
        }
    };
}
