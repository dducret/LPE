use super::super::*;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn add_delegate(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        if let Err(error) = validate_delegate_mailbox_owner(principal, request) {
            return Ok(operation_error_response(
                "AddDelegate",
                "ErrorInvalidOperation",
                &error.to_string(),
            ));
        }
        let delivery = match parse_delegate_meeting_delivery(request) {
            Ok(delivery) => delivery,
            Err(error) => {
                return Ok(operation_error_response(
                    "AddDelegate",
                    "ErrorInvalidOperation",
                    &error.to_string(),
                ))
            }
        };
        let users = match parse_ews_delegate_users(principal, request, &delivery) {
            Ok(users) => users,
            Err(error) => {
                return Ok(operation_error_response(
                    "AddDelegate",
                    "ErrorInvalidOperation",
                    &error.to_string(),
                ))
            }
        };
        self.mutate_ews_delegates("AddDelegate", principal, users, true)
            .await
    }

    pub(in crate::service) async fn update_delegate(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        if let Err(error) = validate_delegate_mailbox_owner(principal, request) {
            return Ok(operation_error_response(
                "UpdateDelegate",
                "ErrorInvalidOperation",
                &error.to_string(),
            ));
        }
        let delivery = match parse_delegate_meeting_delivery(request) {
            Ok(delivery) => delivery,
            Err(error) => {
                return Ok(operation_error_response(
                    "UpdateDelegate",
                    "ErrorInvalidOperation",
                    &error.to_string(),
                ))
            }
        };
        let users = match parse_ews_delegate_users(principal, request, &delivery) {
            Ok(users) => users,
            Err(error) => {
                return Ok(operation_error_response(
                    "UpdateDelegate",
                    "ErrorInvalidOperation",
                    &error.to_string(),
                ))
            }
        };
        self.mutate_ews_delegates("UpdateDelegate", principal, users, true)
            .await
    }

    pub(in crate::service) async fn get_delegate(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        if let Err(error) = validate_delegate_mailbox_owner(principal, request) {
            return Ok(operation_error_response(
                "GetDelegate",
                "ErrorInvalidOperation",
                &error.to_string(),
            ));
        }
        let requested_emails = match parse_delegate_user_id_emails(request) {
            Ok(emails) => emails,
            Err(error) => {
                return Ok(operation_error_response(
                    "GetDelegate",
                    "ErrorInvalidOperation",
                    &error.to_string(),
                ))
            }
        };
        let requested = requested_emails
            .iter()
            .map(|email| email.to_ascii_lowercase())
            .collect::<HashSet<_>>();
        let delegates = self
            .store
            .fetch_ews_delegates(principal.account_id)
            .await?
            .into_iter()
            .filter(|delegate| {
                requested.is_empty()
                    || requested.contains(&delegate.grantee_email.to_ascii_lowercase())
            })
            .collect::<Vec<_>>();
        Ok(get_delegate_response(&delegates))
    }

    pub(in crate::service) async fn remove_delegate(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        if let Err(error) = validate_delegate_mailbox_owner(principal, request) {
            return Ok(operation_error_response(
                "RemoveDelegate",
                "ErrorInvalidOperation",
                &error.to_string(),
            ));
        }
        let emails = match parse_delegate_user_id_emails(request) {
            Ok(emails) => emails,
            Err(error) => {
                return Ok(operation_error_response(
                    "RemoveDelegate",
                    "ErrorInvalidOperation",
                    &error.to_string(),
                ))
            }
        };
        let delegates = self.store.fetch_ews_delegates(principal.account_id).await?;
        let mut ids = Vec::with_capacity(emails.len());
        for email in &emails {
            let Some(delegate) = delegates
                .iter()
                .find(|delegate| delegate.grantee_email.eq_ignore_ascii_case(email))
            else {
                return Ok(operation_error_response(
                    "RemoveDelegate",
                    "ErrorItemNotFound",
                    "Delegate was not found.",
                ));
            };
            ids.push(delegate.grantee_account_id);
        }
        if let Err(error) = self
            .store
            .remove_ews_delegate_batch(
                principal.account_id,
                &ids,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-remove-delegate".to_string(),
                    subject: emails.join(","),
                },
            )
            .await
        {
            return Ok(operation_error_response(
                "RemoveDelegate",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            ));
        }
        let response_messages = ids
            .iter()
            .map(|_| "<m:DelegateUserResponseMessageType ResponseClass=\"Success\"><m:ResponseCode>NoError</m:ResponseCode></m:DelegateUserResponseMessageType>")
            .collect::<String>();
        Ok(delegate_operation_response(
            "RemoveDelegate",
            &response_messages,
        ))
    }

    async fn mutate_ews_delegates(
        &self,
        operation: &str,
        principal: &AccountPrincipal,
        users: Vec<UpsertEwsDelegateInput>,
        include_delegate: bool,
    ) -> Result<String> {
        let subject = users
            .iter()
            .map(|user| user.grantee_email.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let delegates = match self
            .store
            .apply_ews_delegate_batch(
                &users,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: format!("ews-{}", operation.to_ascii_lowercase()),
                    subject,
                },
            )
            .await
        {
            Ok(delegates) => delegates,
            Err(error) => {
                return Ok(operation_error_response(
                    operation,
                    ews_error_code_or(&error, "ErrorInvalidOperation"),
                    &error.to_string(),
                ))
            }
        };
        let response_messages = delegates
            .iter()
            .map(|delegate| delegate_success_response_message(delegate, include_delegate))
            .collect::<String>();
        Ok(delegate_operation_response(operation, &response_messages))
    }
}

pub(in crate::service) fn delegate_operation_response(
    operation: &str,
    response_messages: &str,
) -> String {
    format!(
        concat!(
            "<m:{operation}Response>",
            "<m:ResponseMessages>{response_messages}</m:ResponseMessages>",
            "</m:{operation}Response>"
        ),
        operation = operation,
        response_messages = response_messages,
    )
}

pub(in crate::service) fn get_delegate_response(delegates: &[EwsDelegate]) -> String {
    let response_messages = delegates
        .iter()
        .map(|delegate| delegate_success_response_message(delegate, true))
        .collect::<String>();
    delegate_operation_response("GetDelegate", &response_messages)
}

pub(in crate::service) fn delegate_success_response_message(
    delegate: &EwsDelegate,
    include_delegate: bool,
) -> String {
    let delegate_xml = include_delegate
        .then(|| ews_delegate_user_xml(delegate))
        .unwrap_or_default();
    format!(
        concat!(
            "<m:DelegateUserResponseMessageType ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "{delegate_xml}",
            "</m:DelegateUserResponseMessageType>"
        ),
        delegate_xml = delegate_xml,
    )
}

fn ews_delegate_user_xml(delegate: &EwsDelegate) -> String {
    format!(
        concat!(
            "<m:DelegateUser>",
            "<t:UserId>",
            "<t:SID>{grantee_account_id}</t:SID>",
            "<t:PrimarySmtpAddress>{email}</t:PrimarySmtpAddress>",
            "<t:DisplayName>{display_name}</t:DisplayName>",
            "</t:UserId>",
            "<t:DelegatePermissions>",
            "<t:CalendarFolderPermissionLevel>{calendar_level}</t:CalendarFolderPermissionLevel>",
            "<t:InboxFolderPermissionLevel>{inbox_level}</t:InboxFolderPermissionLevel>",
            "</t:DelegatePermissions>",
            "<t:ReceiveCopiesOfMeetingMessages>{receives_copies}</t:ReceiveCopiesOfMeetingMessages>",
            "<t:ViewPrivateItems>{view_private}</t:ViewPrivateItems>",
            "</m:DelegateUser>"
        ),
        grantee_account_id = delegate.grantee_account_id,
        email = escape_xml(&delegate.grantee_email),
        display_name = escape_xml(&delegate.grantee_display_name),
        calendar_level = ews_delegate_permission_level(&delegate.calendar_rights),
        inbox_level = ews_delegate_permission_level(&delegate.inbox_rights),
        receives_copies = delegate.preferences.receives_meeting_request_copy,
        view_private = delegate.preferences.may_view_private_items,
    )
}

fn ews_delegate_permission_level(rights: &CollaborationRights) -> &'static str {
    if !rights.may_read {
        "None"
    } else if rights.may_write || rights.may_delete || rights.may_share {
        "Editor"
    } else {
        "Reviewer"
    }
}

pub(in crate::service) fn validate_delegate_mailbox_owner(
    principal: &AccountPrincipal,
    request: &str,
) -> Result<()> {
    let mailboxes = element_contents(request, "Mailbox");
    if mailboxes.is_empty() {
        return Ok(());
    }
    if mailboxes.len() != 1 {
        bail!("delegate operation accepts exactly one Mailbox");
    }
    let mailbox = mailboxes[0];
    let Some(email) = element_text(mailbox, "EmailAddress")
        .or_else(|| element_text(mailbox, "PrimarySmtpAddress"))
    else {
        bail!("Mailbox EmailAddress is required");
    };
    if email.eq_ignore_ascii_case(&principal.email) {
        Ok(())
    } else {
        bail!("EWS delegate operations are owner-bound to the authenticated mailbox")
    }
}

pub(in crate::service) fn parse_delegate_meeting_delivery(
    request: &str,
) -> Result<EwsDelegatePreferences> {
    let mut preferences = EwsDelegatePreferences::default();
    if let Some(value) = element_text(request, "DeliverMeetingRequests") {
        preferences.meeting_request_delivery = match value.trim() {
            "DelegatesOnly" => "delegate_only",
            "DelegatesAndMe" | "DelegatesAndSendInformationToMe" => "delegate_and_owner",
            other => bail!("unsupported DeliverMeetingRequests value {other}"),
        }
        .to_string();
    }
    Ok(preferences)
}

pub(in crate::service) fn parse_ews_delegate_users(
    principal: &AccountPrincipal,
    request: &str,
    delivery: &EwsDelegatePreferences,
) -> Result<Vec<UpsertEwsDelegateInput>> {
    let mut users = Vec::new();
    for user in element_contents(request, "DelegateUser") {
        users.push(parse_ews_delegate_user(principal, user, delivery)?);
    }
    if users.is_empty() {
        bail!("DelegateUsers must contain at least one DelegateUser");
    }
    if users.len() > 100 {
        bail!("DelegateUsers exceeds the supported maximum of 100 delegates");
    }
    let mut seen = HashSet::new();
    for user in &users {
        if !seen.insert(user.grantee_email.clone()) {
            bail!("DelegateUsers contains a duplicate delegate");
        }
    }
    Ok(users)
}

fn parse_ews_delegate_user(
    principal: &AccountPrincipal,
    user: &str,
    delivery: &EwsDelegatePreferences,
) -> Result<UpsertEwsDelegateInput> {
    let user_id = element_content(user, "UserId").unwrap_or(user);
    let grantee_email = element_text(user_id, "PrimarySmtpAddress")
        .or_else(|| element_text(user_id, "EmailAddress"))
        .map(|email| normalization::normalize_trimmed_lowercase(&email))
        .filter(|email| !email.is_empty())
        .ok_or_else(|| anyhow!("Delegate UserId PrimarySmtpAddress is required"))?;
    if grantee_email.eq_ignore_ascii_case(&principal.email) {
        bail!("self-delegation is not supported");
    }
    let permissions = element_content(user, "DelegatePermissions").unwrap_or("");
    reject_unsupported_delegate_permissions(permissions)?;
    let inbox_rights = parse_delegate_permission_level(permissions, "InboxFolderPermissionLevel")?;
    let calendar_rights =
        parse_delegate_permission_level(permissions, "CalendarFolderPermissionLevel")?;
    if !inbox_rights.may_read && !calendar_rights.may_read {
        bail!("at least one Inbox or Calendar delegate permission is required");
    }
    let mut preferences = delivery.clone();
    if let Some(value) = element_text(user, "ReceiveCopiesOfMeetingMessages") {
        preferences.receives_meeting_request_copy = parse_xml_bool(&value)?;
    }
    if let Some(value) = element_text(user, "ViewPrivateItems") {
        preferences.may_view_private_items = parse_xml_bool(&value)?;
    }
    Ok(UpsertEwsDelegateInput {
        owner_account_id: principal.account_id,
        grantee_email,
        inbox_rights,
        calendar_rights,
        may_send_on_behalf: true,
        preferences,
    })
}

fn parse_delegate_permission_level(permissions: &str, field: &str) -> Result<CollaborationRights> {
    let level = element_text(permissions, field).unwrap_or_else(|| "None".to_string());
    match level.trim() {
        "" | "None" => Ok(collaboration_rights(false, false, false, false)),
        "Reviewer" => Ok(collaboration_rights(true, false, false, false)),
        "Author" | "Editor" => Ok(collaboration_rights(true, true, true, false)),
        "Custom" => bail!("{field} Custom is Exchange-only and cannot map to canonical rights"),
        other => bail!("unsupported {field} value {other}"),
    }
}

fn reject_unsupported_delegate_permissions(permissions: &str) -> Result<()> {
    for field in [
        "ContactsFolderPermissionLevel",
        "TasksFolderPermissionLevel",
        "NotesFolderPermissionLevel",
        "JournalFolderPermissionLevel",
    ] {
        let Some(level) = element_text(permissions, field) else {
            continue;
        };
        if !matches!(level.trim(), "" | "None") {
            bail!("{field} is not supported by the bounded LPE EWS delegate adapter");
        }
    }
    Ok(())
}

fn collaboration_rights(
    may_read: bool,
    may_write: bool,
    may_delete: bool,
    may_share: bool,
) -> CollaborationRights {
    CollaborationRights {
        may_read,
        may_write,
        may_delete,
        may_share,
    }
}

pub(in crate::service) fn parse_delegate_user_id_emails(request: &str) -> Result<Vec<String>> {
    let user_ids = element_contents(request, "UserId");
    if user_ids.is_empty() || user_ids.len() > 100 {
        bail!("UserIds must contain between one and 100 UserId values");
    }
    let mut emails = Vec::with_capacity(user_ids.len());
    let mut seen = HashSet::new();
    for user_id in user_ids {
        let email = element_text(user_id, "PrimarySmtpAddress")
            .or_else(|| element_text(user_id, "EmailAddress"))
            .map(|email| normalization::normalize_trimmed_lowercase(&email))
            .filter(|email| !email.is_empty())
            .ok_or_else(|| anyhow!("UserId PrimarySmtpAddress is required"))?;
        if !seen.insert(email.clone()) {
            bail!("UserIds contains a duplicate delegate");
        }
        emails.push(email);
    }
    Ok(emails)
}
