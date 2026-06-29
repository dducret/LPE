use super::super::*;

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

pub(in crate::service) fn delegate_error_response_message(code: &str, message: &str) -> String {
    format!(
        concat!(
            "<m:DelegateUserResponseMessageType ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>{code}</m:ResponseCode>",
            "</m:DelegateUserResponseMessageType>"
        ),
        code = escape_xml(code),
        message = escape_xml(message),
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
