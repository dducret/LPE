use super::super::*;

pub(in crate::service) fn mail_app_operation_error_response(
    operation: &str,
    error: &anyhow::Error,
) -> String {
    let message = error.to_string();
    let code = if message.contains("not found") {
        "ErrorItemNotFound"
    } else if message.contains("access is not granted") {
        "ErrorAccessDenied"
    } else {
        "ErrorInvalidOperation"
    };
    operation_error_response(operation, code, &message)
}

pub(in crate::service) fn get_item_error_response(code: &str, message: &str) -> String {
    format!(
        concat!(
            "<m:GetItemResponse>",
            "<m:ResponseMessages>",
            "<m:GetItemResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>{code}</m:ResponseCode>",
            "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
            "<m:Items/>",
            "</m:GetItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetItemResponse>"
        ),
        code = escape_xml(code),
        message = escape_xml(message),
    )
}

pub(in crate::service) fn get_folder_error_response(code: &str, message: &str) -> String {
    format!(
        concat!(
            "<m:GetFolderResponse>",
            "<m:ResponseMessages>",
            "<m:GetFolderResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>{code}</m:ResponseCode>",
            "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
            "<m:Folders/>",
            "</m:GetFolderResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetFolderResponse>"
        ),
        code = escape_xml(code),
        message = escape_xml(message),
    )
}

pub(in crate::service) fn get_user_availability_error_response(message: &str) -> String {
    format!(
        concat!(
            "<m:GetUserAvailabilityResponse>",
            "<m:FreeBusyResponseArray>",
            "<m:FreeBusyResponse>",
            "<m:ResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>ErrorFreeBusyGenerationFailed</m:ResponseCode>",
            "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
            "</m:ResponseMessage>",
            "</m:FreeBusyResponse>",
            "</m:FreeBusyResponseArray>",
            "</m:GetUserAvailabilityResponse>"
        ),
        message = escape_xml(message),
    )
}

pub(in crate::service) fn set_user_oof_settings_error_response(
    code: &str,
    message: &str,
) -> String {
    format!(
        concat!(
            "<m:SetUserOofSettingsResponse>",
            "<m:ResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>{code}</m:ResponseCode>",
            "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
            "</m:ResponseMessage>",
            "</m:SetUserOofSettingsResponse>"
        ),
        code = escape_xml(code),
        message = escape_xml(message),
    )
}

pub(in crate::service) fn ews_error_code_or(
    error: &anyhow::Error,
    fallback: &'static str,
) -> &'static str {
    if error.to_string().contains("access is not granted") {
        "ErrorAccessDenied"
    } else {
        fallback
    }
}

pub(in crate::service) fn operation_error_response(
    operation: &str,
    code: &str,
    message: &str,
) -> String {
    format!(
        concat!(
            "<m:{operation}Response>",
            "<m:ResponseMessages>",
            "<m:{operation}ResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>{code}</m:ResponseCode>",
            "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
            "</m:{operation}ResponseMessage>",
            "</m:ResponseMessages>",
            "</m:{operation}Response>"
        ),
        operation = escape_xml(operation),
        code = escape_xml(code),
        message = escape_xml(message),
    )
}

pub(in crate::service) fn update_item_success_response(items: String) -> String {
    format!(
        concat!(
            "<m:UpdateItemResponse>",
            "<m:ResponseMessages>",
            "<m:UpdateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>{items}</m:Items>",
            "</m:UpdateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:UpdateItemResponse>"
        ),
        items = items,
    )
}

pub(in crate::service) fn delete_item_success_response() -> String {
    concat!(
        "<m:DeleteItemResponse>",
        "<m:ResponseMessages>",
        "<m:DeleteItemResponseMessage ResponseClass=\"Success\">",
        "<m:ResponseCode>NoError</m:ResponseCode>",
        "</m:DeleteItemResponseMessage>",
        "</m:ResponseMessages>",
        "</m:DeleteItemResponse>"
    )
    .to_string()
}

pub(in crate::service) fn move_item_success_response(items: String) -> String {
    format!(
        concat!(
            "<m:MoveItemResponse>",
            "<m:ResponseMessages>",
            "<m:MoveItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>{items}</m:Items>",
            "</m:MoveItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:MoveItemResponse>"
        ),
        items = items,
    )
}

pub(in crate::service) fn archive_item_success_response(items: String) -> String {
    format!(
        concat!(
            "<m:ArchiveItemResponse>",
            "<m:ResponseMessages>",
            "<m:ArchiveItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>{items}</m:Items>",
            "</m:ArchiveItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:ArchiveItemResponse>"
        ),
        items = items,
    )
}

pub(in crate::service) fn copy_item_success_response(items: String) -> String {
    format!(
        concat!(
            "<m:CopyItemResponse>",
            "<m:ResponseMessages>",
            "<m:CopyItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>{items}</m:Items>",
            "</m:CopyItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CopyItemResponse>"
        ),
        items = items,
    )
}

pub(in crate::service) fn simple_operation_success_response(operation: &str) -> String {
    format!(
        concat!(
            "<m:{operation}Response>",
            "<m:ResponseMessages>",
            "<m:{operation}ResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "</m:{operation}ResponseMessage>",
            "</m:ResponseMessages>",
            "</m:{operation}Response>"
        ),
        operation = operation
    )
}

pub(in crate::service) fn mark_as_junk_success_response(moved_item_ids: String) -> String {
    format!(
        concat!(
            "<m:MarkAsJunkResponse>",
            "<m:ResponseMessages>",
            "<m:MarkAsJunkResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "{moved_item_ids}",
            "</m:MarkAsJunkResponseMessage>",
            "</m:ResponseMessages>",
            "</m:MarkAsJunkResponse>"
        ),
        moved_item_ids = moved_item_ids,
    )
}

pub(in crate::service) fn find_item_response(items: String) -> String {
    format!(
        concat!(
            "<m:FindItemResponse>",
            "<m:ResponseMessages>",
            "<m:FindItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:RootFolder TotalItemsInView=\"{count}\" IncludesLastItemInRange=\"true\">",
            "<t:Items>{items}</t:Items>",
            "</m:RootFolder>",
            "</m:FindItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:FindItemResponse>"
        ),
        items = items,
        count = count_tag_occurrences(&items, "<t:ItemId")
    )
}

pub(in crate::service) fn operation_response_message(
    operation: &str,
    code: &str,
    message: &str,
) -> String {
    format!(
        concat!(
            "<m:{operation}ResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>{code}</m:ResponseCode>",
            "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
            "</m:{operation}ResponseMessage>"
        ),
        operation = escape_xml(operation),
        code = escape_xml(code),
        message = escape_xml(message),
    )
}

pub(in crate::service) fn sync_folder_items_response(sync_state: &str, changes: String) -> String {
    format!(
        concat!(
            "<m:SyncFolderItemsResponse>",
            "<m:ResponseMessages>",
            "<m:SyncFolderItemsResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:SyncState>{sync_state}</m:SyncState>",
            "<m:IncludesLastItemInRange>true</m:IncludesLastItemInRange>",
            "<m:Changes>{changes}</m:Changes>",
            "</m:SyncFolderItemsResponseMessage>",
            "</m:ResponseMessages>",
            "</m:SyncFolderItemsResponse>"
        ),
        sync_state = escape_xml(sync_state),
        changes = changes,
    )
}

pub(in crate::service) fn unsupported_operation_response(operation: &str) -> String {
    operation_error_response(
        operation,
        "ErrorInvalidOperation",
        &format!("{operation} is not implemented by the EWS MVP."),
    )
}

pub(in crate::service) fn get_user_photo_error_response(code: &str, message: &str) -> String {
    format!(
        concat!(
            "<m:GetUserPhotoResponse ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>{code}</m:ResponseCode>",
            "<m:HasChanged>false</m:HasChanged>",
            "</m:GetUserPhotoResponse>"
        ),
        code = escape_xml(code),
        message = escape_xml(message),
    )
}

pub(in crate::service) fn get_password_expiration_date_error_response(
    code: &str,
    message: &str,
) -> String {
    format!(
        concat!(
            "<m:GetPasswordExpirationDateResponse ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>{code}</m:ResponseCode>",
            "</m:GetPasswordExpirationDateResponse>"
        ),
        code = escape_xml(code),
        message = escape_xml(message),
    )
}
