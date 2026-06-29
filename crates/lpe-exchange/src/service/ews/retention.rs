use super::super::*;

pub(in crate::service) fn get_user_retention_policy_tags_response(
    tags: &[EwsRetentionPolicyTag],
) -> String {
    let tags_xml = tags
        .iter()
        .map(retention_policy_tag_xml)
        .collect::<String>();
    format!(
        concat!(
            "<m:GetUserRetentionPolicyTagsResponse ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:RetentionPolicyTags>{tags_xml}</m:RetentionPolicyTags>",
            "</m:GetUserRetentionPolicyTagsResponse>"
        ),
        tags_xml = tags_xml,
    )
}

fn retention_policy_tag_xml(tag: &EwsRetentionPolicyTag) -> String {
    format!(
        concat!(
            "<t:RetentionPolicyTag>",
            "<t:DisplayName>{display_name}</t:DisplayName>",
            "<t:RetentionId>{id}</t:RetentionId>",
            "<t:RetentionPeriod>{retention_period}</t:RetentionPeriod>",
            "<t:Type>{tag_type}</t:Type>",
            "<t:RetentionAction>{action}</t:RetentionAction>",
            "<t:Description>{description}</t:Description>",
            "<t:IsVisible>{is_visible}</t:IsVisible>",
            "<t:OptedInto>{opted_into}</t:OptedInto>",
            "<t:IsArchive>{is_archive}</t:IsArchive>",
            "</t:RetentionPolicyTag>"
        ),
        display_name = escape_xml(&tag.display_name),
        id = tag.id,
        retention_period = tag.retention_days.unwrap_or(0),
        tag_type = ews_retention_tag_type(&tag.tag_type),
        action = ews_retention_action(&tag.action),
        description = escape_xml(&tag.description),
        is_visible = tag.is_visible,
        opted_into = tag.opted_into,
        is_archive = tag.action == "move_to_archive",
    )
}

fn ews_retention_tag_type(tag_type: &str) -> &'static str {
    match tag_type {
        "all" => "All",
        "inbox" => "Inbox",
        "sent" => "SentItems",
        "deleted_items" => "DeletedItems",
        "junk_email" => "JunkEmail",
        "custom_folder" | "personal" => "Personal",
        _ => "All",
    }
}

fn ews_retention_action(action: &str) -> &'static str {
    match action {
        "delete_and_allow_recovery" => "DeleteAndAllowRecovery",
        "permanently_delete" => "PermanentlyDelete",
        "move_to_archive" => "MoveToArchive",
        "none" => "None",
        _ => "None",
    }
}
