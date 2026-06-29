use super::super::*;

pub(in crate::service) fn get_discovery_search_configuration_response(
    searches: &[EwsDiscoverySearchConfig],
) -> String {
    let searches_xml = searches
        .iter()
        .map(|search| {
            format!(
                concat!(
                    "<t:DiscoverySearchConfiguration>",
                    "<t:SearchId>{id}</t:SearchId>",
                    "<t:SearchName>{name}</t:SearchName>",
                    "<t:SearchQuery>{query}</t:SearchQuery>",
                    "<t:LastModifiedTime>{updated_at}</t:LastModifiedTime>",
                    "</t:DiscoverySearchConfiguration>"
                ),
                id = search.id,
                name = escape_xml(&search.display_name),
                query = escape_xml(&search.query_text),
                updated_at = escape_xml(&search.updated_at),
            )
        })
        .collect::<String>();
    format!(
        concat!(
            "<m:GetDiscoverySearchConfigurationResponse>",
            "<m:ResponseMessages>",
            "<m:GetDiscoverySearchConfigurationResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:DiscoverySearchConfigurations>{searches_xml}</m:DiscoverySearchConfigurations>",
            "</m:GetDiscoverySearchConfigurationResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetDiscoverySearchConfigurationResponse>"
        ),
        searches_xml = searches_xml,
    )
}

pub(in crate::service) fn get_searchable_mailboxes_response(
    mailboxes: &[EwsSearchableMailbox],
) -> String {
    let mailboxes_xml = mailboxes
        .iter()
        .map(|mailbox| {
            format!(
                concat!(
                    "<t:SearchableMailbox>",
                    "<t:Guid>{id}</t:Guid>",
                    "<t:PrimarySmtpAddress>{email}</t:PrimarySmtpAddress>",
                    "<t:DisplayName>{display_name}</t:DisplayName>",
                    "<t:IsExternalMailbox>false</t:IsExternalMailbox>",
                    "<t:ExternalEmailAddress/>",
                    "<t:IsMembershipGroup>false</t:IsMembershipGroup>",
                    "<t:ReferenceId>{id}</t:ReferenceId>",
                    "<t:LitigationHoldEnabled>{hold}</t:LitigationHoldEnabled>",
                    "</t:SearchableMailbox>"
                ),
                id = mailbox.account_id,
                email = escape_xml(&mailbox.email),
                display_name = escape_xml(&mailbox.display_name),
                hold = mailbox.litigation_hold_enabled,
            )
        })
        .collect::<String>();
    format!(
        concat!(
            "<m:GetSearchableMailboxesResponse>",
            "<m:ResponseMessages>",
            "<m:GetSearchableMailboxesResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:SearchableMailboxes>{mailboxes_xml}</m:SearchableMailboxes>",
            "</m:GetSearchableMailboxesResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetSearchableMailboxesResponse>"
        ),
        mailboxes_xml = mailboxes_xml,
    )
}

pub(in crate::service) fn search_mailboxes_response(result: &EwsDiscoverySearchResult) -> String {
    let items_xml = result
        .items
        .iter()
        .map(|item| {
            format!(
                concat!(
                    "<t:SearchResultItem>",
                    "<t:Id>{id}</t:Id>",
                    "<t:MailboxGuid>{account_id}</t:MailboxGuid>",
                    "<t:ItemId Id=\"message:{message_id}\"/>",
                    "<t:Subject>{subject}</t:Subject>",
                    "<t:Preview>{preview}</t:Preview>",
                    "<t:Rank>{rank}</t:Rank>",
                    "</t:SearchResultItem>"
                ),
                id = item.id,
                account_id = item.account_id,
                message_id = item.message_id,
                subject = escape_xml(&item.subject),
                preview = escape_xml(&item.preview),
                rank = item.rank,
            )
        })
        .collect::<String>();
    format!(
        concat!(
            "<m:SearchMailboxesResponse>",
            "<m:ResponseMessages>",
            "<m:SearchMailboxesResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:SearchId>{search_id}</m:SearchId>",
            "<m:JobId>{job_id}</m:JobId>",
            "<m:SearchQuery>{query}</m:SearchQuery>",
            "<m:ResultCount>{count}</m:ResultCount>",
            "<m:SearchResult>{items_xml}</m:SearchResult>",
            "</m:SearchMailboxesResponseMessage>",
            "</m:ResponseMessages>",
            "</m:SearchMailboxesResponse>"
        ),
        search_id = result.search_id,
        job_id = result.job_id,
        query = escape_xml(&result.query_text),
        count = result.result_count,
        items_xml = items_xml,
    )
}

pub(in crate::service) fn get_hold_on_mailboxes_response(holds: &[EwsHoldMailbox]) -> String {
    let holds_xml = holds.iter().map(hold_mailbox_xml).collect::<String>();
    format!(
        concat!(
            "<m:GetHoldOnMailboxesResponse>",
            "<m:ResponseMessages>",
            "<m:GetHoldOnMailboxesResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:MailboxHoldResult>{holds_xml}</m:MailboxHoldResult>",
            "</m:GetHoldOnMailboxesResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetHoldOnMailboxesResponse>"
        ),
        holds_xml = holds_xml,
    )
}

pub(in crate::service) fn set_hold_on_mailboxes_response(
    holds: &[EwsHoldMailbox],
    enabled: bool,
) -> String {
    let holds_xml = holds.iter().map(hold_mailbox_xml).collect::<String>();
    format!(
        concat!(
            "<m:SetHoldOnMailboxesResponse>",
            "<m:ResponseMessages>",
            "<m:SetHoldOnMailboxesResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Action>{action}</m:Action>",
            "<m:MailboxHoldResult>{holds_xml}</m:MailboxHoldResult>",
            "</m:SetHoldOnMailboxesResponseMessage>",
            "</m:ResponseMessages>",
            "</m:SetHoldOnMailboxesResponse>"
        ),
        action = if enabled { "CreateHold" } else { "ReleaseHold" },
        holds_xml = holds_xml,
    )
}

pub(in crate::service) fn discovery_query_text(request: &str) -> String {
    element_text(request, "Query")
        .or_else(|| element_text(request, "SearchQuery"))
        .or_else(|| element_text(request, "QueryString"))
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn hold_mailbox_xml(hold: &EwsHoldMailbox) -> String {
    format!(
        concat!(
            "<t:MailboxHoldStatus>",
            "<t:Mailbox>{email}</t:Mailbox>",
            "<t:DisplayName>{display_name}</t:DisplayName>",
            "<t:HoldId>{hold_id}</t:HoldId>",
            "<t:HoldName>{hold_name}</t:HoldName>",
            "<t:Query>{query}</t:Query>",
            "<t:IsOnHold>{active}</t:IsOnHold>",
            "</t:MailboxHoldStatus>"
        ),
        email = escape_xml(&hold.email),
        display_name = escape_xml(&hold.display_name),
        hold_id = hold.hold_id.map(|id| id.to_string()).unwrap_or_default(),
        hold_name = escape_xml(hold.hold_name.as_deref().unwrap_or_default()),
        query = escape_xml(hold.query_text.as_deref().unwrap_or_default()),
        active = hold.active,
    )
}

pub(in crate::service) fn get_non_indexable_item_details_response(
    reports: &[EwsNonIndexableReport],
) -> String {
    let reports_xml = reports
        .iter()
        .map(non_indexable_report_xml)
        .collect::<String>();
    format!(
        concat!(
            "<m:GetNonIndexableItemDetailsResponse>",
            "<m:ResponseMessages>",
            "<m:GetNonIndexableItemDetailsResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:NonIndexableItemDetails>{reports_xml}</m:NonIndexableItemDetails>",
            "</m:GetNonIndexableItemDetailsResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetNonIndexableItemDetailsResponse>"
        ),
        reports_xml = reports_xml,
    )
}

pub(in crate::service) fn get_non_indexable_item_statistics_response(
    reports: &[EwsNonIndexableReport],
) -> String {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for report in reports {
        *counts.entry(&report.email).or_default() += 1;
    }
    let stats_xml = counts
        .into_iter()
        .map(|(email, count)| {
            format!(
                concat!(
                    "<t:NonIndexableItemStatistic>",
                    "<t:Mailbox>{email}</t:Mailbox>",
                    "<t:ItemCount>{count}</t:ItemCount>",
                    "</t:NonIndexableItemStatistic>"
                ),
                email = escape_xml(email),
                count = count,
            )
        })
        .collect::<String>();
    format!(
        concat!(
            "<m:GetNonIndexableItemStatisticsResponse>",
            "<m:ResponseMessages>",
            "<m:GetNonIndexableItemStatisticsResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:NonIndexableItemStatistics>{stats_xml}</m:NonIndexableItemStatistics>",
            "</m:GetNonIndexableItemStatisticsResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetNonIndexableItemStatisticsResponse>"
        ),
        stats_xml = stats_xml,
    )
}

fn non_indexable_report_xml(report: &EwsNonIndexableReport) -> String {
    format!(
        concat!(
            "<t:NonIndexableItemDetail>",
            "<t:ItemId>{id}</t:ItemId>",
            "<t:Mailbox>{email}</t:Mailbox>",
            "<t:ItemType>{kind}</t:ItemType>",
            "<t:ErrorDescription>{reason}</t:ErrorDescription>",
            "<t:MessageId>{message_id}</t:MessageId>",
            "<t:AttachmentId>{attachment_id}</t:AttachmentId>",
            "<t:DetectedAt>{detected_at}</t:DetectedAt>",
            "<t:IsResolved>{resolved}</t:IsResolved>",
            "</t:NonIndexableItemDetail>"
        ),
        id = report.id,
        email = escape_xml(&report.email),
        kind = escape_xml(&report.report_kind),
        reason = escape_xml(&report.reason),
        message_id = report
            .message_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
        attachment_id = report
            .attachment_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
        detected_at = escape_xml(&report.detected_at),
        resolved = report.resolved,
    )
}
