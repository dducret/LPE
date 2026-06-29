use super::super::*;

pub(in crate::service) fn resolve_names_response(
    principal: &AccountPrincipal,
    request: &str,
    entries: &[ExchangeAddressBookEntry],
) -> String {
    let query = element_text(request, "UnresolvedEntry")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if query.is_empty() {
        return resolve_names_no_results_response();
    }
    let principal_entry = principal_address_book_entry(principal);
    let matched = entries
        .iter()
        .find(|entry| address_book_entry_matches(entry, &query, true))
        .or_else(|| {
            address_book_lookup_matches_principal(&query, principal).then_some(&principal_entry)
        });
    let Some(entry) = matched else {
        return resolve_names_no_results_response();
    };

    format!(
        concat!(
            "<m:ResolveNamesResponse>",
            "<m:ResponseMessages>",
            "<m:ResolveNamesResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:ResolutionSet TotalItemsInView=\"1\" IncludesLastItemInRange=\"true\">",
            "<t:Resolution>",
            "<t:Mailbox>",
            "<t:Name>{}</t:Name>",
            "<t:EmailAddress>{}</t:EmailAddress>",
            "<t:RoutingType>SMTP</t:RoutingType>",
            "<t:MailboxType>{}</t:MailboxType>",
            "</t:Mailbox>",
            "</t:Resolution>",
            "</m:ResolutionSet>",
            "</m:ResolveNamesResponseMessage>",
            "</m:ResponseMessages>",
            "</m:ResolveNamesResponse>"
        ),
        escape_xml(&entry.display_name),
        escape_xml(&entry.email),
        ews_mailbox_type(entry),
    )
}

pub(in crate::service) fn find_people_response(
    principal: &AccountPrincipal,
    request: &str,
    entries: &[ExchangeAddressBookEntry],
) -> String {
    let query = find_people_query_text(request);
    let mut visible_entries = visible_persona_entries(principal, entries);
    if !query.is_empty() {
        visible_entries.retain(|entry| address_book_entry_matches(entry, &query, true));
    }
    visible_entries.truncate(100);
    let personas_xml = visible_entries
        .iter()
        .map(persona_xml)
        .collect::<Vec<_>>()
        .join("");
    format!(
        concat!(
            "<m:FindPeopleResponse>",
            "<m:ResponseMessages>",
            "<m:FindPeopleResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:People TotalNumberOfPeopleInView=\"{count}\" FirstMatchingRowIndex=\"0\" FirstLoadedRowIndex=\"0\">",
            "{personas_xml}",
            "</m:People>",
            "</m:FindPeopleResponseMessage>",
            "</m:ResponseMessages>",
            "</m:FindPeopleResponse>"
        ),
        count = visible_entries.len(),
        personas_xml = personas_xml,
    )
}

pub(in crate::service) fn get_persona_response(
    principal: &AccountPrincipal,
    request: &str,
    entries: &[ExchangeAddressBookEntry],
) -> String {
    let Some(persona_id) = requested_persona_id(request) else {
        return operation_error_response(
            "GetPersona",
            "ErrorInvalidOperation",
            "PersonaId is required.",
        );
    };
    let Some((kind, id)) = parse_persona_id(&persona_id) else {
        return operation_error_response(
            "GetPersona",
            "ErrorItemNotFound",
            "The requested persona was not found.",
        );
    };
    let visible_entries = visible_persona_entries(principal, entries);
    let Some(entry) = visible_entries
        .iter()
        .find(|entry| entry.entry_kind == kind && entry.id == id)
    else {
        return operation_error_response(
            "GetPersona",
            "ErrorItemNotFound",
            "The requested persona was not found.",
        );
    };
    format!(
        concat!(
            "<m:GetPersonaResponse>",
            "<m:ResponseMessages>",
            "<m:GetPersonaResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "{persona_xml}",
            "</m:GetPersonaResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetPersonaResponse>"
        ),
        persona_xml = persona_xml(entry),
    )
}

pub(in crate::service) fn expand_dl_response(
    principal: &AccountPrincipal,
    request: &str,
    entries: &[ExchangeAddressBookEntry],
) -> String {
    let Some(mailbox) = parse_first_mailbox(request) else {
        return operation_error_response(
            "ExpandDL",
            "ErrorNameResolutionNoResults",
            "No results are found.",
        );
    };
    let query = mailbox.address.trim().to_ascii_lowercase();
    let principal_entry = principal_address_book_entry(principal);
    let mut visible_entries = entries.to_vec();
    if !visible_entries
        .iter()
        .any(|entry| entry.email.eq_ignore_ascii_case(&principal_entry.email))
    {
        visible_entries.push(principal_entry);
    }

    let Some(distribution_list) = visible_entries.iter().find(|entry| {
        entry.entry_kind == ExchangeAddressBookEntryKind::DistributionList
            && address_book_entry_matches(entry, &query, false)
    }) else {
        return operation_error_response(
            "ExpandDL",
            "ErrorNameResolutionNoResults",
            "No results are found.",
        );
    };

    let mut members = Vec::new();
    for member_email in &distribution_list.member_emails {
        let normalized = member_email.trim().to_ascii_lowercase();
        if let Some(member) = visible_entries
            .iter()
            .find(|entry| entry.email.eq_ignore_ascii_case(&normalized))
        {
            members.push(member.clone());
        }
    }
    let member_xml = members
        .iter()
        .map(ews_mailbox_xml)
        .collect::<Vec<_>>()
        .join("");

    format!(
        concat!(
            "<m:ExpandDLResponse>",
            "<m:ResponseMessages>",
            "<m:ExpandDLResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:DLExpansion TotalItemsInView=\"{count}\" IncludesLastItemInRange=\"true\">",
            "{member_xml}",
            "</m:DLExpansion>",
            "</m:ExpandDLResponseMessage>",
            "</m:ResponseMessages>",
            "</m:ExpandDLResponse>"
        ),
        count = members.len(),
        member_xml = member_xml,
    )
}

pub(in crate::service) fn visible_address_book_email(
    principal: &AccountPrincipal,
    entries: &[ExchangeAddressBookEntry],
    email: &str,
) -> bool {
    principal.email.eq_ignore_ascii_case(email)
        || entries.iter().any(|entry| {
            matches!(
                entry.entry_kind,
                ExchangeAddressBookEntryKind::Account | ExchangeAddressBookEntryKind::Contact
            ) && entry.email.eq_ignore_ascii_case(email)
        })
}

fn resolve_names_no_results_response() -> String {
    concat!(
        "<m:ResolveNamesResponse>",
        "<m:ResponseMessages>",
        "<m:ResolveNamesResponseMessage ResponseClass=\"Error\">",
        "<m:MessageText>No results were found.</m:MessageText>",
        "<m:ResponseCode>ErrorNameResolutionNoResults</m:ResponseCode>",
        "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
        "</m:ResolveNamesResponseMessage>",
        "</m:ResponseMessages>",
        "</m:ResolveNamesResponse>"
    )
    .to_string()
}

fn ews_mailbox_xml(entry: &ExchangeAddressBookEntry) -> String {
    format!(
        concat!(
            "<t:Mailbox>",
            "<t:Name>{}</t:Name>",
            "<t:EmailAddress>{}</t:EmailAddress>",
            "<t:RoutingType>SMTP</t:RoutingType>",
            "<t:MailboxType>{}</t:MailboxType>",
            "</t:Mailbox>"
        ),
        escape_xml(&entry.display_name),
        escape_xml(&entry.email),
        ews_mailbox_type(entry),
    )
}

fn visible_persona_entries(
    principal: &AccountPrincipal,
    entries: &[ExchangeAddressBookEntry],
) -> Vec<ExchangeAddressBookEntry> {
    let principal_entry = principal_address_book_entry(principal);
    let mut visible = entries
        .iter()
        .filter(|entry| {
            matches!(
                entry.entry_kind,
                ExchangeAddressBookEntryKind::Account | ExchangeAddressBookEntryKind::Contact
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    if !visible.iter().any(|entry| {
        entry.entry_kind == ExchangeAddressBookEntryKind::Account
            && entry.id == principal.account_id
    }) {
        visible.push(principal_entry);
    }
    visible.sort_by(|left, right| {
        left.display_name
            .to_ascii_lowercase()
            .cmp(&right.display_name.to_ascii_lowercase())
            .then_with(|| {
                left.email
                    .to_ascii_lowercase()
                    .cmp(&right.email.to_ascii_lowercase())
            })
            .then_with(|| persona_id(left).cmp(&persona_id(right)))
    });
    visible.dedup_by(|left, right| left.entry_kind == right.entry_kind && left.id == right.id);
    visible
}

fn find_people_query_text(request: &str) -> String {
    element_text(request, "QueryString")
        .or_else(|| element_text(request, "SearchString"))
        .or_else(|| element_text(request, "Query"))
        .or_else(|| element_text(request, "EmailAddress"))
        .or_else(|| element_text(request, "SmtpAddress"))
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

fn requested_persona_id(request: &str) -> Option<String> {
    attribute_values_for_tag(request, "PersonaId", "Id")
        .first()
        .map(|value| (*value).to_string())
        .or_else(|| element_text(request, "PersonaId"))
        .or_else(|| element_text(request, "PersonaID"))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn persona_id(entry: &ExchangeAddressBookEntry) -> String {
    match entry.entry_kind {
        ExchangeAddressBookEntryKind::Account => format!("persona:account:{}", entry.id),
        ExchangeAddressBookEntryKind::Contact => format!("persona:contact:{}", entry.id),
        ExchangeAddressBookEntryKind::DistributionList => format!("persona:group:{}", entry.id),
    }
}

fn parse_persona_id(value: &str) -> Option<(ExchangeAddressBookEntryKind, Uuid)> {
    let value = value.trim();
    let Some(rest) = value.strip_prefix("persona:") else {
        return None;
    };
    let (kind, id) = rest.split_once(':')?;
    let id = Uuid::parse_str(id).ok()?;
    let kind = match kind {
        "account" => ExchangeAddressBookEntryKind::Account,
        "contact" => ExchangeAddressBookEntryKind::Contact,
        _ => return None,
    };
    Some((kind, id))
}

fn persona_xml(entry: &ExchangeAddressBookEntry) -> String {
    let persona_id = persona_id(entry);
    let persona_type = match entry.entry_kind {
        ExchangeAddressBookEntryKind::Account => "Person",
        ExchangeAddressBookEntryKind::Contact => "Contact",
        ExchangeAddressBookEntryKind::DistributionList => "DistributionList",
    };
    format!(
        concat!(
            "<t:Persona>",
            "<t:PersonaId Id=\"{persona_id}\"/>",
            "<t:PersonaType>{persona_type}</t:PersonaType>",
            "<t:DisplayName>{display_name}</t:DisplayName>",
            "<t:DisplayNameFirstLast>{display_name}</t:DisplayNameFirstLast>",
            "<t:DisplayNameLastFirst>{display_name}</t:DisplayNameLastFirst>",
            "<t:FileAs>{display_name}</t:FileAs>",
            "<t:EmailAddress>{email}</t:EmailAddress>",
            "<t:EmailAddresses>",
            "<t:EmailAddress>",
            "<t:Name>{display_name}</t:Name>",
            "<t:EmailAddress>{email}</t:EmailAddress>",
            "<t:RoutingType>SMTP</t:RoutingType>",
            "<t:MailboxType>{mailbox_type}</t:MailboxType>",
            "</t:EmailAddress>",
            "</t:EmailAddresses>",
            "</t:Persona>"
        ),
        persona_id = escape_xml(&persona_id),
        persona_type = persona_type,
        display_name = escape_xml(&entry.display_name),
        email = escape_xml(&entry.email),
        mailbox_type = ews_mailbox_type(entry),
    )
}

fn principal_address_book_entry(principal: &AccountPrincipal) -> ExchangeAddressBookEntry {
    ExchangeAddressBookEntry {
        id: principal.account_id,
        display_name: principal.display_name.clone(),
        email: principal.email.clone(),
        entry_kind: ExchangeAddressBookEntryKind::Account,
        directory_kind: ExchangeAddressBookDirectoryKind::Person,
        member_emails: Vec::new(),
        details: ExchangeAddressBookEntryDetails::default(),
    }
}

fn address_book_lookup_matches_principal(value: &str, principal: &AccountPrincipal) -> bool {
    let value = normalize_address_book_lookup(value);
    let email = principal.email.to_ascii_lowercase();
    let display_name = principal.display_name.to_ascii_lowercase();
    value == email || value == display_name || email.contains(value.as_str())
}

fn address_book_entry_matches(
    entry: &ExchangeAddressBookEntry,
    value: &str,
    allow_partial: bool,
) -> bool {
    let value = normalize_address_book_lookup(value);
    if value.is_empty() {
        return false;
    }
    let email = entry.email.to_ascii_lowercase();
    let display_name = entry.display_name.to_ascii_lowercase();
    value == email
        || value == display_name
        || value == format!("smtp:{email}")
        || value == format!("=smtp:{email}")
        || (allow_partial
            && (email.contains(value.as_str()) || display_name.contains(value.as_str())))
}

fn normalize_address_book_lookup(value: &str) -> String {
    normalization::normalize_smtp_lookup_value(value)
}

fn ews_mailbox_type(entry: &ExchangeAddressBookEntry) -> &'static str {
    match entry.entry_kind {
        ExchangeAddressBookEntryKind::Contact => "Contact",
        ExchangeAddressBookEntryKind::Account => "Mailbox",
        ExchangeAddressBookEntryKind::DistributionList => "PublicDL",
    }
}
