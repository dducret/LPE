use anyhow::Result;
use lpe_storage::{AccessibleContact, AccessibleEvent, DavTask};

use crate::{
    paths::{contact_href, event_href, task_href},
    serialize::{format_ical_datetime, format_ical_timestamp},
};

#[derive(Debug, Default)]
pub(crate) struct ReportFilter {
    pub(crate) hrefs: Vec<String>,
    pub(crate) text_terms: Vec<String>,
    pub(crate) time_range_start: Option<String>,
    pub(crate) time_range_end: Option<String>,
}

pub(crate) fn parse_report_filter(body: &[u8]) -> Result<ReportFilter> {
    if body.is_empty() {
        return Ok(ReportFilter::default());
    }
    let xml = std::str::from_utf8(body)?;
    Ok(ReportFilter {
        hrefs: xml_tag_values(xml, "href"),
        text_terms: xml_text_match_values(xml),
        time_range_start: xml_attribute_value(xml, "time-range", "start"),
        time_range_end: xml_attribute_value(xml, "time-range", "end"),
    })
}

fn xml_tag_values(xml: &str, local_name: &str) -> Vec<String> {
    let mut values = Vec::new();
    let needle = format!(":{local_name}>");
    let mut remaining = xml;
    while let Some(index) = remaining.find(&needle) {
        let value_start = index + needle.len();
        let rest = &remaining[value_start..];
        let Some(value_end) = rest.find('<') else {
            break;
        };
        let value = rest[..value_end].trim();
        if !value.is_empty() {
            values.push(value.to_string());
        }
        remaining = &rest[value_end..];
    }
    values
}

fn xml_text_match_values(xml: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut remaining = xml;
    while let Some(index) = remaining.find(":text-match") {
        let Some(open_end) = remaining[index..].find('>') else {
            break;
        };
        let rest = &remaining[index + open_end + 1..];
        let Some(close_index) = rest.find("</") else {
            break;
        };
        let value = rest[..close_index].trim();
        if !value.is_empty() {
            values.push(value.to_string());
        }
        remaining = &rest[close_index + 2..];
    }
    values
}

fn xml_attribute_value(xml: &str, element: &str, attribute: &str) -> Option<String> {
    let needle = format!(":{element}");
    let index = xml.find(&needle)?;
    let rest = &xml[index..];
    let open_end = rest.find('>')?;
    let element_text = &rest[..open_end];
    let attr = format!("{attribute}=\"");
    let attr_index = element_text.find(&attr)?;
    let value_start = attr_index + attr.len();
    let value = &element_text[value_start..];
    let value_end = value.find('"')?;
    Some(value[..value_end].to_string())
}

pub(crate) fn contact_matches_report(contact: &AccessibleContact, filter: &ReportFilter) -> bool {
    if !filter.hrefs.is_empty()
        && !filter
            .hrefs
            .iter()
            .any(|href| href == &contact_href(&contact.collection_id, contact.id))
    {
        return false;
    }
    if filter.text_terms.is_empty() {
        return true;
    }
    let haystack = format!(
        "{} {} {} {} {} {}",
        contact.name, contact.email, contact.role, contact.phone, contact.team, contact.notes
    )
    .to_lowercase();
    filter
        .text_terms
        .iter()
        .all(|term| haystack.contains(&term.trim().to_lowercase()))
}

pub(crate) fn event_matches_report(event: &AccessibleEvent, filter: &ReportFilter) -> bool {
    if !filter.hrefs.is_empty()
        && !filter
            .hrefs
            .iter()
            .any(|href| href == &event_href(&event.collection_id, event.id))
    {
        return false;
    }
    if !filter.text_terms.is_empty() {
        let haystack = format!(
            "{} {} {} {}",
            event.title, event.location, event.attendees, event.notes
        )
        .to_lowercase();
        if !filter
            .text_terms
            .iter()
            .all(|term| haystack.contains(&term.trim().to_lowercase()))
        {
            return false;
        }
    }
    let start = format_ical_datetime(&event.date, &event.time);
    if let Some(range_start) = filter.time_range_start.as_deref() {
        if normalize_time_range_value(range_start).as_deref() > Some(start.as_str()) {
            return false;
        }
    }
    if let Some(range_end) = filter.time_range_end.as_deref() {
        if normalize_time_range_value(range_end).as_deref() <= Some(start.as_str()) {
            return false;
        }
    }
    true
}

pub(crate) fn task_matches_report(task: &DavTask, filter: &ReportFilter) -> bool {
    if !filter.hrefs.is_empty()
        && !filter
            .hrefs
            .iter()
            .any(|href| href == &task_href(&task.collection_id, task.id))
    {
        return false;
    }
    if !filter.text_terms.is_empty() {
        let haystack =
            format!("{} {} {}", task.title, task.description, task.status).to_lowercase();
        if !filter
            .text_terms
            .iter()
            .all(|term| haystack.contains(&term.trim().to_lowercase()))
        {
            return false;
        }
    }
    if filter.time_range_start.is_some() || filter.time_range_end.is_some() {
        let Some(due_at) = task.due_at.as_deref() else {
            return false;
        };
        let due = normalize_time_range_value(&format_ical_timestamp(due_at))
            .unwrap_or_else(|| format_ical_timestamp(due_at));
        if let Some(range_start) = filter.time_range_start.as_deref() {
            if normalize_time_range_value(range_start).as_deref() > Some(due.as_str()) {
                return false;
            }
        }
        if let Some(range_end) = filter.time_range_end.as_deref() {
            if normalize_time_range_value(range_end).as_deref() <= Some(due.as_str()) {
                return false;
            }
        }
    }
    true
}

fn normalize_time_range_value(value: &str) -> Option<String> {
    let value = value.trim_end_matches('Z');
    if value.len() < 15 {
        return None;
    }
    Some(value[..15].to_string())
}
