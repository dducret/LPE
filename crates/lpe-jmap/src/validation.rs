use anyhow::{bail, Result};

use crate::parse::{parse_local_datetime, parse_uuid};
use crate::protocol::{
    CalendarEventQueryFilter, ContactCardQueryFilter, EmailQuerySort, EntityQuerySort,
    TaskQueryFilter, TaskQuerySort,
};

pub(crate) fn validate_query_sort(sort: Option<&[EmailQuerySort]>) -> Result<()> {
    if let Some(sort) = sort {
        for item in sort {
            if item.property != "receivedAt" || item.is_ascending.unwrap_or(false) {
                bail!("only receivedAt descending sort is supported");
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_entity_sort(
    sort: Option<&[EntityQuerySort]>,
    expected_property: &str,
    ascending: bool,
) -> Result<()> {
    if let Some(sort) = sort {
        for item in sort {
            if item.property != expected_property || item.is_ascending.unwrap_or(true) != ascending
            {
                let direction = if ascending { "ascending" } else { "descending" };
                bail!("only {expected_property} {direction} sort is supported");
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_contact_filter(filter: Option<&ContactCardQueryFilter>) -> Result<()> {
    if let Some(filter) = filter {
        if let Some(address_book_id) = filter.in_address_book.as_deref() {
            require_collection_id(address_book_id, "addressBook")?;
        }
    }
    Ok(())
}

pub(crate) fn validate_calendar_event_filter(
    filter: Option<&CalendarEventQueryFilter>,
) -> Result<()> {
    if let Some(filter) = filter {
        if let Some(calendar_id) = filter.in_calendar.as_deref() {
            require_collection_id(calendar_id, "calendar")?;
        }
        if let Some(after) = filter.after.as_deref() {
            parse_local_datetime(after)?;
        }
        if let Some(before) = filter.before.as_deref() {
            parse_local_datetime(before)?;
        }
    }
    Ok(())
}

pub(crate) fn validate_task_sort(sort: Option<&[TaskQuerySort]>) -> Result<()> {
    if let Some(sort) = sort {
        for item in sort {
            if item.property != "sortOrder" || item.is_ascending.unwrap_or(true) != true {
                bail!("only sortOrder ascending sort is supported");
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_task_filter(filter: Option<&TaskQueryFilter>) -> Result<()> {
    if let Some(filter) = filter {
        if let Some(task_list_id) = filter.in_task_list.as_deref() {
            parse_uuid(task_list_id)?;
        }
        if let Some(status) = filter.status.as_deref() {
            validate_task_status_value(status)?;
        }
    }
    Ok(())
}

pub(crate) fn require_collection_id(value: &str, kind: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{kind} id is required");
    }
    Ok(())
}

pub(crate) fn validate_task_status_value(status: &str) -> Result<()> {
    match status.trim().to_ascii_lowercase().as_str() {
        "" | "needs-action" | "in-progress" | "completed" | "cancelled" => Ok(()),
        other => bail!("unsupported task status: {other}"),
    }
}
