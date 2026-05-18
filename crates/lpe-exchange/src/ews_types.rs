use anyhow::{bail, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EwsDeleteType {
    HardDelete,
    SoftDelete,
    MoveToDeletedItems,
}

impl EwsDeleteType {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        match normalized(value).as_str() {
            "harddelete" => Ok(Self::HardDelete),
            "softdelete" => Ok(Self::SoftDelete),
            "movetodeleteditems" => Ok(Self::MoveToDeletedItems),
            other => bail!("unsupported EWS DeleteType {other}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EwsDistinguishedFolderIdName {
    Inbox,
    Drafts,
    SentItems,
    DeletedItems,
    MsgFolderRoot,
    Root,
    Contacts,
    Calendar,
    Tasks,
}

impl EwsDistinguishedFolderIdName {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match normalized(value).as_str() {
            "inbox" => Some(Self::Inbox),
            "drafts" => Some(Self::Drafts),
            "sentitems" | "sent" => Some(Self::SentItems),
            "deleteditems" | "trash" => Some(Self::DeletedItems),
            "msgfolderroot" => Some(Self::MsgFolderRoot),
            "root" => Some(Self::Root),
            "contacts" => Some(Self::Contacts),
            "calendar" => Some(Self::Calendar),
            "tasks" => Some(Self::Tasks),
            _ => None,
        }
    }

    pub(crate) fn mailbox_role(self) -> Option<&'static str> {
        match self {
            Self::Inbox => Some("inbox"),
            Self::Drafts => Some("drafts"),
            Self::SentItems => Some("sent"),
            Self::DeletedItems => Some("trash"),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EwsExternalAudience {
    None,
    Known,
    All,
}

impl EwsExternalAudience {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        match normalized(value).as_str() {
            "" | "none" => Ok(Self::None),
            "known" => Ok(Self::Known),
            "all" => Ok(Self::All),
            other => bail!("unsupported ExternalAudience {other}"),
        }
    }

    pub(crate) fn as_ews(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::Known => "Known",
            Self::All => "All",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EwsMonth {
    January,
    February,
    March,
    April,
    May,
    June,
    July,
    August,
    September,
    October,
    November,
    December,
}

impl EwsMonth {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        match normalized(value).as_str() {
            "january" => Ok(Self::January),
            "february" => Ok(Self::February),
            "march" => Ok(Self::March),
            "april" => Ok(Self::April),
            "may" => Ok(Self::May),
            "june" => Ok(Self::June),
            "july" => Ok(Self::July),
            "august" => Ok(Self::August),
            "september" => Ok(Self::September),
            "october" => Ok(Self::October),
            "november" => Ok(Self::November),
            "december" => Ok(Self::December),
            other => bail!("unsupported recurrence month {other}"),
        }
    }

    pub(crate) fn number(self) -> u32 {
        match self {
            Self::January => 1,
            Self::February => 2,
            Self::March => 3,
            Self::April => 4,
            Self::May => 5,
            Self::June => 6,
            Self::July => 7,
            Self::August => 8,
            Self::September => 9,
            Self::October => 10,
            Self::November => 11,
            Self::December => 12,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EwsOofState {
    Disabled,
    Enabled,
    Scheduled,
}

impl EwsOofState {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        match normalized(value).as_str() {
            "disabled" => Ok(Self::Disabled),
            "enabled" => Ok(Self::Enabled),
            "scheduled" => Ok(Self::Scheduled),
            other => bail!("unsupported OofState {other}"),
        }
    }

    pub(crate) fn as_ews(self) -> &'static str {
        match self {
            Self::Disabled => "Disabled",
            Self::Enabled => "Enabled",
            Self::Scheduled => "Scheduled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EwsResponseType {
    Accept,
    Tentative,
    Decline,
    NoResponseReceived,
    Unknown,
}

impl EwsResponseType {
    pub(crate) fn parse(value: &str) -> Self {
        match normalized(value).as_str() {
            "accept" => Self::Accept,
            "tentative" => Self::Tentative,
            "decline" => Self::Decline,
            "noresponsereceived" => Self::NoResponseReceived,
            _ => Self::Unknown,
        }
    }

    pub(crate) fn partstat(self) -> &'static str {
        match self {
            Self::Accept => "accepted",
            Self::Tentative => "tentative",
            Self::Decline => "declined",
            Self::NoResponseReceived | Self::Unknown => "needs-action",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EwsTaskStatus {
    NotStarted,
    InProgress,
    Completed,
    WaitingOnOthers,
    Deferred,
}

impl EwsTaskStatus {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        match normalized(value).as_str() {
            "" | "notstarted" | "needs-action" => Ok(Self::NotStarted),
            "inprogress" | "in-progress" => Ok(Self::InProgress),
            "completed" => Ok(Self::Completed),
            "waitingonothers" => Ok(Self::WaitingOnOthers),
            "deferred" | "cancelled" | "canceled" => Ok(Self::Deferred),
            other => bail!("unsupported task Status {other}"),
        }
    }

    pub(crate) fn canonical_status(self) -> &'static str {
        match self {
            Self::NotStarted => "needs-action",
            Self::InProgress | Self::WaitingOnOthers => "in-progress",
            Self::Completed => "completed",
            Self::Deferred => "cancelled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EwsWeekday {
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday,
}

impl EwsWeekday {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        match normalized(value).as_str() {
            "monday" => Ok(Self::Monday),
            "tuesday" => Ok(Self::Tuesday),
            "wednesday" => Ok(Self::Wednesday),
            "thursday" => Ok(Self::Thursday),
            "friday" => Ok(Self::Friday),
            "saturday" => Ok(Self::Saturday),
            "sunday" => Ok(Self::Sunday),
            other => bail!("unsupported recurrence weekday {other}"),
        }
    }

    pub(crate) fn rrule_day(self) -> &'static str {
        match self {
            Self::Monday => "MO",
            Self::Tuesday => "TU",
            Self::Wednesday => "WE",
            Self::Thursday => "TH",
            Self::Friday => "FR",
            Self::Saturday => "SA",
            Self::Sunday => "SU",
        }
    }
}

fn normalized(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ews_simple_type_enums_accept_documented_mvp_values() {
        assert_eq!(
            EwsDeleteType::parse("MoveToDeletedItems").unwrap(),
            EwsDeleteType::MoveToDeletedItems
        );
        assert_eq!(
            EwsDistinguishedFolderIdName::parse("sentitems")
                .unwrap()
                .mailbox_role(),
            Some("sent")
        );
        assert_eq!(
            EwsExternalAudience::parse("Known").unwrap().as_ews(),
            "Known"
        );
        assert_eq!(EwsMonth::parse("September").unwrap().number(), 9);
        assert_eq!(
            EwsOofState::parse("Scheduled").unwrap().as_ews(),
            "Scheduled"
        );
        assert_eq!(EwsResponseType::parse("Decline").partstat(), "declined");
        assert_eq!(
            EwsTaskStatus::parse("WaitingOnOthers")
                .unwrap()
                .canonical_status(),
            "in-progress"
        );
        assert_eq!(EwsWeekday::parse("Thursday").unwrap().rrule_day(), "TH");
    }
}
