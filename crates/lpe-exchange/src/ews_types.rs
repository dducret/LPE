use anyhow::{bail, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EwsDeleteType {
    HardDelete,
    SoftDelete,
    MoveToDeletedItems,
}

impl EwsDeleteType {
    #[allow(dead_code)]
    pub(crate) const DOCUMENTED_VALUES: &'static [(&'static str, Self)] = &[
        ("HardDelete", Self::HardDelete),
        ("SoftDelete", Self::SoftDelete),
        ("MoveToDeletedItems", Self::MoveToDeletedItems),
    ];

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
    #[allow(dead_code)]
    pub(crate) const DOCUMENTED_SUPPORTED_VALUES: &'static [(&'static str, Self)] = &[
        ("inbox", Self::Inbox),
        ("drafts", Self::Drafts),
        ("sentitems", Self::SentItems),
        ("deleteditems", Self::DeletedItems),
        ("msgfolderroot", Self::MsgFolderRoot),
        ("root", Self::Root),
        ("contacts", Self::Contacts),
        ("calendar", Self::Calendar),
        ("tasks", Self::Tasks),
    ];

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

    #[allow(dead_code)]
    pub(crate) fn known_unsupported_name(value: &str) -> Option<&'static str> {
        match normalized(value).as_str() {
            "publicfoldersroot" => Some("publicfoldersroot"),
            "searchfolders" => Some("searchfolders"),
            "voicemail" => Some("voicemail"),
            "recoverableitemsroot" => Some("recoverableitemsroot"),
            "recoverableitemsdeletions" => Some("recoverableitemsdeletions"),
            "recoverableitemsversions" => Some("recoverableitemsversions"),
            "recoverableitemspurges" => Some("recoverableitemspurges"),
            "archiveroot" => Some("archiveroot"),
            "archivemsgfolderroot" => Some("archivemsgfolderroot"),
            "archiveinbox" => Some("archiveinbox"),
            "archivedeleteditems" => Some("archivedeleteditems"),
            "archivecontacts" => Some("archivecontacts"),
            "archivecalendar" => Some("archivecalendar"),
            "archivetasks" => Some("archivetasks"),
            "archivejunkemail" => Some("archivejunkemail"),
            "archiverecoverableitemsroot" => Some("archiverecoverableitemsroot"),
            "archiverecoverableitemsdeletions" => Some("archiverecoverableitemsdeletions"),
            "archiverecoverableitemsversions" => Some("archiverecoverableitemsversions"),
            "archiverecoverableitemspurges" => Some("archiverecoverableitemspurges"),
            "syncissues" => Some("syncissues"),
            "conflicts" => Some("conflicts"),
            "localfailures" => Some("localfailures"),
            "serverfailures" => Some("serverfailures"),
            "recipientcache" => Some("recipientcache"),
            "quickcontacts" => Some("quickcontacts"),
            "conversationhistory" => Some("conversationhistory"),
            "adminauditlogs" => Some("adminauditlogs"),
            "todosearch" => Some("todosearch"),
            "mycontacts" => Some("mycontacts"),
            "directory" => Some("directory"),
            "imcontactlist" => Some("imcontactlist"),
            "peopleconnect" => Some("peopleconnect"),
            "favorites" => Some("favorites"),
            "junkemail" => Some("junkemail"),
            "journal" => Some("journal"),
            "notes" => Some("notes"),
            "outbox" => Some("outbox"),
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
    #[allow(dead_code)]
    pub(crate) const DOCUMENTED_VALUES: &'static [(&'static str, Self)] = &[
        ("None", Self::None),
        ("Known", Self::Known),
        ("All", Self::All),
    ];

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
    #[allow(dead_code)]
    pub(crate) const DOCUMENTED_VALUES: &'static [(&'static str, Self, u32)] = &[
        ("January", Self::January, 1),
        ("February", Self::February, 2),
        ("March", Self::March, 3),
        ("April", Self::April, 4),
        ("May", Self::May, 5),
        ("June", Self::June, 6),
        ("July", Self::July, 7),
        ("August", Self::August, 8),
        ("September", Self::September, 9),
        ("October", Self::October, 10),
        ("November", Self::November, 11),
        ("December", Self::December, 12),
    ];

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
    #[allow(dead_code)]
    pub(crate) const DOCUMENTED_VALUES: &'static [(&'static str, Self)] = &[
        ("Disabled", Self::Disabled),
        ("Enabled", Self::Enabled),
        ("Scheduled", Self::Scheduled),
    ];

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
    #[allow(dead_code)]
    pub(crate) const DOCUMENTED_VALUES: &'static [(&'static str, Self, &'static str)] = &[
        ("Accept", Self::Accept, "accepted"),
        ("Tentative", Self::Tentative, "tentative"),
        ("Decline", Self::Decline, "declined"),
        (
            "NoResponseReceived",
            Self::NoResponseReceived,
            "needs-action",
        ),
        ("Unknown", Self::Unknown, "needs-action"),
    ];

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
    #[allow(dead_code)]
    pub(crate) const DOCUMENTED_VALUES: &'static [(&'static str, Self, &'static str)] = &[
        ("NotStarted", Self::NotStarted, "needs-action"),
        ("InProgress", Self::InProgress, "in-progress"),
        ("Completed", Self::Completed, "completed"),
        ("WaitingOnOthers", Self::WaitingOnOthers, "in-progress"),
        ("Deferred", Self::Deferred, "cancelled"),
    ];

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
    #[allow(dead_code)]
    pub(crate) const DOCUMENTED_VALUES: &'static [(&'static str, Self, &'static str)] = &[
        ("Monday", Self::Monday, "MO"),
        ("Tuesday", Self::Tuesday, "TU"),
        ("Wednesday", Self::Wednesday, "WE"),
        ("Thursday", Self::Thursday, "TH"),
        ("Friday", Self::Friday, "FR"),
        ("Saturday", Self::Saturday, "SA"),
        ("Sunday", Self::Sunday, "SU"),
    ];

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
        for (value, expected) in EwsDeleteType::DOCUMENTED_VALUES {
            assert_eq!(EwsDeleteType::parse(value).unwrap(), *expected);
        }
        for (value, expected) in EwsDistinguishedFolderIdName::DOCUMENTED_SUPPORTED_VALUES {
            assert_eq!(EwsDistinguishedFolderIdName::parse(value), Some(*expected));
        }
        for (value, expected) in EwsExternalAudience::DOCUMENTED_VALUES {
            assert_eq!(EwsExternalAudience::parse(value).unwrap(), *expected);
            assert_eq!(expected.as_ews(), *value);
        }
        for (value, expected, number) in EwsMonth::DOCUMENTED_VALUES {
            assert_eq!(EwsMonth::parse(value).unwrap(), *expected);
            assert_eq!(expected.number(), *number);
        }
        for (value, expected) in EwsOofState::DOCUMENTED_VALUES {
            assert_eq!(EwsOofState::parse(value).unwrap(), *expected);
            assert_eq!(expected.as_ews(), *value);
        }
        for (value, expected, partstat) in EwsResponseType::DOCUMENTED_VALUES {
            assert_eq!(EwsResponseType::parse(value), *expected);
            assert_eq!(expected.partstat(), *partstat);
        }
        for (value, expected, canonical) in EwsTaskStatus::DOCUMENTED_VALUES {
            assert_eq!(EwsTaskStatus::parse(value).unwrap(), *expected);
            assert_eq!(expected.canonical_status(), *canonical);
        }
        for (value, expected, rrule_day) in EwsWeekday::DOCUMENTED_VALUES {
            assert_eq!(EwsWeekday::parse(value).unwrap(), *expected);
            assert_eq!(expected.rrule_day(), *rrule_day);
        }
        assert_eq!(
            EwsDistinguishedFolderIdName::known_unsupported_name("junkemail"),
            Some("junkemail")
        );
    }
}
