import type { ContactItem, EventItem, Message } from "./client-types";

export const currentUser = { name: "Alex Meyer", email: "alex.meyer@lpe.example" };

export const seedMessages: Message[] = [
  {
    id: "m1",
    folder: "focused",
    from: "Marta Vogel",
    fromAddress: "marta.vogel@northwind.example",
    to: currentUser.email,
    cc: "legal@lpe.example",
    subject: "Contract review before domain migration",
    preview: "I added the revised delivery clauses and the PDF for legal review before tonight's cutover window.",
    receivedAt: "2026-04-15 08:42",
    timeLabel: "08:42",
    unread: true,
    flagged: true,
    category: "priority",
    tags: ["Legal", "Migration"],
    attachments: [{ id: "a1", name: "northwind-domain-cutover.pdf", kind: "PDF", size: "2.4 MB" }],
    body: [
      "Hello team,",
      "I have attached the revised contract package for the Northwind domain migration. Please confirm the wording around delegated administration and retention before we freeze the window.",
      "If everything looks correct, I will approve the rollout for tonight and notify support.",
      "Marta"
    ]
  },
  {
    id: "m2",
    folder: "focused",
    from: "Support Queue",
    fromAddress: "support@lpe.example",
    to: currentUser.email,
    cc: "",
    subject: "Three users reported delayed sync on mobile",
    preview: "The incidents all point to the same JMAP session refresh path after mailbox rule changes.",
    receivedAt: "2026-04-15 07:18",
    timeLabel: "07:18",
    unread: true,
    flagged: false,
    category: "customer",
    tags: ["Support", "JMAP"],
    attachments: [],
    body: [
      "Morning,",
      "We have three fresh reports from the Lyon office. Mail eventually arrives, but the mobile client takes around ninety seconds to refresh after a server-side rule update.",
      "The desktop web client stays consistent, so this likely sits on the session or push path rather than the search index.",
      "Support queue"
    ]
  },
  {
    id: "m3",
    folder: "inbox",
    from: "Elena Rossi",
    fromAddress: "elena.rossi@lpe.example",
    to: currentUser.email,
    cc: "",
    subject: "Attachment extraction metrics for this week",
    preview: "PDF, DOCX and ODT extraction stayed within budget; the weekly summary is attached as a DOCX memo.",
    receivedAt: "2026-04-14 17:00",
    timeLabel: "Yesterday",
    unread: false,
    flagged: false,
    category: "internal",
    tags: ["Search", "Indexing"],
    attachments: [{ id: "a2", name: "attachment-extraction-weekly.docx", kind: "DOCX", size: "418 KB" }],
    body: [
      "Hi,",
      "The extraction pipeline stayed stable across the supported v1 formats. PDF remained the largest share, with DOCX following and ODT staying marginal.",
      "I attached the memo we can reuse for the product status meeting.",
      "Elena"
    ]
  },
  {
    id: "m4",
    folder: "drafts",
    from: currentUser.name,
    fromAddress: currentUser.email,
    to: "exec@lpe.example",
    cc: "",
    subject: "Draft: migration wave briefing",
    preview: "Preparing the executive summary for the next domain onboarding wave with risk, owners and schedule.",
    receivedAt: "2026-04-13 09:00",
    timeLabel: "Mon",
    unread: false,
    flagged: false,
    category: "priority",
    tags: ["Draft"],
    attachments: [{ id: "a3", name: "wave-briefing.odt", kind: "ODT", size: "280 KB" }],
    body: [
      "Draft notes:",
      "Outline the onboarding wave, responsible domain admins, mailbox transfer checkpoints and support escalation contacts.",
      "Need final numbers from finance before sending."
    ]
  }
];

export const seedEvents: EventItem[] = [
  { id: "e1", date: "2026-04-15", time: "09:30", title: "Migration stand-up", location: "Room Atlas", attendees: "Ops, Support, Product", notes: "Review blockers for the next domain cutover." },
  { id: "e2", date: "2026-04-15", time: "12:00", title: "Mailbox search review", location: "Video", attendees: "Search, Storage", notes: "Check extracted-text indexing latency for supported attachments." },
  { id: "e3", date: "2026-04-15", time: "16:30", title: "Domain admin onboarding", location: "Room Rhine", attendees: "Customer Success", notes: "Walk through delegated admin policies and mailbox lifecycle." }
];

export const seedContacts: ContactItem[] = [
  { id: "c1", name: "Marta Vogel", role: "Customer migration lead", email: "marta.vogel@northwind.example", phone: "+49 30 555 0142", team: "Northwind", notes: "Primary contact for the April migration wave." },
  { id: "c2", name: "Elena Rossi", role: "Search engineer", email: "elena.rossi@lpe.example", phone: "+39 02 555 2031", team: "Platform", notes: "Owns search and attachment extraction metrics." },
  { id: "c3", name: "Jonas Keller", role: "Domain administrator", email: "jonas.keller@contoso.example", phone: "+41 44 555 9912", team: "Contoso", notes: "Needs mailbox import coordination and alias review." }
];
