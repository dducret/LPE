---
type: TypeScript Function
title: MailDetail
resource: web/client/src/components/MailDetail.tsx#L6-L158
generated:
  by: okf-rs/0.3.0
---

# Signature

`function MailDetail(props: { copy: ClientCopy; current: Message | null; mode: Mode; draft: MessageDraft; messageBusy: boolean; notice: string; composerMailboxes: MailboxAccountAccess[]; setDraft: React.Dispatch<React.SetStateAction<MessageDraft>>; onReply: (message: Message) => void; onForward: (message: Message) => void; onToggleFlag: (message: Message) => void; onCompleteFlag: (message: Message, completed: boolean) => void; onSetFlagDue: (message: Message, daysFromToday: number | null) => void; onSetFlagReminder: (message: Message, minutesFromNow: number | null) => void; onCancel: () => void; onSaveDraft: () => void; onSend: () => void; onDeleteDraft: () => void; })`