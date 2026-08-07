---
type: TypeScript Function
title: MailDetail
resource: web/client/src/components/MailDetail.tsx#L6-L187
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/web/client/src/components/MailDetail/uploadAttachment
---

# Signature

`function MailDetail(props: { copy: ClientCopy; current: Message | null; mode: Mode; draft: MessageDraft; messageBusy: boolean; notice: string; composerMailboxes: MailboxAccountAccess[]; setDraft: React.Dispatch<React.SetStateAction<MessageDraft>>; onReply: (message: Message) => void; onForward: (message: Message) => void; onToggleFlag: (message: Message) => void; onCompleteFlag: (message: Message, completed: boolean) => void; onSetFlagDue: (message: Message, daysFromToday: number | null) => void; onSetFlagReminder: (message: Message, minutesFromNow: number | null) => void; onCancel: () => void; onSaveDraft: () => void; onSend: () => void; onDeleteDraft: () => void; draftMessageId: string | null; onUploadAttachment: (file: File) => Promise<void>; onOpenAttachment: (message: Message, attachment: Attachment) => Promise<void>; })`

# Calls

- [uploadAttachment](../../../../../../functions/web/client/src/components/MailDetail/uploadAttachment.md)