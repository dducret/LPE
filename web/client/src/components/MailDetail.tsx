import React from "react";
import type { ClientCopy } from "../i18n";
import type { MailboxAccountAccess, Message, MessageDraft, Mode } from "../client-types";

export function MailDetail(props: {
  copy: ClientCopy;
  current: Message | null;
  mode: Mode;
  draft: MessageDraft;
  composerMailboxes: MailboxAccountAccess[];
  setDraft: React.Dispatch<React.SetStateAction<MessageDraft>>;
  onReply: (message: Message) => void;
  onForward: (message: Message) => void;
  onCancel: () => void;
  onSaveDraft: () => void;
  onSend: () => void;
  onDeleteDraft: () => void;
}) {
  if (props.mode !== "closed") {
    const selectedMailbox =
      props.composerMailboxes.find((entry) => entry.accountId === props.draft.mailboxAccountId)
      ?? props.composerMailboxes[0]
      ?? null;
    const delegatedModeOptions = selectedMailbox
      ? [
          ...(selectedMailbox.maySendAs ? [{ value: "send_as" as const, label: props.copy.senderModes.sendAs }] : []),
          ...(selectedMailbox.maySendOnBehalf ? [{ value: "send_on_behalf" as const, label: props.copy.senderModes.sendOnBehalf }] : [])
        ]
      : [];
    const showSenderMode = selectedMailbox && !selectedMailbox.isOwned;

    return (
      <section className="editor-shell">
        <div className="editor-shell-header">
          <div>
            <p className="detail-label">Compose drawer</p>
            <h3>{props.copy.editorTitles[props.mode]}</h3>
            <p className="editor-shell-copy">Client composition stays aligned with the canonical LPE submission model.</p>
          </div>
          <button className="ghost-button" type="button" onClick={props.onCancel}>{props.copy.editorActions.cancel}</button>
        </div>

        <div className="form-grid">
          <label className="field field-wide">
            <span>{props.copy.fields.from}</span>
            <select
              value={props.draft.mailboxAccountId}
              onChange={(event) => props.setDraft((value) => ({ ...value, mailboxAccountId: event.target.value }))}
            >
              {props.composerMailboxes.map((mailbox) => (
                <option key={mailbox.accountId} value={mailbox.accountId}>
                  {`${mailbox.displayName} <${mailbox.email}>`}
                </option>
              ))}
            </select>
          </label>
          {showSenderMode ? (
            <label className="field field-wide">
              <span>{props.copy.fields.senderMode}</span>
              <select
                value={props.draft.senderMode}
                onChange={(event) => props.setDraft((value) => ({
                  ...value,
                  senderMode: event.target.value as MessageDraft["senderMode"]
                }))}
                disabled={delegatedModeOptions.length <= 1}
              >
                {delegatedModeOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
          ) : null}
          <label className="field field-wide">
            <span>{props.copy.fields.to}</span>
            <input value={props.draft.to} onChange={(event) => props.setDraft((value) => ({ ...value, to: event.target.value }))} />
          </label>
          <label className="field field-wide">
            <span>{props.copy.fields.cc}</span>
            <input value={props.draft.cc} onChange={(event) => props.setDraft((value) => ({ ...value, cc: event.target.value }))} />
          </label>
          <label className="field field-wide">
            <span>{props.copy.fields.subject}</span>
            <input value={props.draft.subject} onChange={(event) => props.setDraft((value) => ({ ...value, subject: event.target.value }))} />
          </label>
          <label className="field field-wide">
            <span>{props.copy.fields.body}</span>
            <textarea rows={14} value={props.draft.body} onChange={(event) => props.setDraft((value) => ({ ...value, body: event.target.value }))} />
          </label>
        </div>

        <div className="editor-shell-actions">
          {props.mode === "draft" ? <button className="danger-button" type="button" onClick={props.onDeleteDraft}>{props.copy.editorActions.deleteDraft}</button> : null}
          <button className="ghost-button" type="button" onClick={props.onSaveDraft}>{props.copy.editorActions.saveDraft}</button>
          <button className="primary-button" type="button" onClick={props.onSend}>{props.copy.editorActions.send}</button>
        </div>
      </section>
    );
  }

  if (!props.current) {
    return (
      <section className="reading-empty-state">
        <p className="detail-label">{props.copy.readingPane}</p>
        <h3>Select a message</h3>
        <p>The detailed reading pane stays hidden until a message is selected from the list.</p>
      </section>
    );
  }

  const current = props.current;

  return (
    <article className="reading-pane-card">
      <div className="detail-header">
        <div><p className="detail-label">{props.copy.readingPane}</p><h3>{current.subject}</h3></div>
        <div className="detail-actions">
          <button className="ghost-button" type="button" onClick={() => props.onReply(current)}>{props.copy.messageActions.reply}</button>
          <button className="ghost-button" type="button" onClick={() => props.onForward(current)}>{props.copy.messageActions.forward}</button>
        </div>
      </div>

      <div className="sender-card"><div className="sender-avatar">{current.from.slice(0, 2).toUpperCase()}</div><div><strong>{current.from}</strong><p>{current.fromAddress}</p><span>{props.copy.fields.to}: {current.to}</span></div><span>{current.receivedAt}</span></div>
      <div className="tag-row">{current.tags.map((tag) => <span className="tag-pill" key={tag}>{tag}</span>)}</div>
      <article className="message-body">
        {current.body.map((paragraph) => <p key={paragraph}>{paragraph}</p>)}
      </article>
      <section className="attachment-panel"><div className="pane-header compact"><div><p className="pane-kicker">{props.copy.attachmentsTitle}</p><h4>{props.copy.attachmentsSubtitle}</h4></div></div><div className="attachment-list">{current.attachments.length > 0 ? current.attachments.map((item) => <article className="attachment-card" key={item.id}><span className="attachment-kind">{item.kind}</span><div><strong>{item.name}</strong><p>{item.size}</p></div></article>) : <div className="empty-state compact">{props.copy.noAttachments}</div>}</div></section>
    </article>
  );
}
