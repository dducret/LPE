import React from "react";
import type { ClientCopy } from "../i18n";
import type { Message, MessageDraft, Mode } from "../client-types";

export function MailDetail(props: {
  copy: ClientCopy;
  current: Message | null;
  mode: Mode;
  draft: MessageDraft;
  setDraft: React.Dispatch<React.SetStateAction<MessageDraft>>;
  onReply: (message: Message) => void;
  onForward: (message: Message) => void;
  onCancel: () => void;
  onSaveDraft: () => void;
  onSend: () => void;
}) {
  if (props.mode !== "closed") {
    return <section className="editor-shell"><div className="detail-header"><div><p className="detail-label">{props.copy.editorLabel}</p><h3>{props.copy.editorTitles[props.mode]}</h3></div><div className="detail-actions"><button className="ghost-button" type="button" onClick={props.onSaveDraft}>{props.copy.editorActions.saveDraft}</button><button className="ghost-button" type="button" onClick={props.onCancel}>{props.copy.editorActions.cancel}</button><button className="primary-button" type="button" onClick={props.onSend}>{props.copy.editorActions.send}</button></div></div><div className="form-grid"><label className="field"><span>{props.copy.fields.to}</span><input value={props.draft.to} onChange={(event) => props.setDraft((value) => ({ ...value, to: event.target.value }))} /></label><label className="field"><span>{props.copy.fields.cc}</span><input value={props.draft.cc} onChange={(event) => props.setDraft((value) => ({ ...value, cc: event.target.value }))} /></label><label className="field field-wide"><span>{props.copy.fields.subject}</span><input value={props.draft.subject} onChange={(event) => props.setDraft((value) => ({ ...value, subject: event.target.value }))} /></label><label className="field field-wide"><span>{props.copy.fields.body}</span><textarea rows={14} value={props.draft.body} onChange={(event) => props.setDraft((value) => ({ ...value, body: event.target.value }))} /></label></div></section>;
  }

  if (!props.current) return <div className="empty-state">{props.copy.noMessages}</div>;

  return (
    <>
      <div className="reading-titlebar">{props.copy.rightPaneTitle}</div>
      <div className="detail-header">
        <div><p className="detail-label">{props.copy.readingPane}</p><h3>{props.current.subject}</h3></div>
        <div className="detail-actions">
          <button className="ghost-button" type="button" onClick={() => props.onReply(props.current!)}>{props.copy.messageActions.reply}</button>
          <button className="ghost-button" type="button" onClick={() => props.onForward(props.current!)}>{props.copy.messageActions.forward}</button>
          <button className="ghost-button" type="button">{props.copy.messageActions.archive}</button>
        </div>
      </div>

      <div className="sender-card"><div className="sender-avatar">{props.current.from.slice(0, 2).toUpperCase()}</div><div><strong>{props.current.from}</strong><p>{props.current.fromAddress}</p><span>{props.copy.fields.to}: {props.current.to}</span></div><span>{props.current.receivedAt}</span></div>
      <div className="tag-row">{props.current.tags.map((tag) => <span className="tag-pill" key={tag}>{tag}</span>)}</div>
      <article className="message-body">
        {props.current.body.map((paragraph) => <p key={paragraph}>{paragraph}</p>)}
      </article>
      <section className="attachment-panel"><div className="pane-header compact"><div><p className="pane-kicker">{props.copy.attachmentsTitle}</p><h4>{props.copy.attachmentsSubtitle}</h4></div></div><div className="attachment-list">{props.current.attachments.length > 0 ? props.current.attachments.map((item) => <article className="attachment-card" key={item.id}><span className="attachment-kind">{item.kind}</span><div><strong>{item.name}</strong><p>{item.size}</p></div></article>) : <div className="empty-state compact">{props.copy.noAttachments}</div>}</div></section>
    </>
  );
}
