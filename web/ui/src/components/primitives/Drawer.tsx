import React from "react";
import { cn } from "../../lib/cn";

type DrawerProps = {
  open: boolean;
  onClose: () => void;
  title?: string;
  children: React.ReactNode;
  className?: string;
};

export function Drawer(props: DrawerProps) {
  if (!props.open) return null;
  const { className } = props;

  return (
    <div className="fixed inset-0 z-40 flex justify-end bg-slate-950/35 backdrop-blur-sm" onClick={props.onClose} role="presentation">
      <aside
        aria-modal="true"
        role="dialog"
        aria-label={props.title}
        onClick={(event) => event.stopPropagation()}
        className={cn(
          "m-4 h-[calc(100vh-2rem)] w-[min(28rem,calc(100vw-2rem))] overflow-auto rounded-[24px] border border-border bg-white p-5 shadow-soft",
          className,
        )}
      >
        {props.children}
      </aside>
    </div>
  );
}
