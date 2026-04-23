import React from "react";
import { cn } from "../../lib/cn";

export function Tabs(props: React.HTMLAttributes<HTMLDivElement>) {
  const { className, ...rest } = props;
  return <div {...rest} className={cn("flex flex-wrap gap-2", className)} />;
}

export function TabButton(props: React.ButtonHTMLAttributes<HTMLButtonElement> & { active?: boolean }) {
  const { active = false, className, type = "button", ...rest } = props;
  return (
    <button
      {...rest}
      type={type}
      className={cn(
        "inline-flex min-h-11 items-center justify-center rounded-[16px] border px-4 text-sm font-medium transition-all duration-200 ease-spring",
        active
          ? "border-primary/20 bg-white text-text shadow-panel"
          : "border-border bg-white/70 text-muted shadow-panel hover:-translate-y-px hover:border-border-strong",
        className,
      )}
    />
  );
}
