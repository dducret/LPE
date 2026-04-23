import React from "react";
import { cn } from "../../lib/cn";

export type TextareaProps = React.TextareaHTMLAttributes<HTMLTextAreaElement>;

export const Textarea = React.forwardRef<HTMLTextAreaElement, TextareaProps>(function Textarea(
  { className, ...props },
  ref,
) {
  return (
    <textarea
      {...props}
      ref={ref}
      className={cn(
        "min-h-28 w-full rounded-[16px] border border-border bg-white/90 px-3.5 py-3 text-text shadow-[inset_0_1px_0_rgba(255,255,255,0.78)] outline-none transition-all duration-200 ease-spring placeholder:text-muted focus:border-primary/40 focus:ring-4 focus:ring-primary/10",
        className,
      )}
    />
  );
});
