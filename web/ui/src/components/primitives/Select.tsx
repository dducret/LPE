import React from "react";
import { cn } from "../../lib/cn";

export type SelectProps = React.SelectHTMLAttributes<HTMLSelectElement>;

export const Select = React.forwardRef<HTMLSelectElement, SelectProps>(function Select(
  { className, ...props },
  ref,
) {
  return (
    <select
      {...props}
      ref={ref}
      className={cn(
        "min-h-12 w-full rounded-[16px] border border-border bg-white/90 px-3.5 py-3 text-text shadow-[inset_0_1px_0_rgba(255,255,255,0.78)] outline-none transition-all duration-200 ease-spring focus:border-primary/40 focus:ring-4 focus:ring-primary/10",
        className,
      )}
    />
  );
});
