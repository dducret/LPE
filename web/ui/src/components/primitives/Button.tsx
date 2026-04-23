import React from "react";
import { cn } from "../../lib/cn";

type ButtonVariant = "primary" | "secondary" | "ghost" | "danger";
type ButtonSize = "sm" | "md";

const variantClasses: Record<ButtonVariant, string> = {
  primary: "border-transparent bg-gradient-to-br from-primary to-primary-strong text-white shadow-[0_12px_24px_rgba(46,111,242,0.2)] hover:-translate-y-px",
  secondary: "border-border bg-surface-strong text-text shadow-panel hover:-translate-y-px hover:border-border-strong",
  ghost: "border-border bg-white/60 text-text shadow-none hover:-translate-y-px hover:bg-white/80",
  danger: "border-danger/20 bg-danger-soft text-danger shadow-none hover:-translate-y-px hover:border-danger/30",
};

const sizeClasses: Record<ButtonSize, string> = {
  sm: "min-h-9 px-3.5 text-sm",
  md: "min-h-11 px-4 text-sm",
};

export type ButtonProps = React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: ButtonVariant;
  size?: ButtonSize;
};

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  { className, variant = "secondary", size = "md", type = "button", ...props },
  ref,
) {
  return (
    <button
      {...props}
      ref={ref}
      type={type}
      className={cn(
        "inline-flex items-center justify-center gap-2 rounded-[16px] border font-medium transition-all duration-200 ease-spring disabled:cursor-not-allowed disabled:opacity-60",
        sizeClasses[size],
        variantClasses[variant],
        className,
      )}
    />
  );
});
