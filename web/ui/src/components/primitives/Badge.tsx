import React from "react";
import { cn } from "../../lib/cn";

type BadgeVariant = "info" | "success" | "warning" | "danger" | "neutral";

const variantClasses: Record<BadgeVariant, string> = {
  info: "bg-primary-soft text-primary",
  success: "bg-success-soft text-success",
  warning: "bg-warning-soft text-warning",
  danger: "bg-danger-soft text-danger",
  neutral: "bg-surface-soft text-muted",
};

export function Badge(props: React.HTMLAttributes<HTMLSpanElement> & { variant?: BadgeVariant }) {
  const { className, variant = "neutral", ...rest } = props;
  return (
    <span
      {...rest}
      className={cn(
        "inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-xs font-semibold",
        variantClasses[variant],
        className,
      )}
    />
  );
}
