import React from "react";
import { cn } from "../../lib/cn";

export function Card(props: React.HTMLAttributes<HTMLElement> & { as?: "article" | "section" | "div"; variant?: "default" | "soft" | "premium" }) {
  const { as = "section", className, variant = "default", ...rest } = props;
  const Component = as;
  const variantClass =
    variant === "soft"
      ? "bg-surface-soft"
      : variant === "premium"
        ? "bg-surface-premium"
        : "bg-surface-strong";
  return (
    <Component
      {...rest}
      className={cn(
        "rounded-[22px] border border-border p-5 shadow-panel backdrop-blur-premium",
        variantClass,
        className,
      )}
    />
  );
}
