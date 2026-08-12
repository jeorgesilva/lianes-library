import type { ButtonHTMLAttributes } from "react";

type Variant = "primary" | "secondary" | "danger";

const variantClasses: Record<Variant, string> = {
  primary: "bg-primary text-on-primary hover:bg-primary/90",
  secondary: "bg-surface text-text border border-border hover:bg-border",
  danger: "bg-danger text-on-primary hover:bg-danger/90",
};

export function Button({
  variant = "primary",
  className = "",
  ...props
}: ButtonHTMLAttributes<HTMLButtonElement> & { variant?: Variant }) {
  return (
    <button
      className={`rounded-lg px-4 py-2 text-sm font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer ${variantClasses[variant]} ${className}`}
      {...props}
    />
  );
}
