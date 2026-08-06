import type { InputHTMLAttributes, LabelHTMLAttributes, ReactNode } from "react";

export function Input({ className = "", ...props }: InputHTMLAttributes<HTMLInputElement>) {
  return (
    <input
      className={`w-full rounded-lg border border-border bg-surface px-3 py-2 text-sm text-text placeholder:text-text-muted focus:outline-none focus:ring-2 focus:ring-primary ${className}`}
      {...props}
    />
  );
}

export function Field({
  label,
  children,
  ...props
}: LabelHTMLAttributes<HTMLLabelElement> & { label: string; children: ReactNode }) {
  return (
    <label className="flex flex-col gap-1 text-sm text-text-muted" {...props}>
      {label}
      {children}
    </label>
  );
}
