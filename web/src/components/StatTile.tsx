type Tone = "neutral" | "danger" | "success";

const toneClasses: Record<Tone, string> = {
  neutral: "text-text",
  danger: "text-danger",
  success: "text-success",
};

export function StatTile({
  label,
  value,
  tone = "neutral",
}: {
  label: string;
  value: string | number;
  tone?: Tone;
}) {
  return (
    <div className="flex flex-col justify-center gap-1 rounded-2xl bg-surface-hover/60 px-4 py-3">
      <span className={`font-sans text-2xl font-semibold leading-none tabular-nums sm:text-3xl ${toneClasses[tone]}`}>
        {value}
      </span>
      <span className="text-[11px] text-text-muted">{label}</span>
    </div>
  );
}
