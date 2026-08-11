import { useState, type ReactNode } from "react";

export function Tabs({
  tabs,
  active: activeProp,
  onChange,
}: {
  tabs: { label: string; content: ReactNode }[];
  active?: number;
  onChange?: (index: number) => void;
}) {
  const [internalActive, setInternalActive] = useState(0);
  const active = activeProp ?? internalActive;
  const setActive = onChange ?? setInternalActive;

  return (
    <div>
      <div className="mb-4 flex gap-1 border-b border-border">
        {tabs.map((tab, i) => (
          <button
            key={tab.label}
            onClick={() => setActive(i)}
            className={`cursor-pointer border-b-2 px-4 py-2 text-sm font-medium transition-colors ${
              active === i
                ? "border-accent text-accent"
                : "border-transparent text-text-muted hover:text-text"
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>
      {tabs[active].content}
    </div>
  );
}
