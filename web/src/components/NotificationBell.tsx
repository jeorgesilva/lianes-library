import { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { EmptyState } from "./EmptyState";
import type { AppNotification, NotificationType } from "../lib/api";

const TYPE_ICON: Record<NotificationType, string> = {
  OVERDUE_LOAN: "🔄",
  PRICE_DROP: "💰",
  BORROW_DUE_SOON: "⏳",
  EVENT_NEARBY: "🎟️",
};

export function NotificationBell({
  notifications = [],
  onMarkRead,
}: {
  notifications?: AppNotification[];
  onMarkRead?: (id: number) => void;
}) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);
  const navigate = useNavigate();
  const unreadCount = notifications.filter((n) => !n.read_at).length;

  useEffect(() => {
    if (!open) return;
    const onClickOutside = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onClickOutside);
    return () => document.removeEventListener("mousedown", onClickOutside);
  }, [open]);

  return (
    <div className="relative" ref={rootRef}>
      <button
        type="button"
        aria-label="Notifications"
        onClick={() => setOpen((v) => !v)}
        className="relative rounded-lg p-2 text-text-muted transition-colors hover:bg-border/50 hover:text-text"
      >
        🔔
        {unreadCount > 0 && (
          <span className="absolute right-0.5 top-0.5 flex h-4 min-w-4 items-center justify-center rounded-full bg-danger px-1 text-[10px] font-bold text-white">
            {unreadCount > 9 ? "9+" : unreadCount}
          </span>
        )}
      </button>

      {open && (
        <div className="absolute right-0 z-20 mt-2 w-80 rounded-xl border border-border bg-surface-raised p-2 shadow-xl">
          <p className="px-2 py-1 text-sm font-semibold text-text">Notifications</p>
          {notifications.length === 0 ? (
            <EmptyState icon="🔔" title="You're all caught up" description="Nothing new to see here." />
          ) : (
            <ul className="max-h-96 overflow-y-auto">
              {notifications.map((n) => (
                <li key={n.notification_id}>
                  <button
                    type="button"
                    onClick={() => {
                      if (!n.read_at) onMarkRead?.(n.notification_id);
                      if (n.link) {
                        setOpen(false);
                        navigate(n.link);
                      }
                    }}
                    className={`flex w-full flex-col gap-0.5 rounded-lg px-2 py-2 text-left text-sm transition-colors hover:bg-surface-hover ${
                      n.read_at ? "opacity-60" : ""
                    }`}
                  >
                    <span className="font-medium text-text">
                      {TYPE_ICON[n.type]} {n.title}
                    </span>
                    {n.body && <span className="text-xs text-text-muted">{n.body}</span>}
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
