import { useState } from "react";
import { NavLink } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useAuth } from "../lib/AuthContext";
import { api } from "../lib/api";
import { NotificationBell } from "./NotificationBell";

interface NavItem {
  to: string;
  label: string;
  icon: string;
  end?: boolean;
}

interface NavGroup {
  heading?: string;
  items: NavItem[];
}

const navGroups: NavGroup[] = [
  {
    heading: "Discover",
    items: [
      { to: "/", label: "Home", icon: "🍿", end: true },
      { to: "/recommendations", label: "Recommendations", icon: "✨" },
      { to: "/events", label: "Events", icon: "🎟️" },
    ],
  },
  { heading: "My Reading", items: [{ to: "/reading", label: "My Reading", icon: "📓" }] },
  {
    heading: "My Collection",
    items: [
      { to: "/catalog", label: "Book Catalog", icon: "📖" },
      { to: "/wishlist", label: "Wishlist", icon: "💜" },
    ],
  },
  {
    heading: "Loans",
    items: [
      { to: "/loans", label: "Loaned Out", icon: "🔄" },
      { to: "/borrowed-by-me", label: "Borrowed by Me", icon: "📦" },
    ],
  },
  { heading: "People", items: [{ to: "/borrowers", label: "Borrowers", icon: "👥" }] },
  { items: [{ to: "/assistant", label: "Smart Assistant", icon: "🧠" }] },
  { heading: "Insights", items: [{ to: "/analytics", label: "Analytics", icon: "📊" }] },
];

function NavGroups({ onNavigate }: { onNavigate?: () => void }) {
  return (
    <nav className="mt-8 flex flex-col gap-5">
      {navGroups.map((group, i) => (
        <div key={group.heading ?? i}>
          {group.heading && (
            <p className="mb-1 px-3 text-[10px] font-semibold uppercase tracking-wider text-text-muted/70">
              {group.heading}
            </p>
          )}
          <div className="flex flex-col gap-1">
            {group.items.map((link) => (
              <NavLink
                key={link.to}
                to={link.to}
                end={link.end}
                onClick={onNavigate}
                className={({ isActive }) =>
                  `rounded-lg px-3 py-2 text-sm transition-colors ${
                    isActive
                      ? "bg-primary/20 text-primary font-medium"
                      : "text-text-muted hover:bg-border/50 hover:text-text"
                  }`
                }
              >
                {link.icon} {link.label}
              </NavLink>
            ))}
          </div>
        </div>
      ))}
    </nav>
  );
}

function UserFooter() {
  const { user, logout } = useAuth();
  if (!user) return null;
  return (
    <div className="mt-8 border-t border-border pt-4">
      <p className="truncate text-sm text-text">
        {user.first_name} {user.last_name}
      </p>
      <p className="truncate text-xs text-text-muted">{user.email}</p>
      <button
        type="button"
        onClick={logout}
        className="mt-2 text-xs text-text-muted underline hover:text-text"
      >
        Sair
      </button>
    </div>
  );
}

export function Sidebar() {
  const [mobileOpen, setMobileOpen] = useState(false);
  const queryClient = useQueryClient();
  const { data: notifications } = useQuery({ queryKey: ["notifications"], queryFn: api.notifications.list });
  const markReadMutation = useMutation({
    mutationFn: (id: number) => api.notifications.markRead(id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["notifications"] }),
  });

  return (
    <>
      <header className="sticky top-0 z-30 flex items-center justify-between border-b border-border bg-bg/95 px-4 py-3 backdrop-blur sm:hidden">
        <button
          type="button"
          aria-label="Open menu"
          onClick={() => setMobileOpen(true)}
          className="rounded-lg p-2 text-text hover:bg-border/50"
        >
          ☰
        </button>
        <h1 className="font-display text-base font-bold text-text">📚 Liane's Library</h1>
        <NotificationBell notifications={notifications} onMarkRead={(id) => markReadMutation.mutate(id)} />
      </header>

      <aside className="hidden w-64 shrink-0 flex-col border-r border-border bg-surface/50 p-6 sm:flex">
        <div className="flex items-center justify-between gap-2">
          <div>
            <h1 className="font-display text-lg font-bold text-text">📚 Liane's Library</h1>
            <p className="mt-1 text-sm text-text-muted">Welcome to your smart book tracker!</p>
          </div>
          <NotificationBell notifications={notifications} onMarkRead={(id) => markReadMutation.mutate(id)} />
        </div>
        <NavGroups />
        <UserFooter />
      </aside>

      {mobileOpen && (
        <div className="fixed inset-0 z-40 sm:hidden">
          <div
            className="absolute inset-0 bg-black/60"
            onClick={() => setMobileOpen(false)}
            aria-hidden
          />
          <aside className="absolute inset-y-0 left-0 flex w-72 flex-col overflow-y-auto border-r border-border bg-surface p-6 shadow-xl">
            <div className="flex items-center justify-between">
              <h1 className="font-display text-lg font-bold text-text">📚 Liane's Library</h1>
              <button
                type="button"
                aria-label="Close menu"
                onClick={() => setMobileOpen(false)}
                className="rounded-lg p-2 text-text-muted hover:bg-border/50 hover:text-text"
              >
                ✕
              </button>
            </div>
            <NavGroups onNavigate={() => setMobileOpen(false)} />
            <UserFooter />
          </aside>
        </div>
      )}
    </>
  );
}
