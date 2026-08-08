import { NavLink } from "react-router-dom";
import { useAuth } from "../lib/AuthContext";

const links = [
  { to: "/", label: "Dashboard", icon: "🍿", end: true },
  { to: "/assistant", label: "Smart Assistant", icon: "🧠" },
  { to: "/catalog", label: "Book Catalog", icon: "📖" },
  { to: "/borrowers", label: "Borrowers", icon: "👥" },
  { to: "/loans", label: "Loans", icon: "🔄" },
  { to: "/analytics", label: "Analytics", icon: "📊" },
];

export function Sidebar() {
  const { user, logout } = useAuth();

  return (
    <aside className="w-64 shrink-0 border-r border-border bg-surface/50 p-6">
      <h1 className="text-lg font-bold text-text">📚 Liane's Library</h1>
      <p className="mt-1 text-sm text-text-muted">Welcome to your smart book tracker!</p>
      <nav className="mt-8 flex flex-col gap-1">
        {links.map((link) => (
          <NavLink
            key={link.to}
            to={link.to}
            end={link.end}
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
      </nav>

      {user && (
        <div className="mt-8 border-t border-border pt-4">
          <p className="truncate text-sm text-text">{user.first_name} {user.last_name}</p>
          <p className="truncate text-xs text-text-muted">{user.email}</p>
          <button
            type="button"
            onClick={logout}
            className="mt-2 text-xs text-text-muted underline hover:text-text"
          >
            Sair
          </button>
        </div>
      )}
    </aside>
  );
}
