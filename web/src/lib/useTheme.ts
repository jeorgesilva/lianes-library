import { useCallback, useEffect, useState } from "react";

export type Theme = "light" | "dark";

const STORAGE_KEY = "lianes_theme";

function getInitialTheme(): Theme {
  if (typeof window === "undefined") return "light";
  const stored = window.localStorage.getItem(STORAGE_KEY);
  if (stored === "light" || stored === "dark") return stored;
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

function applyTheme(theme: Theme) {
  document.documentElement.classList.toggle("dark", theme === "dark");
  document.documentElement.style.colorScheme = theme;
}

/**
 * Cozy Minimalist light (cream/ivory) <-> sepia/coffee dark theme switch.
 * Applies a `.dark` class to <html> — index.css re-points every `--color-*`
 * token under that selector, so components never need `dark:` variants,
 * they just keep using bg-surface/text-text/etc as normal.
 *
 * A tiny inline script in index.html applies the same class before first
 * paint (avoids a flash of the wrong theme); this hook takes over after
 * hydration and is the only way to *change* it.
 *
 * Single consumer today (ThemeToggle, mounted once in Sidebar). If more
 * components need to reactively read the current theme, promote this to a
 * React Context so all instances share one state value — right now every
 * useTheme() call has its own useState, which only stays visually in sync
 * because they all write to the same DOM class.
 */
export function useTheme() {
  const [theme, setThemeState] = useState<Theme>(getInitialTheme);

  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  const setTheme = useCallback((next: Theme) => {
    window.localStorage.setItem(STORAGE_KEY, next);
    setThemeState(next);
  }, []);

  const toggleTheme = useCallback(() => {
    setTheme(theme === "dark" ? "light" : "dark");
  }, [theme, setTheme]);

  return { theme, setTheme, toggleTheme };
}
