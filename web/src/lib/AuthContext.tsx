import { createContext, useContext, useState, type ReactNode } from "react";
import { api, auth, type AuthUser } from "./api";

interface AuthContextValue {
  user: AuthUser | null;
  login: (email: string, password: string) => Promise<void>;
  register: (payload: { first_name: string; last_name?: string; email: string; password: string }) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(() => auth.getUser());

  const login = async (email: string, password: string) => {
    const res = await api.auth.login({ email, password });
    auth.setSession(res.access_token, res.user);
    setUser(res.user);
  };

  const register = async (payload: { first_name: string; last_name?: string; email: string; password: string }) => {
    const res = await api.auth.register(payload);
    auth.setSession(res.access_token, res.user);
    setUser(res.user);
  };

  const logout = () => {
    auth.clearSession();
    setUser(null);
  };

  return <AuthContext.Provider value={{ user, login, register, logout }}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within an AuthProvider");
  return ctx;
}
