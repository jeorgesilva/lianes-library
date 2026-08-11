import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../lib/AuthContext";
import { Button } from "../components/Button";
import { Field, Input } from "../components/Input";
import { Card } from "../components/Card";

export function Login() {
  const { login, register } = useAuth();
  const navigate = useNavigate();
  const [mode, setMode] = useState<"login" | "register">("login");
  const [error, setError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [form, setForm] = useState({ first_name: "", last_name: "", email: "", password: "" });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setIsSubmitting(true);
    try {
      if (mode === "login") {
        await login(form.email, form.password);
      } else {
        await register(form);
      }
      navigate("/");
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="flex min-h-svh w-full items-center justify-center">
      <Card className="w-full max-w-sm">
        <h1 className="mb-1 text-xl font-bold text-text">📚 Liane's Library</h1>
        <p className="mb-6 text-sm text-text-muted">
          {mode === "login" ? "Entre com sua conta" : "Crie sua conta para ter sua própria biblioteca"}
        </p>

        <form className="flex flex-col gap-3" onSubmit={handleSubmit}>
          {mode === "register" && (
            <>
              <Field label="Nome">
                <Input required value={form.first_name} onChange={(e) => setForm({ ...form, first_name: e.target.value })} />
              </Field>
              <Field label="Sobrenome">
                <Input value={form.last_name} onChange={(e) => setForm({ ...form, last_name: e.target.value })} />
              </Field>
            </>
          )}
          <Field label="Email">
            <Input
              type="email"
              required
              value={form.email}
              onChange={(e) => setForm({ ...form, email: e.target.value })}
            />
          </Field>
          <Field label="Senha">
            <Input
              type="password"
              required
              minLength={8}
              value={form.password}
              onChange={(e) => setForm({ ...form, password: e.target.value })}
            />
          </Field>

          {error && <p className="text-sm text-danger">{error}</p>}

          <Button type="submit" disabled={isSubmitting} className="mt-2">
            {mode === "login" ? "Entrar" : "Criar conta"}
          </Button>
        </form>

        <button
          type="button"
          className="mt-4 text-sm text-text-muted underline hover:text-text"
          onClick={() => {
            setError(null);
            setMode(mode === "login" ? "register" : "login");
          }}
        >
          {mode === "login" ? "Não tem conta? Cadastre-se" : "Já tem conta? Entrar"}
        </button>
      </Card>
    </div>
  );
}
