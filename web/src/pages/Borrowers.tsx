import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../lib/api";
import { Button } from "../components/Button";
import { Field, Input } from "../components/Input";
import { Card } from "../components/Card";

export function Borrowers() {
  const queryClient = useQueryClient();
  const { data: borrowers, isLoading } = useQuery({ queryKey: ["borrowers"], queryFn: api.borrowers.list });
  const [form, setForm] = useState({ first_name: "", last_name: "", email: "", phone_number: "" });

  const mutation = useMutation({
    mutationFn: () => api.borrowers.create(form),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["borrowers"] });
      setForm({ first_name: "", last_name: "", email: "", phone_number: "" });
    },
  });

  return (
    <div>
      <h1 className="mb-6 text-2xl font-bold text-text">👥 Borrowers</h1>

      <Card className="mb-6 max-w-sm">
        <h2 className="mb-3 font-semibold">➕ Add New Borrower</h2>
        <form
          className="flex flex-col gap-3"
          onSubmit={(e) => {
            e.preventDefault();
            mutation.mutate();
          }}
        >
          <Field label="First name">
            <Input required value={form.first_name} onChange={(e) => setForm({ ...form, first_name: e.target.value })} />
          </Field>
          <Field label="Last name">
            <Input required value={form.last_name} onChange={(e) => setForm({ ...form, last_name: e.target.value })} />
          </Field>
          <Field label="Email">
            <Input type="email" value={form.email} onChange={(e) => setForm({ ...form, email: e.target.value })} />
          </Field>
          <Field label="Phone">
            <Input value={form.phone_number} onChange={(e) => setForm({ ...form, phone_number: e.target.value })} />
          </Field>
          <Button type="submit" disabled={mutation.isPending} className="w-fit">
            Add Borrower
          </Button>
        </form>
      </Card>

      {!isLoading && (
        <table className="w-full text-left text-sm">
          <thead className="text-text-muted">
            <tr>
              <th className="pb-2">ID</th>
              <th className="pb-2">First name</th>
              <th className="pb-2">Last name</th>
              <th className="pb-2">Status</th>
            </tr>
          </thead>
          <tbody>
            {borrowers?.map((b) => (
              <tr key={b.person_id} className="border-t border-border">
                <td className="py-2">{b.person_id}</td>
                <td className="py-2">{b.first_name}</td>
                <td className="py-2">{b.last_name}</td>
                <td className="py-2">{b.status}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
