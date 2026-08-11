import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../lib/api";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import { Field, Input } from "../components/Input";
import { BarcodeScanner } from "../components/BarcodeScanner";

function Checkout() {
  const queryClient = useQueryClient();
  const { data: books } = useQuery({ queryKey: ["books"], queryFn: () => api.books.list() });
  const { data: borrowers } = useQuery({ queryKey: ["borrowers"], queryFn: api.borrowers.list });

  const [isbnOrId, setIsbnOrId] = useState("");
  const [personId, setPersonId] = useState<number | "">("");
  const [days, setDays] = useState(14);
  const [message, setMessage] = useState<{ type: "success" | "error"; text: string } | null>(null);

  const mutation = useMutation({
    mutationFn: () => {
      const book = books?.find((b) => b.ISBN === isbnOrId || String(b.book_id) === isbnOrId);
      const bookId = book?.book_id ?? (/^\d+$/.test(isbnOrId) ? Number(isbnOrId) : null);
      if (!bookId || !personId) throw new Error("Book not found in inventory or invalid borrower.");
      return api.loans.create({ book_id: bookId, person_id: Number(personId), loan_period_days: days });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["loans"] });
      queryClient.invalidateQueries({ queryKey: ["books"] });
      setMessage({ type: "success", text: "Loan registered!" });
      setIsbnOrId("");
    },
    onError: (e: Error) => setMessage({ type: "error", text: e.message }),
  });

  return (
    <Card>
      <h2 className="mb-3 font-semibold">📤 Loan</h2>
      <BarcodeScanner onDecode={setIsbnOrId} />
      <form
        className="mt-4 flex flex-col gap-3"
        onSubmit={(e) => {
          e.preventDefault();
          mutation.mutate();
        }}
      >
        <Field label="ISBN or Book ID">
          <Input value={isbnOrId} onChange={(e) => setIsbnOrId(e.target.value)} />
        </Field>
        <Field label="Borrower">
          <select
            className="w-full rounded-lg border border-border bg-surface px-3 py-2 text-sm text-text"
            value={personId}
            onChange={(e) => setPersonId(e.target.value ? Number(e.target.value) : "")}
          >
            <option value="">Select...</option>
            {borrowers?.map((b) => (
              <option key={b.person_id} value={b.person_id}>
                {b.first_name} {b.last_name}
              </option>
            ))}
          </select>
        </Field>
        <Field label="Loan period (days)">
          <Input type="number" min={1} value={days} onChange={(e) => setDays(Number(e.target.value))} />
        </Field>
        <Button type="submit" disabled={mutation.isPending}>
          Register Loan
        </Button>
        {message && (
          <p className={message.type === "success" ? "text-success text-sm" : "text-danger text-sm"}>{message.text}</p>
        )}
      </form>
    </Card>
  );
}

function Return() {
  const queryClient = useQueryClient();
  const { data: books } = useQuery({ queryKey: ["books"], queryFn: () => api.books.list() });
  const { data: activeLoans } = useQuery({ queryKey: ["loans", "active"], queryFn: api.loans.active });

  const [input, setInput] = useState("");
  const [message, setMessage] = useState<{ type: "success" | "error"; text: string } | null>(null);

  const mutation = useMutation({
    mutationFn: () => {
      let transactionId: number | null = null;
      if (/^\d+$/.test(input) && input.length < 10) {
        transactionId = Number(input);
      } else {
        const book = books?.find((b) => b.ISBN === input);
        const loan = activeLoans?.find((l) => l.book_title === book?.title);
        transactionId = loan?.transaction_id ?? null;
      }
      if (!transactionId) throw new Error("No active loan found for this book.");
      return api.loans.return(transactionId);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["loans"] });
      queryClient.invalidateQueries({ queryKey: ["books"] });
      setMessage({ type: "success", text: "Book returned successfully!" });
      setInput("");
    },
    onError: (e: Error) => setMessage({ type: "error", text: e.message }),
  });

  return (
    <Card>
      <h2 className="mb-3 font-semibold">📥 Return</h2>
      <BarcodeScanner onDecode={setInput} />
      <form
        className="mt-4 flex flex-col gap-3"
        onSubmit={(e) => {
          e.preventDefault();
          mutation.mutate();
        }}
      >
        <Field label="Book ISBN or Transaction ID">
          <Input value={input} onChange={(e) => setInput(e.target.value)} />
        </Field>
        <Button type="submit" disabled={mutation.isPending}>
          Register Return
        </Button>
        {message && (
          <p className={message.type === "success" ? "text-success text-sm" : "text-danger text-sm"}>{message.text}</p>
        )}
      </form>
    </Card>
  );
}

export function Loans() {
  return (
    <div>
      <h1 className="mb-6 text-2xl font-bold text-text">🔄 Loan Management</h1>
      <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
        <Checkout />
        <Return />
      </div>
    </div>
  );
}
