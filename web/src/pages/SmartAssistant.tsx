import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { api, type ChatMessage, type VibeSearchResult } from "../lib/api";
import { Button } from "../components/Button";
import { Input } from "../components/Input";
import { Card } from "../components/Card";
import { Tabs } from "../components/Tabs";

function VibeSearch() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<VibeSearchResult[] | null>(null);

  const mutation = useMutation({
    mutationFn: () => api.search.vibe(query, 3),
    onSuccess: (data) => setResults(data.results),
  });

  return (
    <div>
      <h3 className="mb-3 text-lg font-semibold">
        Find a book by <em>Vibe</em>
      </h3>
      <div className="mb-4 flex flex-col gap-2">
        <label className="text-sm text-text-muted">Describe what you want to read today:</label>
        <Input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && query && mutation.mutate()}
        />
        <Button
          type="button"
          className="w-fit"
          variant="secondary"
          disabled={!query || mutation.isPending}
          onClick={() => mutation.mutate()}
        >
          {mutation.isPending ? "Analyzing the vibes..." : "Search by Vibe"}
        </Button>
      </div>

      {mutation.isError && <p className="text-danger">{(mutation.error as Error).message}</p>}
      {results && results.length === 0 && <p className="text-text-muted">No books matched that vibe.</p>}

      <div className="flex flex-col gap-3">
        {results?.map((book) => (
          <Card key={book.book_id}>
            <div className="flex items-start justify-between">
              <strong className="text-text">📖 {book.title}</strong>
              <span className="shrink-0 rounded-full bg-accent/20 px-2 py-0.5 text-xs text-accent">
                {Math.round(book.relevance_score * 100)}% Vibe Match
              </span>
            </div>
            <div className="mt-1 text-sm font-medium text-accent-2">{book.author}</div>
            <p className="mt-2 text-sm text-text-muted">{book.content_summary}</p>
          </Card>
        ))}
      </div>
    </div>
  );
}

function ChatWithLibrarian() {
  const [messages, setMessages] = useState<ChatMessage[]>([
    { role: "assistant", content: "Hi! Ask me anything about Liane's book collection." },
  ]);
  const [input, setInput] = useState("");

  const mutation = useMutation({
    mutationFn: (message: string) => api.chat.ask(message),
    onSuccess: (data) => setMessages((m) => [...m, { role: "assistant", content: data.reply }]),
    onError: (err: Error) => setMessages((m) => [...m, { role: "assistant", content: `❌ ${err.message}` }]),
  });

  const send = () => {
    if (!input.trim() || mutation.isPending) return;
    setMessages((m) => [...m, { role: "user", content: input }]);
    mutation.mutate(input);
    setInput("");
  };

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-col gap-2">
        {messages.map((m, i) => (
          <div
            key={i}
            className={`max-w-[80%] rounded-lg px-3 py-2 text-sm ${
              m.role === "user" ? "self-end bg-primary text-white" : "self-start bg-surface text-text"
            }`}
          >
            {m.content}
          </div>
        ))}
        {mutation.isPending && <div className="self-start text-sm text-text-muted">Teacher is thinking...</div>}
      </div>
      <div className="flex gap-2">
        <Input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && send()}
          placeholder="Ask the librarian..."
        />
        <Button type="button" onClick={send} disabled={mutation.isPending}>
          Send
        </Button>
      </div>
    </div>
  );
}

export function SmartAssistant() {
  return (
    <div>
      <h1 className="mb-1 text-2xl font-bold text-text">🧠 Smart Library Assistant</h1>
      <p className="mb-6 text-text-muted">
        Use the power of AI to find books that match your vibe or ask the librarian anything about your collection!
      </p>
      <Tabs
        tabs={[
          { label: "✨ Vibe Search", content: <VibeSearch /> },
          { label: "💬 Chat with Librarian", content: <ChatWithLibrarian /> },
        ]}
      />
    </div>
  );
}
