const API_URL = import.meta.env.VITE_API_URL as string;

export interface Book {
  book_id: number;
  ISBN: string | null;
  title: string;
  author: string | null;
  author_id: number | null;
  genre: string | null;
  cost_book: number | null;
  book_status: "AVAILABLE" | "BORROWED" | "LOST" | "DAMAGED";
  date_added: string;
  cover_url: string | null;
}

export interface Borrower {
  person_id: number;
  first_name: string;
  last_name: string;
  relationship_type: string | null;
  phone_number: string | null;
  email: string | null;
  address: string | null;
  status: "ACTIVE" | "INACTIVE";
  date_joined: string;
}

export interface ActiveLoan {
  transaction_id: number;
  book_id: number;
  book_title: string;
  person_id: number;
  borrower_name: string;
  loan_date: string;
  due_date: string;
  days_overdue: number;
}

export interface VibeSearchResult {
  book_id: number;
  title: string;
  author: string;
  content_summary: string;
  relevance_score: number;
}

export interface VibeSearchResponse {
  query: string;
  count: number;
  results: VibeSearchResult[];
}

export interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_URL}${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...init?.headers },
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail ?? `Request failed: ${res.status}`);
  }
  return res.json();
}

export const api = {
  books: {
    list: (params?: { title?: string; author?: string; status?: string; limit?: number }) => {
      const qs = new URLSearchParams();
      if (params?.title) qs.set("title", params.title);
      if (params?.author) qs.set("author", params.author);
      if (params?.status) qs.set("status", params.status);
      if (params?.limit) qs.set("limit", String(params.limit));
      const suffix = qs.toString() ? `?${qs}` : "";
      return request<Book[]>(`/books/${suffix}`);
    },
    create: (payload: { title: string; author?: string; isbn?: string; cost?: number; cover_url?: string }) =>
      request<Book>("/books/", { method: "POST", body: JSON.stringify(payload) }),
  },
  borrowers: {
    list: () => request<Borrower[]>("/borrowers/"),
    create: (payload: { first_name: string; last_name: string; email?: string; phone_number?: string }) =>
      request<Borrower>("/borrowers/", { method: "POST", body: JSON.stringify(payload) }),
  },
  loans: {
    active: () => request<ActiveLoan[]>("/loans/active"),
    create: (payload: { book_id: number; person_id: number; loan_period_days?: number }) =>
      request<unknown>("/loans/", { method: "POST", body: JSON.stringify(payload) }),
    return: (transactionId: number) =>
      request<unknown>(`/loans/${transactionId}/return`, { method: "POST", body: JSON.stringify({}) }),
  },
  search: {
    vibe: (q: string, limit = 5) =>
      request<VibeSearchResponse>(`/search/vibe?q=${encodeURIComponent(q)}&limit=${limit}`),
  },
  chat: {
    ask: (message: string) => request<{ reply: string }>("/chat/", { method: "POST", body: JSON.stringify({ message }) }),
  },
  openLibrary: {
    lookup: async (isbn: string) => {
      const res = await fetch(
        `https://openlibrary.org/api/books?bibkeys=ISBN:${isbn}&format=json&jscmd=data`
      );
      const data = await res.json();
      const book = data[`ISBN:${isbn}`];
      if (!book) return null;
      return {
        title: book.title as string,
        author: (book.authors?.[0]?.name as string) ?? "Unknown author",
        cover_url: book.cover?.large as string | undefined,
      };
    },
  },
};
