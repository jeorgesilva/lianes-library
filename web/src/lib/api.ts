const API_URL = import.meta.env.VITE_API_URL as string;

const TOKEN_KEY = "lianes_token";
const USER_KEY = "lianes_user";

export interface AuthUser {
  user_id: number;
  first_name: string;
  last_name: string | null;
  email: string;
}

export const auth = {
  getToken: () => localStorage.getItem(TOKEN_KEY),
  getUser: (): AuthUser | null => {
    const raw = localStorage.getItem(USER_KEY);
    return raw ? (JSON.parse(raw) as AuthUser) : null;
  },
  setSession: (token: string, user: AuthUser) => {
    localStorage.setItem(TOKEN_KEY, token);
    localStorage.setItem(USER_KEY, JSON.stringify(user));
  },
  clearSession: () => {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(USER_KEY);
  },
};

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

export type ReadingStatus = "WANT_TO_READ" | "READING" | "READ" | "DNF";

export interface ReadingLogItem {
  reading_log_id: number;
  book_id: number | null;
  title: string;
  author: string | null;
  cover_url: string | null;
  status: ReadingStatus;
  started_at: string | null;
  finished_at: string | null;
  rating: number | null;
  current_page: number | null;
  total_pages: number | null;
  created_at: string;
}

export interface JournalEntry {
  entry_id: number;
  reading_log_id: number;
  entry_date: string;
  content: string;
  page_at_entry: number | null;
  mood: string | null;
  contains_spoilers: number;
}

export interface BorrowRecord {
  borrow_id: number;
  title: string;
  author: string | null;
  isbn: string | null;
  cover_url: string | null;
  lender_name: string;
  borrowed_date: string;
  due_date: string | null;
  returned_date: string | null;
  reminder_lead_days: number;
  notes: string | null;
  days_until_due: number | null;
  created_at: string;
}

export interface WishlistItem {
  wishlist_item_id: number;
  title: string;
  author: string | null;
  isbn: string | null;
  cover_url: string | null;
  target_price: number | null;
  notes: string | null;
  status: "ACTIVE" | "PURCHASED" | "ARCHIVED";
  added_at: string;
  latest_price: number | null;
  latest_currency: string | null;
  latest_price_url: string | null;
  latest_checked_at: string | null;
  lowest_price: number | null;
  first_price: number | null;
  drop_pct: number | null;
}

export interface WishlistPriceSnapshot {
  snapshot_id: number;
  wishlist_item_id: number;
  source: string;
  price: number;
  currency: string;
  url: string | null;
  checked_at: string;
}

export type NotificationType = "OVERDUE_LOAN" | "PRICE_DROP" | "BORROW_DUE_SOON" | "EVENT_NEARBY";

export interface AppNotification {
  notification_id: number;
  type: NotificationType;
  title: string;
  body: string | null;
  link: string | null;
  read_at: string | null;
  created_at: string;
}

export interface AnalyticsSummary {
  totals: { books: number; borrowers: number; active_loans: number; overdue_now: number };
  loans_per_month: { month: string; count: number }[];
  top_books: { book_id: number; title: string; author: string | null; loan_count: number }[];
  late_return_rate_per_month: { month: string; total: number; late: number; late_pct: number }[];
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const token = auth.getToken();
  const res = await fetch(`${API_URL}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...init?.headers,
    },
  });
  if (res.status === 401) {
    auth.clearSession();
    if (!window.location.pathname.startsWith("/login")) {
      window.location.href = "/login";
    }
    throw new Error("Sessão expirada. Faça login novamente.");
  }
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail ?? `Request failed: ${res.status}`);
  }
  return res.json();
}

export const api = {
  auth: {
    login: (payload: { email: string; password: string }) =>
      request<{ access_token: string; token_type: string; user: AuthUser }>("/auth/login", {
        method: "POST",
        body: JSON.stringify(payload),
      }),
    register: (payload: { first_name: string; last_name?: string; email: string; password: string }) =>
      request<{ access_token: string; token_type: string; user: AuthUser }>("/auth/register", {
        method: "POST",
        body: JSON.stringify(payload),
      }),
  },
  books: {
    list: (params?: { title?: string; author?: string; status?: string; limit?: number; offset?: number }) => {
      const qs = new URLSearchParams();
      if (params?.title) qs.set("title", params.title);
      if (params?.author) qs.set("author", params.author);
      if (params?.status) qs.set("status", params.status);
      if (params?.limit) qs.set("limit", String(params.limit));
      if (params?.offset) qs.set("offset", String(params.offset));
      const suffix = qs.toString() ? `?${qs}` : "";
      return request<Book[]>(`/books/${suffix}`);
    },
    count: (params?: { title?: string; author?: string; status?: string }) => {
      const qs = new URLSearchParams();
      if (params?.title) qs.set("title", params.title);
      if (params?.author) qs.set("author", params.author);
      if (params?.status) qs.set("status", params.status);
      const suffix = qs.toString() ? `?${qs}` : "";
      return request<{ count: number }>(`/books/count${suffix}`);
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
  analytics: {
    summary: (months = 12) => request<AnalyticsSummary>(`/analytics/summary?months=${months}`),
  },
  reading: {
    list: (status?: ReadingStatus) =>
      request<ReadingLogItem[]>(`/reading/${status ? `?status=${status}` : ""}`),
    create: (payload: {
      title?: string;
      author?: string;
      book_id?: number;
      cover_url?: string;
      total_pages?: number;
      status?: ReadingStatus;
    }) => request<ReadingLogItem>("/reading/", { method: "POST", body: JSON.stringify(payload) }),
    updateStatus: (id: number, status: ReadingStatus, rating?: number) =>
      request<ReadingLogItem>(`/reading/${id}/status`, {
        method: "PATCH",
        body: JSON.stringify({ status, rating }),
      }),
    updateProgress: (id: number, current_page: number, total_pages?: number) =>
      request<ReadingLogItem>(`/reading/${id}/progress`, {
        method: "PATCH",
        body: JSON.stringify({ current_page, total_pages }),
      }),
    remove: (id: number) => request<{ detail: string }>(`/reading/${id}`, { method: "DELETE" }),
    entries: (id: number) => request<JournalEntry[]>(`/reading/${id}/entries`),
    addEntry: (
      id: number,
      payload: { content: string; page_at_entry?: number; mood?: string; contains_spoilers?: boolean }
    ) => request<JournalEntry>(`/reading/${id}/entries`, { method: "POST", body: JSON.stringify(payload) }),
  },
  borrowed: {
    list: (status?: "active" | "returned") =>
      request<BorrowRecord[]>(`/borrowed/${status ? `?status=${status}` : ""}`),
    create: (payload: {
      title: string;
      lender_name: string;
      author?: string;
      isbn?: string;
      cover_url?: string;
      borrowed_date?: string;
      due_date?: string;
      reminder_lead_days?: number;
      notes?: string;
    }) => request<BorrowRecord>("/borrowed/", { method: "POST", body: JSON.stringify(payload) }),
    update: (
      id: number,
      payload: { due_date?: string; lender_name?: string; reminder_lead_days?: number; notes?: string }
    ) => request<BorrowRecord>(`/borrowed/${id}`, { method: "PATCH", body: JSON.stringify(payload) }),
    return: (id: number, returned_date?: string) =>
      request<BorrowRecord>(`/borrowed/${id}/return`, { method: "POST", body: JSON.stringify({ returned_date }) }),
    remove: (id: number) => request<{ detail: string }>(`/borrowed/${id}`, { method: "DELETE" }),
  },
  notifications: {
    list: () => request<AppNotification[]>("/notifications/"),
    markRead: (id: number) => request<{ detail: string }>(`/notifications/${id}/read`, { method: "POST" }),
  },
  wishlist: {
    list: (status?: "ACTIVE" | "PURCHASED" | "ARCHIVED") =>
      request<WishlistItem[]>(`/wishlist/${status ? `?status=${status}` : ""}`),
    create: (payload: {
      title: string;
      author?: string;
      isbn?: string;
      cover_url?: string;
      target_price?: number;
      notes?: string;
    }) => request<WishlistItem>("/wishlist/", { method: "POST", body: JSON.stringify(payload) }),
    update: (id: number, payload: { target_price?: number; notes?: string }) =>
      request<WishlistItem>(`/wishlist/${id}`, { method: "PATCH", body: JSON.stringify(payload) }),
    markPurchased: (id: number) => request<WishlistItem>(`/wishlist/${id}/purchased`, { method: "POST" }),
    archive: (id: number) => request<WishlistItem>(`/wishlist/${id}/archive`, { method: "POST" }),
    reactivate: (id: number) => request<WishlistItem>(`/wishlist/${id}/reactivate`, { method: "POST" }),
    snapshots: (id: number) => request<WishlistPriceSnapshot[]>(`/wishlist/${id}/snapshots`),
    remove: (id: number) => request<{ detail: string }>(`/wishlist/${id}`, { method: "DELETE" }),
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
