import { BrowserRouter, Routes, Route } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Sidebar } from "./components/Sidebar";
import { Dashboard } from "./pages/Dashboard";
import { SmartAssistant } from "./pages/SmartAssistant";
import { BookCatalog } from "./pages/BookCatalog";
import { Borrowers } from "./pages/Borrowers";
import { Loans } from "./pages/Loans";

const queryClient = new QueryClient();

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <div className="flex min-h-svh">
          <Sidebar />
          <main className="flex-1 p-8">
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/assistant" element={<SmartAssistant />} />
              <Route path="/catalog" element={<BookCatalog />} />
              <Route path="/borrowers" element={<Borrowers />} />
              <Route path="/loans" element={<Loans />} />
            </Routes>
          </main>
        </div>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
