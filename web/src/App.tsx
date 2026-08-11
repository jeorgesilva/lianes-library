import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { AuthProvider, useAuth } from "./lib/AuthContext";
import { Sidebar } from "./components/Sidebar";
import { Dashboard } from "./pages/Dashboard";
import { SmartAssistant } from "./pages/SmartAssistant";
import { Reading } from "./pages/Reading";
import { BookCatalog } from "./pages/BookCatalog";
import { Borrowers } from "./pages/Borrowers";
import { Loans } from "./pages/Loans";
import { Login } from "./pages/Login";
import { Analytics } from "./pages/Analytics";

const queryClient = new QueryClient();

function ProtectedLayout() {
  const { user } = useAuth();
  if (!user) return <Navigate to="/login" replace />;

  return (
    <div className="flex min-h-svh flex-col sm:flex-row">
      <Sidebar />
      <main className="flex-1 p-4 sm:p-8">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/reading" element={<Reading />} />
          <Route path="/assistant" element={<SmartAssistant />} />
          <Route path="/catalog" element={<BookCatalog />} />
          <Route path="/borrowers" element={<Borrowers />} />
          <Route path="/loans" element={<Loans />} />
          <Route path="/analytics" element={<Analytics />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </div>
  );
}

function AppRoutes() {
  const { user } = useAuth();

  return (
    <Routes>
      <Route path="/login" element={user ? <Navigate to="/" replace /> : <Login />} />
      <Route path="/*" element={<ProtectedLayout />} />
    </Routes>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AuthProvider>
          <AppRoutes />
        </AuthProvider>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
