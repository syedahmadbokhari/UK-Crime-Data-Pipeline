import { BrowserRouter, Route, Routes } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import Layout from "./components/Layout";
import Dashboard from "./pages/Dashboard";
import CrimeSearch from "./pages/CrimeSearch";
import ReportGenerator from "./pages/ReportGenerator";
import AskAI from "./pages/AskAI";
import Health from "./pages/Health";

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1, staleTime: 30_000 } },
});

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Layout />}>
            <Route index element={<Dashboard />} />
            <Route path="crimes" element={<CrimeSearch />} />
            <Route path="reports" element={<ReportGenerator />} />
            <Route path="ask" element={<AskAI />} />
            <Route path="health" element={<Health />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
