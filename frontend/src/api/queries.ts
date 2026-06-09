import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, API_V1 } from "./client";
import type { AskResponse, AuthToken, CrimeSummary, HealthStatus, PaginatedCrimes, ReportJob } from "../types/api";

// ── Auth ──────────────────────────────────────────────────────────────────────
export const useLogin = () =>
  useMutation({
    mutationFn: (body: { email: string; password: string }) =>
      api.post<AuthToken>(`${API_V1}/auth/login`, body).then((r) => r.data),
    onSuccess: (data) => localStorage.setItem("access_token", data.access_token),
  });

// ── Crimes ────────────────────────────────────────────────────────────────────
export const useCrimes = (params: { force?: string; month?: string; crime_type?: string; page?: number }) =>
  useQuery({
    queryKey: ["crimes", params],
    queryFn: () => api.get<PaginatedCrimes>(`${API_V1}/crimes/`, { params }).then((r) => r.data),
  });

export const useCrimeSummary = (force: string) =>
  useQuery({
    queryKey: ["summary", force],
    queryFn: () => api.get<CrimeSummary>(`${API_V1}/crimes/summary`, { params: { force } }).then((r) => r.data),
    enabled: !!force,
  });

// ── Reports ───────────────────────────────────────────────────────────────────
export const useGenerateReport = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { force: string; start_date?: string; end_date?: string }) =>
      api.post<{ job_id: string; status: string }>(`${API_V1}/reports/generate`, body).then((r) => r.data),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["report-jobs"] }),
  });
};

export const useReportJob = (jobId: string | null) =>
  useQuery({
    queryKey: ["report-job", jobId],
    queryFn: () => api.get<ReportJob>(`${API_V1}/reports/${jobId}`).then((r) => r.data),
    enabled: !!jobId,
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      return status === "queued" || status === "processing" ? 2000 : false;
    },
  });

// ── RAG ───────────────────────────────────────────────────────────────────────
export const useAskQuestion = () =>
  useMutation({
    mutationFn: (question: string) =>
      api.post<AskResponse>(`${API_V1}/crimes/ask`, { question }).then((r) => r.data),
  });

// ── Health ────────────────────────────────────────────────────────────────────
export const useHealth = () =>
  useQuery({
    queryKey: ["health"],
    queryFn: () => api.get<HealthStatus>("/health/ready").then((r) => r.data),
    refetchInterval: 30_000,
  });
