export interface Crime {
  id: number;
  month: string;
  force: string;
  crime_type: string;
  lsoa_code?: string;
  lsoa_name?: string;
  outcome?: string;
  latitude?: number;
  longitude?: number;
}

export interface PaginatedCrimes {
  total: number;
  page: number;
  page_size: number;
  results: Crime[];
}

export interface CursorPaginatedCrimes {
  data: Crime[];
  next_cursor: number | null;
  has_more: boolean;
}

export interface CrimeSummary {
  force: string;
  total_crimes: number;
  top_crime_type: string;
  under_investigation_pct: number;
}

export interface ReportJob {
  job_id: string;
  status: "queued" | "processing" | "completed" | "failed";
  result?: {
    report: string;
    force: string;
    total_crimes: number;
    period_covered: string;
    generated_at: string;
  };
  error?: string;
  created_at: string;
  completed_at?: string;
}

export interface AskResponse {
  answer: string;
  sources: Array<{ id: number; month: string; force: string; crime_type: string; lsoa_name?: string }>;
  confidence: number;
  records_analysed: number;
}

export interface HealthStatus {
  status: string;
  version: string;
  uptime_seconds: number;
  timestamp: string;
  services: {
    database: string;
    redis: string;
    gemini: string;
  };
}

export interface AuthToken {
  access_token: string;
  token_type: string;
}
