import { useCrimeSummary } from "../api/queries";
import LoadingSpinner from "../components/LoadingSpinner";

const FORCES = ["West Yorkshire Police", "Greater Manchester Police", "Metropolitan Police"];

export default function Dashboard() {
  const { data, isLoading, error } = useCrimeSummary("West Yorkshire");

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Dashboard</h1>

      {isLoading && <LoadingSpinner label="Loading summary..." />}
      {error && <p className="text-red-500 text-sm">Sign in to view summary data.</p>}

      {data && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
          <StatCard label="Force" value={data.force} />
          <StatCard label="Total Crimes" value={data.total_crimes.toLocaleString()} />
          <StatCard label="Top Crime Type" value={data.top_crime_type} />
          <StatCard label="Under Investigation" value={`${data.under_investigation_pct}%`} />
        </div>
      )}

      <div className="bg-white rounded-lg border p-6">
        <h2 className="font-semibold mb-2">About this API</h2>
        <p className="text-sm text-gray-600">
          Production-grade REST API for UK Police crime data. Supports filtering, pagination,
          force-level summaries, AI-generated briefing reports, and natural language queries.
        </p>
        <div className="mt-4 flex gap-3">
          <a href="/docs" target="_blank" className="text-sm text-blue-600 hover:underline">Swagger Docs →</a>
          <a href="https://github.com/syedahmadbokhari/UK-Crime-Data-Pipeline" target="_blank" className="text-sm text-blue-600 hover:underline">GitHub →</a>
        </div>
      </div>
    </div>
  );
}

function StatCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-white rounded-lg border p-4">
      <p className="text-xs text-gray-500 mb-1">{label}</p>
      <p className="font-semibold text-gray-900 truncate">{value}</p>
    </div>
  );
}
