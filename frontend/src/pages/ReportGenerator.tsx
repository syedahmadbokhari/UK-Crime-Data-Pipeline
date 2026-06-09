import { useState } from "react";
import { useGenerateReport, useReportJob } from "../api/queries";
import LoadingSpinner from "../components/LoadingSpinner";

export default function ReportGenerator() {
  const [force, setForce] = useState("West Yorkshire Police");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [jobId, setJobId] = useState<string | null>(null);

  const generate = useGenerateReport();
  const job = useReportJob(jobId);

  const handleSubmit = () => {
    generate.mutate(
      { force, start_date: startDate || undefined, end_date: endDate || undefined },
      { onSuccess: (data) => setJobId(data.job_id) },
    );
  };

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">AI Report Generator</h1>
      <p className="text-sm text-gray-600 mb-6">Generate a formal crime briefing report using Gemini AI. Requires authentication.</p>

      <div className="bg-white rounded-lg border p-6 mb-6">
        <div className="grid grid-cols-3 gap-3 mb-4">
          <div>
            <label className="block text-xs text-gray-500 mb-1">Force</label>
            <input className="w-full border rounded px-3 py-2 text-sm" value={force} onChange={(e) => setForce(e.target.value)} />
          </div>
          <div>
            <label className="block text-xs text-gray-500 mb-1">Start Month (YYYY-MM)</label>
            <input className="w-full border rounded px-3 py-2 text-sm" placeholder="2026-01" value={startDate} onChange={(e) => setStartDate(e.target.value)} />
          </div>
          <div>
            <label className="block text-xs text-gray-500 mb-1">End Month (YYYY-MM)</label>
            <input className="w-full border rounded px-3 py-2 text-sm" placeholder="2026-02" value={endDate} onChange={(e) => setEndDate(e.target.value)} />
          </div>
        </div>
        <button onClick={handleSubmit} disabled={generate.isPending} className="bg-blue-600 text-white px-4 py-2 rounded text-sm hover:bg-blue-700 disabled:opacity-50">
          {generate.isPending ? "Queuing..." : "Generate Report"}
        </button>
      </div>

      {jobId && (
        <div className="bg-white rounded-lg border p-6">
          <div className="flex items-center gap-2 mb-4">
            <span className="text-xs font-mono text-gray-500">{jobId}</span>
            <StatusBadge status={job.data?.status} />
          </div>
          {(job.data?.status === "queued" || job.data?.status === "processing") && <LoadingSpinner label="Generating report..." />}
          {job.data?.status === "completed" && job.data.result && (
            <pre className="whitespace-pre-wrap text-sm text-gray-800 leading-relaxed">{job.data.result.report}</pre>
          )}
          {job.data?.status === "failed" && <p className="text-red-500 text-sm">{job.data.error}</p>}
        </div>
      )}
    </div>
  );
}

function StatusBadge({ status }: { status?: string }) {
  const colours: Record<string, string> = {
    queued: "bg-yellow-100 text-yellow-800",
    processing: "bg-blue-100 text-blue-800",
    completed: "bg-green-100 text-green-800",
    failed: "bg-red-100 text-red-800",
  };
  return (
    <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${colours[status ?? ""] ?? "bg-gray-100 text-gray-600"}`}>
      {status ?? "unknown"}
    </span>
  );
}
