import { useState } from "react";
import { useCrimes } from "../api/queries";
import LoadingSpinner from "../components/LoadingSpinner";

export default function CrimeSearch() {
  const [force, setForce] = useState("");
  const [month, setMonth] = useState("");
  const [crimeType, setCrimeType] = useState("");
  const [page, setPage] = useState(1);

  const { data, isLoading } = useCrimes({
    force: force || undefined,
    month: month || undefined,
    crime_type: crimeType || undefined,
    page,
  });

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Crime Search</h1>

      <div className="grid grid-cols-3 gap-3 mb-6">
        <input className="border rounded px-3 py-2 text-sm" placeholder="Force (e.g. West Yorkshire)" value={force} onChange={(e) => { setForce(e.target.value); setPage(1); }} />
        <input className="border rounded px-3 py-2 text-sm" placeholder="Month (YYYY-MM)" value={month} onChange={(e) => { setMonth(e.target.value); setPage(1); }} />
        <input className="border rounded px-3 py-2 text-sm" placeholder="Crime type" value={crimeType} onChange={(e) => { setCrimeType(e.target.value); setPage(1); }} />
      </div>

      {isLoading && <LoadingSpinner />}

      {data && (
        <>
          <p className="text-sm text-gray-500 mb-3">{data.total.toLocaleString()} results</p>
          <div className="bg-white rounded-lg border overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 text-gray-600 text-left">
                <tr>
                  <th className="px-4 py-2">Month</th>
                  <th className="px-4 py-2">Force</th>
                  <th className="px-4 py-2">Crime Type</th>
                  <th className="px-4 py-2">Outcome</th>
                </tr>
              </thead>
              <tbody>
                {data.results.map((c) => (
                  <tr key={c.id} className="border-t hover:bg-gray-50">
                    <td className="px-4 py-2">{c.month}</td>
                    <td className="px-4 py-2">{c.force}</td>
                    <td className="px-4 py-2">{c.crime_type}</td>
                    <td className="px-4 py-2 text-gray-500 truncate max-w-[200px]">{c.outcome ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="flex gap-2 mt-4">
            <button className="px-3 py-1 text-sm border rounded disabled:opacity-40" onClick={() => setPage((p) => p - 1)} disabled={page === 1}>← Prev</button>
            <span className="text-sm self-center">Page {page}</span>
            <button className="px-3 py-1 text-sm border rounded disabled:opacity-40" onClick={() => setPage((p) => p + 1)} disabled={data.results.length < 100}>Next →</button>
          </div>
        </>
      )}
    </div>
  );
}
