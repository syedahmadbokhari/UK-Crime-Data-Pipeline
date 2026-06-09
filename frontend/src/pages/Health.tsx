import { useHealth } from "../api/queries";
import LoadingSpinner from "../components/LoadingSpinner";

export default function Health() {
  const { data, isLoading } = useHealth();

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Health Status</h1>
      {isLoading && <LoadingSpinner />}
      {data && (
        <div className="space-y-4">
          <StatusCard label="Overall" value={data.status} />
          <div className="bg-white rounded-lg border p-6">
            <h2 className="font-semibold mb-4">Services</h2>
            <div className="space-y-2">
              {Object.entries(data.services).map(([svc, status]) => (
                <div key={svc} className="flex justify-between items-center">
                  <span className="text-sm capitalize">{svc}</span>
                  <StatusBadge status={status} />
                </div>
              ))}
            </div>
          </div>
          <div className="bg-white rounded-lg border p-6 text-sm text-gray-600 space-y-1">
            <p><strong>Version:</strong> {data.version}</p>
            <p><strong>Uptime:</strong> {data.uptime_seconds}s</p>
            <p><strong>Timestamp:</strong> {new Date(data.timestamp).toLocaleString()}</p>
          </div>
        </div>
      )}
    </div>
  );
}

function StatusCard({ label, value }: { label: string; value: string }) {
  const isHealthy = value === "healthy" || value === "ok";
  return (
    <div className={`rounded-lg border p-4 ${isHealthy ? "bg-green-50 border-green-200" : "bg-yellow-50 border-yellow-200"}`}>
      <p className="text-xs text-gray-500">{label}</p>
      <p className={`font-semibold capitalize ${isHealthy ? "text-green-700" : "text-yellow-700"}`}>{value}</p>
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const ok = ["healthy", "configured", "ok"].includes(status);
  return (
    <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${ok ? "bg-green-100 text-green-800" : "bg-gray-100 text-gray-600"}`}>
      {status}
    </span>
  );
}
