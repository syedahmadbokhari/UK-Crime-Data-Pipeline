import { NavLink, Outlet } from "react-router-dom";

const nav = [
  { to: "/", label: "Dashboard" },
  { to: "/crimes", label: "Crime Search" },
  { to: "/reports", label: "AI Reports" },
  { to: "/ask", label: "Ask AI" },
  { to: "/health", label: "Health" },
];

export default function Layout() {
  return (
    <div className="min-h-screen bg-gray-50">
      <nav className="bg-gray-900 text-white px-6 py-3 flex items-center gap-6">
        <span className="font-bold text-lg mr-4">Crime Data API</span>
        {nav.map((n) => (
          <NavLink
            key={n.to}
            to={n.to}
            end={n.to === "/"}
            className={({ isActive }) =>
              `text-sm ${isActive ? "text-blue-400 font-semibold" : "text-gray-300 hover:text-white"}`
            }
          >
            {n.label}
          </NavLink>
        ))}
      </nav>
      <main className="max-w-5xl mx-auto px-6 py-8">
        <Outlet />
      </main>
    </div>
  );
}
