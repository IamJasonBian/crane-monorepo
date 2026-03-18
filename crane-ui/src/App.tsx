import { BrowserRouter as Router, Routes, Route, Link, Navigate, useLocation } from 'react-router-dom';
import { BarChart3, List, Target, Search, Activity } from 'lucide-react';

import PricesPage from './pages/PricesPage';
import ListingsPage from './pages/ListingsPage';
import TermsPage from './pages/TermsPage';
import TargetsPage from './pages/TargetsPage';
import EventsPage from './pages/EventsPage';

function NavLink({ to, children }: { to: string; children: React.ReactNode }) {
  const location = useLocation();
  const isActive = location.pathname === to;

  return (
    <Link
      to={to}
      className={`text-xs uppercase tracking-wide transition-colors ${
        isActive ? 'text-white' : 'text-[#555] hover:text-[#888]'
      }`}
    >
      {children}
    </Link>
  );
}

function AppContent() {
  return (
    <div className="min-h-screen">
      <header className="border-b border-[#222] px-4 py-2">
        <div className="max-w-7xl mx-auto flex items-center justify-between">
          <Link to="/" className="flex items-center gap-2">
            <BarChart3 className="h-4 w-4 text-[#4a4]" />
            <h1 className="text-sm font-medium text-white tracking-wider">CRANE</h1>
          </Link>
          <nav className="flex items-center gap-6">
            <NavLink to="/listings">
              <span className="flex items-center gap-1.5">
                <List className="w-3 h-3" />
                Listings
              </span>
            </NavLink>
            <NavLink to="/terms">
              <span className="flex items-center gap-1.5">
                <Search className="w-3 h-3" />
                Terms
              </span>
            </NavLink>
            <NavLink to="/targets">
              <span className="flex items-center gap-1.5">
                <Target className="w-3 h-3" />
                Targets
              </span>
            </NavLink>
            <NavLink to="/events">
              <span className="flex items-center gap-1.5">
                <Activity className="w-3 h-3" />
                Events
              </span>
            </NavLink>
            <NavLink to="/prices">
              <span className="flex items-center gap-1.5">
                <BarChart3 className="w-3 h-3" />
                Prices
              </span>
            </NavLink>
          </nav>
        </div>
      </header>

      <main>
        <Routes>
          <Route path="/" element={<Navigate to="/listings" replace />} />
          <Route path="/listings" element={<ListingsPage />} />
          <Route path="/prices" element={<PricesPage />} />
          <Route path="/terms" element={<TermsPage />} />
          <Route path="/targets" element={<TargetsPage />} />
          <Route path="/events" element={<EventsPage />} />
        </Routes>
      </main>
    </div>
  );
}

export function App() {
  return (
    <Router>
      <AppContent />
    </Router>
  );
}
