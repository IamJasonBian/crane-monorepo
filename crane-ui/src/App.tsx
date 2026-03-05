import { BrowserRouter as Router, Routes, Route, Link, useLocation } from 'react-router-dom';
import { BarChart3, List, Target } from 'lucide-react';

import PricesPage from './pages/PricesPage';
import ListingsPage from './pages/ListingsPage';
import TargetsPage from './pages/TargetsPage';

function NavLink({ to, children }: { to: string; children: React.ReactNode }) {
  const location = useLocation();
  const isActive = location.pathname === to;

  return (
    <Link
      to={to}
      className={`font-medium transition-colors ${
        isActive ? 'text-blue-600' : 'text-gray-500 hover:text-gray-700'
      }`}
    >
      {children}
    </Link>
  );
}

function AppContent() {
  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex items-center justify-between">
            <Link to="/" className="flex items-center group">
              <div className="p-2 bg-blue-100 rounded-xl group-hover:bg-blue-200 transition-colors">
                <BarChart3 className="h-5 w-5 text-blue-600" />
              </div>
              <h1 className="ml-3 text-xl font-bold text-gray-900">Crane</h1>
            </Link>
            <nav className="flex items-center space-x-8">
              <NavLink to="/">
                <span className="flex items-center gap-2">
                  <BarChart3 className="w-4 h-4" />
                  Prices
                </span>
              </NavLink>
              <NavLink to="/listings">
                <span className="flex items-center gap-2">
                  <List className="w-4 h-4" />
                  Listings
                </span>
              </NavLink>
              <NavLink to="/targets">
                <span className="flex items-center gap-2">
                  <Target className="w-4 h-4" />
                  Targets
                </span>
              </NavLink>
            </nav>
          </div>
        </div>
      </header>

      <main>
        <Routes>
          <Route path="/" element={<PricesPage />} />
          <Route path="/listings" element={<ListingsPage />} />
          <Route path="/targets" element={<TargetsPage />} />
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
