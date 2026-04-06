import { Outlet, NavLink } from 'react-router-dom'

export default function Layout() {
  return (
    <div className="min-h-screen flex flex-col">
      <header className="border-b border-slate-800 bg-slate-950/80 backdrop-blur-sm sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <NavLink to="/" className="flex items-center gap-3">
            <span className="text-maple-500 font-bold text-xl tracking-tight">CA</span>
            <span className="text-slate-100 font-semibold text-sm tracking-wide uppercase">
              Demographics
            </span>
          </NavLink>
          <nav className="flex items-center gap-6">
            <NavLink
              to="/"
              end
              className={({ isActive }) =>
                `text-sm transition-colors ${isActive ? 'text-slate-100' : 'text-slate-400 hover:text-slate-200'}`
              }
            >
              Overview
            </NavLink>
            <NavLink
              to="/methodology"
              className={({ isActive }) =>
                `text-sm transition-colors ${isActive ? 'text-slate-100' : 'text-slate-400 hover:text-slate-200'}`
              }
            >
              Methodology
            </NavLink>
          </nav>
        </div>
      </header>

      <main className="flex-1">
        <Outlet />
      </main>

      <footer className="border-t border-slate-800 py-8 mt-16">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex flex-col sm:flex-row items-center justify-between gap-4">
          <p className="text-slate-500 text-sm">
            Data sourced from Statistics Canada and Immigration, Refugees and Citizenship Canada.
          </p>
          <p className="text-slate-600 text-xs">
            Not an official Government of Canada publication.
          </p>
        </div>
      </footer>
    </div>
  )
}
