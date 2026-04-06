import { Link } from 'react-router-dom'
import { useData } from '../hooks/useData'
import DataBadge from './DataBadge'

interface PopulationCurrent {
  population: number
  ref_date: string
  source: string
  frequency: string
}

function formatPopulation(n: number): string {
  return n.toLocaleString('en-CA')
}

function formatRefDate(dateStr: string): string {
  const d = new Date(dateStr)
  return d.toLocaleDateString('en-CA', { year: 'numeric', month: 'long' })
}

export default function HeroTotal() {
  const { data, loading, error } = useData<PopulationCurrent>('population_current.json')

  return (
    <section className="relative overflow-hidden rounded-2xl border border-slate-800 bg-gradient-to-br from-slate-900 to-slate-950 p-8 sm:p-12">
      {/* subtle maple-red accent */}
      <div className="absolute -top-16 -right-16 w-64 h-64 rounded-full bg-maple-600/5 blur-3xl pointer-events-none" />

      <p className="text-slate-500 text-xs uppercase tracking-widest mb-3">
        Total Canadian Population
      </p>

      {loading && (
        <div className="h-20 flex items-center">
          <div className="h-16 w-64 rounded-lg bg-slate-800 animate-pulse" />
        </div>
      )}

      {error && (
        <p className="text-red-400 text-sm">Failed to load population data.</p>
      )}

      {data && (
        <>
          <div className="flex items-end gap-4 flex-wrap">
            <h1 className="text-6xl sm:text-7xl font-bold text-slate-100 tabular-nums tracking-tight">
              {formatPopulation(data.population)}
            </h1>
          </div>

          <div className="mt-4 flex items-center gap-4 flex-wrap">
            <DataBadge source={data.source} period={`Q ending ${formatRefDate(data.ref_date)}`} />
            <Link
              to="/trends/population"
              className="text-xs text-maple-500 hover:text-maple-400 transition-colors"
            >
              View historical trend →
            </Link>
          </div>
        </>
      )}
    </section>
  )
}
