import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer } from 'recharts'
import { useData } from '../hooks/useData'
import DataBadge from './DataBadge'

interface StatusRow {
  status: string
  label: string
  count: number
  ref_date: string | null
  source: string
}

interface StatusPayload {
  actual_latest: { rows: StatusRow[]; total: number }
  estimated_latest: {
    rows: StatusRow[]
    total: number
    reconciles_to_population_total: number
    ref_date: string | null
  }
  methodology: {
    summary: string
    actual_definition: string
    estimated_definition: string
  }
}

const COLORS: Record<string, string> = {
  canadian_born: '#f83b3b',
  naturalized: '#fb923c',
  permanent_resident: '#facc15',
  non_permanent_resident: '#60a5fa',
}

function formatCount(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(0)}K`
  return n.toLocaleString('en-CA')
}

function formatRefDate(dateStr: string | null): string {
  if (!dateStr) return 'Date unavailable'
  return new Date(dateStr).toLocaleDateString('en-CA', { year: 'numeric', month: 'long' })
}

export default function StatusBreakdown() {
  const [mode, setMode] = useState<'estimated' | 'actual'>('estimated')
  const { data, loading, error } = useData<StatusPayload>('status_breakdown.json')

  const rows = useMemo(() => {
    if (!data) return []
    return mode === 'estimated' ? data.estimated_latest.rows : data.actual_latest.rows
  }, [data, mode])

  if (loading) return <SectionSkeleton />
  if (error || !data) return <SectionError />

  const total = rows.reduce((sum, row) => sum + row.count, 0)

  return (
    <section>
      <div className="flex items-start justify-between gap-4 mb-6">
        <div>
          <h2 className="text-lg font-semibold text-slate-200">Population by Status</h2>
          <p className="text-slate-500 text-sm mt-1">
            Toggle between the latest published values and a current estimate that reconciles to the Canada total.
          </p>
        </div>
        <Link to="/trends/status-breakdown" className="text-xs text-maple-500 hover:text-maple-400 transition-colors shrink-0 mt-1">
          View census history →
        </Link>
      </div>

      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6">
        <div className="flex flex-wrap items-center justify-between gap-4 mb-6">
          <div className="inline-flex rounded-lg border border-slate-700 bg-slate-950 p-1">
            {(['estimated', 'actual'] as const).map((key) => (
              <button
                key={key}
                onClick={() => setMode(key)}
                className={`px-3 py-1.5 text-xs rounded-md transition-colors ${
                  mode === key ? 'bg-slate-700 text-slate-100' : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                {key === 'estimated' ? 'Estimated' : 'Actual'}
              </button>
            ))}
          </div>
          <p className="text-slate-500 text-xs max-w-2xl">
            {mode === 'estimated' ? data.methodology.estimated_definition : data.methodology.actual_definition}
          </p>
        </div>

        <div className="flex flex-col lg:flex-row gap-8 items-center">
          <div className="w-full lg:w-72 h-64 shrink-0">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={rows} cx="50%" cy="50%" innerRadius="55%" outerRadius="80%" paddingAngle={2} dataKey="count">
                  {rows.map((row) => (
                    <Cell key={row.status} fill={COLORS[row.status] ?? '#888'} />
                  ))}
                </Pie>
                <Tooltip
                  formatter={(value: number, _name: unknown, props: { payload?: StatusRow }) => [
                    value.toLocaleString('en-CA'),
                    props.payload?.label ?? 'Status',
                  ]}
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #1e293b', borderRadius: '8px' }}
                  labelStyle={{ display: 'none' }}
                  itemStyle={{ color: '#cbd5e1' }}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>

          <div className="flex-1 grid grid-cols-1 sm:grid-cols-2 gap-3 w-full">
            {rows.map((row) => {
              const pct = total > 0 ? ((row.count / total) * 100).toFixed(1) : '—'
              return (
                <div key={row.status} className="flex items-start gap-3 p-3 rounded-lg bg-slate-800/50">
                  <div className="w-3 h-3 rounded-full mt-0.5 shrink-0" style={{ backgroundColor: COLORS[row.status] }} />
                  <div className="min-w-0">
                    <p className="text-slate-300 text-sm font-medium leading-tight">{row.label}</p>
                    <p className="text-slate-100 text-xl font-bold tabular-nums mt-0.5">{formatCount(row.count)}</p>
                    <p className="text-slate-500 text-xs">{pct}% of displayed total</p>
                    <p className="text-slate-700 text-xs mt-1">
                      {mode === 'estimated' ? `Estimated for ${formatRefDate(row.ref_date)}` : `Published as of ${formatRefDate(row.ref_date)}`}
                    </p>
                  </div>
                </div>
              )
            })}
          </div>
        </div>

        <div className="mt-5 pt-4 border-t border-slate-800 flex flex-wrap gap-4">
          <DataBadge
            source={mode === 'estimated' ? 'Automated estimate' : 'Latest published sources'}
            period={mode === 'estimated'
              ? `Reconciles to ${data.estimated_latest.reconciles_to_population_total.toLocaleString('en-CA')}`
              : `Displayed total ${data.actual_latest.total.toLocaleString('en-CA')}`}
          />
        </div>
        <p className="text-slate-600 text-xs mt-2">{data.methodology.summary}</p>
      </div>
    </section>
  )
}

function SectionSkeleton() {
  return (
    <section>
      <div className="h-6 w-48 rounded bg-slate-800 animate-pulse mb-6" />
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6 h-80 animate-pulse" />
    </section>
  )
}

function SectionError() {
  return (
    <section>
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6">
        <p className="text-red-400 text-sm">Failed to load status breakdown data.</p>
      </div>
    </section>
  )
}
