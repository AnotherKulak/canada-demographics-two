import { useState, useMemo } from 'react'
import { Link } from 'react-router-dom'
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from 'recharts'
import { useData } from '../../hooks/useData'
import DataBadge from '../../components/DataBadge'

interface OriginPRHistory {
  data: { year: number; country: string; count: number }[]
}

export default function PermanentResidentTrends() {
  const { data, loading, error } = useData<OriginPRHistory>('origin_pr_history.json')

  // Aggregate top-10-country admissions by year for a total trend line
  const byYear = useMemo(() => {
    if (!data) return []
    const totals = new Map<number, number>()
    for (const row of data.data) {
      totals.set(row.year, (totals.get(row.year) ?? 0) + row.count)
    }
    return Array.from(totals.entries())
      .sort(([a], [b]) => a - b)
      .map(([year, total]) => ({ year, total }))
  }, [data])

  const allYears = useMemo(() => byYear.map(d => d.year), [byYear])

  const [startYear, setStartYear] = useState<number | null>(null)
  const [endYear, setEndYear] = useState<number | null>(null)

  const minYear = allYears[0] ?? 2015
  const maxYear = allYears[allYears.length - 1] ?? 2024
  const effectiveStart = startYear ?? minYear
  const effectiveEnd = endYear ?? maxYear

  const chartData = useMemo(
    () => byYear.filter(d => d.year >= effectiveStart && d.year <= effectiveEnd),
    [byYear, effectiveStart, effectiveEnd]
  )

  if (loading) return <PageSkeleton />
  if (error || !data) return <PageError />

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
      <Link to="/" className="text-slate-500 text-sm hover:text-slate-300 transition-colors mb-6 inline-flex items-center gap-1">
        ← Overview
      </Link>
      <h1 className="text-2xl font-bold text-slate-100 mb-2 mt-2">Permanent Residents — Historical</h1>
      <p className="text-slate-400 text-sm mb-6">
        Annual PR admissions, 2015–present. Totals reflect top source countries. Source: IRCC open data.
      </p>

      <div className="flex items-center gap-3 mb-6">
        <span className="text-slate-500 text-sm">From</span>
        <select
          className="bg-slate-800 border border-slate-700 text-slate-300 text-sm rounded px-2 py-1"
          value={effectiveStart}
          onChange={e => setStartYear(Number(e.target.value))}
        >
          {allYears.map(y => <option key={y} value={y}>{y}</option>)}
        </select>
        <span className="text-slate-500 text-sm">to</span>
        <select
          className="bg-slate-800 border border-slate-700 text-slate-300 text-sm rounded px-2 py-1"
          value={effectiveEnd}
          onChange={e => setEndYear(Number(e.target.value))}
        >
          {allYears.map(y => <option key={y} value={y}>{y}</option>)}
        </select>
      </div>

      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6">
        <div style={{ height: 400 }}>
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData} margin={{ top: 10, right: 20, left: 10, bottom: 0 }}>
              <defs>
                <linearGradient id="prGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#4ade80" stopOpacity={0.25} />
                  <stop offset="95%" stopColor="#4ade80" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis
                dataKey="year"
                tick={{ fill: '#475569', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                tickFormatter={v => `${(v / 1_000).toFixed(0)}K`}
                tick={{ fill: '#475569', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                width={52}
              />
              <Tooltip
                formatter={(v: number) => [v.toLocaleString('en-CA'), 'PR Admissions']}
                labelFormatter={l => `Year: ${l}`}
                contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #1e293b', borderRadius: '8px' }}
                labelStyle={{ color: '#cbd5e1', fontSize: 12 }}
                itemStyle={{ color: '#94a3b8' }}
              />
              <Area type="monotone" dataKey="total" stroke="#4ade80" strokeWidth={2} fill="url(#prGrad)" dot={{ fill: '#4ade80', r: 3 }} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
        <div className="mt-4 pt-4 border-t border-slate-800">
          <DataBadge source="IRCC open data (CKAN)" period="Annual, 2015–present" />
        </div>
      </div>
    </div>
  )
}

function PageSkeleton() {
  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
      <div className="h-4 w-24 rounded bg-slate-800 animate-pulse mb-8" />
      <div className="h-8 w-72 rounded bg-slate-800 animate-pulse mb-4" />
      <div className="h-4 w-96 rounded bg-slate-800 animate-pulse mb-8" />
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6 h-96 animate-pulse" />
    </div>
  )
}

function PageError() {
  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6">
        <p className="text-red-400 text-sm">Failed to load permanent resident history.</p>
      </div>
    </div>
  )
}
