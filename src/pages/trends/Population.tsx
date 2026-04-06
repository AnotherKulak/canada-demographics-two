import { useState, useMemo } from 'react'
import { Link } from 'react-router-dom'
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from 'recharts'
import { useData } from '../../hooks/useData'
import DataBadge from '../../components/DataBadge'

interface PopulationHistory {
  data: { date: string; population: number }[]
}

export default function PopulationTrends() {
  const { data, loading, error } = useData<PopulationHistory>('population_history.json')

  const allYears = useMemo(() => {
    if (!data) return []
    const years = new Set(data.data.map(d => new Date(d.date).getFullYear()))
    return Array.from(years).sort((a, b) => a - b)
  }, [data])

  const [startYear, setStartYear] = useState<number | null>(null)
  const [endYear, setEndYear] = useState<number | null>(null)

  const minYear = allYears[0] ?? 2000
  const maxYear = allYears[allYears.length - 1] ?? 2025
  const effectiveStart = startYear ?? minYear
  const effectiveEnd = endYear ?? maxYear

  const chartData = useMemo(() => {
    if (!data) return []
    return data.data.filter(d => {
      const y = new Date(d.date).getFullYear()
      return y >= effectiveStart && y <= effectiveEnd
    })
  }, [data, effectiveStart, effectiveEnd])

  if (loading) return <PageSkeleton />
  if (error || !data) return <PageError />

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
      <Link to="/" className="text-slate-500 text-sm hover:text-slate-300 transition-colors mb-6 inline-flex items-center gap-1">
        ← Overview
      </Link>
      <h1 className="text-2xl font-bold text-slate-100 mb-2 mt-2">Total Population — Historical</h1>
      <p className="text-slate-400 text-sm mb-6">Quarterly estimates, 2000–present. Source: StatsCan 17-10-0009-01.</p>

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
                <linearGradient id="popGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#60a5fa" stopOpacity={0.25} />
                  <stop offset="95%" stopColor="#60a5fa" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis
                dataKey="date"
                tickFormatter={v => String(new Date(v).getFullYear())}
                tick={{ fill: '#475569', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                interval="preserveStartEnd"
              />
              <YAxis
                tickFormatter={v => `${(v / 1_000_000).toFixed(0)}M`}
                tick={{ fill: '#475569', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                width={48}
              />
              <Tooltip
                formatter={(v: number) => [v.toLocaleString('en-CA'), 'Population']}
                labelFormatter={l => new Date(l).toLocaleDateString('en-CA', { year: 'numeric', month: 'short' })}
                contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #1e293b', borderRadius: '8px' }}
                labelStyle={{ color: '#cbd5e1', fontSize: 12 }}
                itemStyle={{ color: '#94a3b8' }}
              />
              <Area type="monotone" dataKey="population" stroke="#60a5fa" strokeWidth={2} fill="url(#popGrad)" dot={false} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
        <div className="mt-4 pt-4 border-t border-slate-800">
          <DataBadge source="Statistics Canada 17-10-0009-01" period="Quarterly, 2000–present" />
        </div>
      </div>
    </div>
  )
}

function PageSkeleton() {
  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
      <div className="h-4 w-24 rounded bg-slate-800 animate-pulse mb-8" />
      <div className="h-8 w-80 rounded bg-slate-800 animate-pulse mb-4" />
      <div className="h-4 w-96 rounded bg-slate-800 animate-pulse mb-8" />
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6 h-96 animate-pulse" />
    </div>
  )
}

function PageError() {
  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6">
        <p className="text-red-400 text-sm">Failed to load population history.</p>
      </div>
    </div>
  )
}
