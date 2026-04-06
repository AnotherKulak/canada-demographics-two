import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend } from 'recharts'
import { useData } from '../../hooks/useData'
import DataBadge from '../../components/DataBadge'

interface HistoryRow {
  date: string
  stream: string
  count: number
  cadence: 'quarterly' | 'annual'
  source_origin: 'statcan' | 'ircc'
}

interface VisaStreamsHistory {
  data: HistoryRow[]
  series_meta?: Record<string, { label: string; group: string }>
  note?: string
}

const COLORS = ['#60a5fa', '#818cf8', '#a78bfa', '#c4b5fd', '#fb923c', '#fbbf24', '#fcd34d', '#fde68a', '#34d399', '#f87171', '#38bdf8']

export default function VisaStreamTrends() {
  const { data, loading, error } = useData<VisaStreamsHistory>('visa_streams_history.json')

  const allYears = useMemo(() => {
    if (!data) return []
    const years = new Set(data.data.map((row) => new Date(row.date).getFullYear()))
    return Array.from(years).sort((a, b) => a - b)
  }, [data])

  const seriesKeys = useMemo(() => {
    if (!data) return []
    return data.series_meta ? Object.keys(data.series_meta) : Array.from(new Set(data.data.map((row) => row.stream)))
  }, [data])

  const [startYear, setStartYear] = useState<number | null>(null)
  const [endYear, setEndYear] = useState<number | null>(null)

  const minYear = allYears[0] ?? 2015
  const maxYear = allYears[allYears.length - 1] ?? 2026
  const effectiveStart = startYear ?? minYear
  const effectiveEnd = endYear ?? maxYear

  const chartData = useMemo(() => {
    if (!data) return []
    const byDate = new Map<string, Record<string, number | string>>()
    for (const row of data.data) {
      const year = new Date(row.date).getFullYear()
      if (year < effectiveStart || year > effectiveEnd) continue
      if (!byDate.has(row.date)) byDate.set(row.date, { date: row.date })
      byDate.get(row.date)![row.stream] = row.count
    }
    return Array.from(byDate.values()).sort((a, b) => String(a.date).localeCompare(String(b.date)))
  }, [data, effectiveStart, effectiveEnd])

  if (loading) return <PageSkeleton />
  if (error || !data) return <PageError />

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
      <Link to="/" className="text-slate-500 text-sm hover:text-slate-300 transition-colors mb-6 inline-flex items-center gap-1">
        ← Overview
      </Link>
      <h1 className="text-2xl font-bold text-slate-100 mb-2 mt-2">Temporary Residents by Visa Stream — Historical</h1>
      <p className="text-slate-400 text-sm mb-2">
        Automatic latest-source history with quarterly StatsCan data where available and annual IRCC fallback for earlier years.
      </p>
      <p className="text-slate-500 text-xs mb-6">
        {data.note ?? 'Quarterly StatsCan history is shown where available. Earlier years will use annual fallback data after the next pipeline refresh.'}
      </p>

      <div className="flex items-center gap-3 mb-6">
        <span className="text-slate-500 text-sm">From</span>
        <select className="bg-slate-800 border border-slate-700 text-slate-300 text-sm rounded px-2 py-1" value={effectiveStart} onChange={e => setStartYear(Number(e.target.value))}>
          {allYears.map((year) => <option key={year} value={year}>{year}</option>)}
        </select>
        <span className="text-slate-500 text-sm">to</span>
        <select className="bg-slate-800 border border-slate-700 text-slate-300 text-sm rounded px-2 py-1" value={effectiveEnd} onChange={e => setEndYear(Number(e.target.value))}>
          {allYears.map((year) => <option key={year} value={year}>{year}</option>)}
        </select>
      </div>

      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6">
        <div style={{ height: 460 }}>
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData} margin={{ top: 10, right: 20, left: 10, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis
                dataKey="date"
                tickFormatter={(value) => new Date(value).toLocaleDateString('en-CA', { year: 'numeric', month: 'short' })}
                tick={{ fill: '#475569', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                interval="preserveStartEnd"
              />
              <YAxis tickFormatter={(value) => `${(value / 1_000).toFixed(0)}K`} tick={{ fill: '#475569', fontSize: 11 }} axisLine={false} tickLine={false} width={52} />
              <Tooltip
                formatter={(value: number, name: string) => [value.toLocaleString('en-CA'), data.series_meta?.[name]?.label ?? name]}
                labelFormatter={(label) => new Date(label).toLocaleDateString('en-CA', { year: 'numeric', month: 'long' })}
                contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #1e293b', borderRadius: '8px' }}
                labelStyle={{ color: '#cbd5e1', fontSize: 12 }}
                itemStyle={{ fontSize: 11 }}
              />
              <Legend formatter={(value) => data.series_meta?.[value]?.label ?? value} wrapperStyle={{ fontSize: 11, color: '#94a3b8' }} />
              {seriesKeys.map((key, index) => (
                <Area
                  key={key}
                  type="monotone"
                  dataKey={key}
                  stroke={COLORS[index % COLORS.length]}
                  fill={COLORS[index % COLORS.length]}
                  fillOpacity={0.12}
                  strokeWidth={1.75}
                  dot={false}
                  connectNulls
                />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        </div>
        <div className="mt-4 pt-4 border-t border-slate-800">
          <DataBadge source="Statistics Canada + IRCC" period="Quarterly where available, annual fallback otherwise" />
        </div>
      </div>
    </div>
  )
}

function PageSkeleton() {
  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
      <div className="h-4 w-24 rounded bg-slate-800 animate-pulse mb-8" />
      <div className="h-8 w-96 rounded bg-slate-800 animate-pulse mb-4" />
      <div className="h-4 w-80 rounded bg-slate-800 animate-pulse mb-8" />
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6 h-96 animate-pulse" />
    </div>
  )
}

function PageError() {
  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6">
        <p className="text-red-400 text-sm">Failed to load visa stream history.</p>
      </div>
    </div>
  )
}
