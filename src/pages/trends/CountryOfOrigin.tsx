import { useState, useMemo } from 'react'
import { Link } from 'react-router-dom'
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend,
} from 'recharts'
import { useData } from '../../hooks/useData'
import DataBadge from '../../components/DataBadge'

interface OriginPRHistory {
  data: { year: number; country: string; count: number }[]
}

// 10 distinct colours for country lines
const COUNTRY_COLORS = [
  '#60a5fa', '#4ade80', '#f472b6', '#fb923c', '#a78bfa',
  '#fbbf24', '#34d399', '#f87171', '#38bdf8', '#e879f9',
]

// Shorten long country names for legend readability
function shortName(name: string): string {
  return name
    .replace("China, People's Republic of", 'China')
    .replace('United States of America', 'USA')
    .replace('United Kingdom of Great Britain and Northern Ireland', 'UK')
}

export default function CountryOfOriginTrends() {
  const { data, loading, error } = useData<OriginPRHistory>('origin_pr_history.json')

  // Find top countries by total count across all years
  const topCountries = useMemo(() => {
    if (!data) return []
    const totals = new Map<string, number>()
    for (const row of data.data) {
      totals.set(row.country, (totals.get(row.country) ?? 0) + row.count)
    }
    return Array.from(totals.entries())
      .sort(([, a], [, b]) => b - a)
      .slice(0, 10)
      .map(([country]) => country)
  }, [data])

  const allYears = useMemo(() => {
    if (!data) return []
    const years = new Set(data.data.map(d => d.year))
    return Array.from(years).sort((a, b) => a - b)
  }, [data])

  const [startYear, setStartYear] = useState<number | null>(null)
  const [endYear, setEndYear] = useState<number | null>(null)

  const minYear = allYears[0] ?? 2015
  const maxYear = allYears[allYears.length - 1] ?? 2024
  const effectiveStart = startYear ?? minYear
  const effectiveEnd = endYear ?? maxYear

  // Pivot: one object per year, each top country as a key
  const chartData = useMemo(() => {
    if (!data || !topCountries.length) return []
    const countrySet = new Set(topCountries)
    const byYear = new Map<number, Record<string, number | string>>()
    for (const row of data.data) {
      if (row.year < effectiveStart || row.year > effectiveEnd) continue
      if (!countrySet.has(row.country)) continue
      if (!byYear.has(row.year)) byYear.set(row.year, { year: row.year })
      byYear.get(row.year)![row.country] = row.count
    }
    return Array.from(byYear.values()).sort((a, b) => Number(a.year) - Number(b.year))
  }, [data, topCountries, effectiveStart, effectiveEnd])

  if (loading) return <PageSkeleton />
  if (error || !data) return <PageError />

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
      <Link to="/" className="text-slate-500 text-sm hover:text-slate-300 transition-colors mb-6 inline-flex items-center gap-1">
        ← Overview
      </Link>
      <h1 className="text-2xl font-bold text-slate-100 mb-2 mt-2">Country of Origin — Historical</h1>
      <p className="text-slate-400 text-sm mb-6">
        Top 10 source countries for permanent residents, 2015–present. Source: IRCC open data.
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
        <div style={{ height: 460 }}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData} margin={{ top: 10, right: 20, left: 10, bottom: 0 }}>
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
                formatter={(v: number, name: string) => [v.toLocaleString('en-CA'), shortName(name)]}
                labelFormatter={l => `Year: ${l}`}
                contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #1e293b', borderRadius: '8px' }}
                labelStyle={{ color: '#cbd5e1', fontSize: 12 }}
                itemStyle={{ fontSize: 11 }}
              />
              <Legend
                formatter={value => shortName(value)}
                wrapperStyle={{ fontSize: 11, color: '#94a3b8' }}
              />
              {topCountries.map((country, i) => (
                <Line
                  key={country}
                  type="monotone"
                  dataKey={country}
                  stroke={COUNTRY_COLORS[i % COUNTRY_COLORS.length]}
                  strokeWidth={2}
                  dot={{ r: 3 }}
                  activeDot={{ r: 5 }}
                  connectNulls
                />
              ))}
            </LineChart>
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
        <p className="text-red-400 text-sm">Failed to load country of origin history.</p>
      </div>
    </div>
  )
}
