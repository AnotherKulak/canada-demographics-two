import { useMemo } from 'react'
import { Link } from 'react-router-dom'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend,
} from 'recharts'
import { useData } from '../../hooks/useData'
import DataBadge from '../../components/DataBadge'

interface StatusBreakdownData {
  actual_latest: { rows: { status: string; count: number }[]; total: number }
  estimated_latest: { rows: { status: string; count: number }[]; total: number }
  census_snapshots: { year: number; status: string; count: number }[]
  latest_census_year: number
}

const STATUS_META: Record<string, { label: string; color: string }> = {
  canadian_born:          { label: 'Canadian-born',          color: '#60a5fa' },
  naturalized:            { label: 'Naturalized Citizen',    color: '#4ade80' },
  permanent_resident:     { label: 'Permanent Resident',     color: '#a78bfa' },
  non_permanent_resident: { label: 'Non-Permanent Resident', color: '#fb923c' },
}

const STATUSES = Object.keys(STATUS_META)

export default function StatusBreakdownTrends() {
  const { data, loading, error } = useData<StatusBreakdownData>('status_breakdown.json')

  // Pivot: one object per census year, each status as a key
  const chartData = useMemo(() => {
    if (!data) return []
    const byYear = new Map<number, Record<string, number | string>>()
    for (const row of data.census_snapshots) {
      if (!byYear.has(row.year)) byYear.set(row.year, { year: row.year })
      byYear.get(row.year)![row.status] = row.count
    }
    return Array.from(byYear.values()).sort((a, b) => Number(a.year) - Number(b.year))
  }, [data])

  if (loading) return <PageSkeleton />
  if (error || !data) return <PageError />

  const isSingleSnapshot = chartData.length === 1

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
      <Link to="/" className="text-slate-500 text-sm hover:text-slate-300 transition-colors mb-6 inline-flex items-center gap-1">
        ← Overview
      </Link>
      <h1 className="text-2xl font-bold text-slate-100 mb-2 mt-2">Population by Status — Census Snapshots</h1>
      <p className="text-slate-400 text-sm mb-6">
        Census-to-census comparison. Source: Statistics Canada 98-10-0302-01.
      </p>

      {isSingleSnapshot && (
        <p className="text-slate-600 text-xs mb-6 italic">
          Only the 2021 Census is currently loaded. Prior census years (2016, 2011, …) will appear here as the pipeline adds historical snapshots.
        </p>
      )}

      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6">
        <div style={{ height: 420 }}>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartData} margin={{ top: 10, right: 20, left: 10, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis
                dataKey="year"
                tick={{ fill: '#475569', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                tickFormatter={v => `${(v / 1_000_000).toFixed(0)}M`}
                tick={{ fill: '#475569', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                width={52}
              />
              <Tooltip
                formatter={(v: number, name: string) => [
                  v.toLocaleString('en-CA'),
                  STATUS_META[name]?.label ?? name,
                ]}
                labelFormatter={l => `${l} Census`}
                contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #1e293b', borderRadius: '8px' }}
                labelStyle={{ color: '#cbd5e1', fontSize: 12 }}
                itemStyle={{ fontSize: 11 }}
              />
              <Legend
                formatter={value => STATUS_META[value]?.label ?? value}
                wrapperStyle={{ fontSize: 11, color: '#94a3b8' }}
              />
              {STATUSES.map(s => (
                <Bar
                  key={s}
                  dataKey={s}
                  fill={STATUS_META[s].color}
                  radius={[3, 3, 0, 0]}
                />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>
        <div className="mt-4 pt-4 border-t border-slate-800">
          <DataBadge source="Statistics Canada 98-10-0302-01" period="Census (every 5 years)" />
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
      <div className="h-4 w-72 rounded bg-slate-800 animate-pulse mb-8" />
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6 h-96 animate-pulse" />
    </div>
  )
}

function PageError() {
  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6">
        <p className="text-red-400 text-sm">Failed to load status breakdown data.</p>
      </div>
    </div>
  )
}
