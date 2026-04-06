import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts'
import { useData } from '../hooks/useData'
import DataBadge from './DataBadge'

interface CountryRow {
  country: string
  count: number
}

interface OriginCategory {
  category: string
  label: string
  total: number
  top10: CountryRow[]
  non_top_10: number
  source_period: string
  estimation_basis: string
  is_estimated: boolean
}

interface OriginOverview {
  categories: OriginCategory[]
  methodology: {
    summary: string
    foreign_born_definition: string
  }
}

const COLORS = ['#60a5fa', '#fb923c', '#facc15', '#4ade80', '#f472b6', '#38bdf8', '#a78bfa', '#f87171', '#34d399', '#fbbf24', '#64748b']

function formatCount(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(0)}K`
  return n.toLocaleString('en-CA')
}

export default function CountryOriginPanel() {
  const [selectedCategory, setSelectedCategory] = useState('foreign_born_total')
  const { data, loading, error } = useData<OriginOverview>('origin_overview.json')

  const activeCategory = useMemo(
    () => data?.categories.find((category) => category.category === selectedCategory) ?? data?.categories[0],
    [data, selectedCategory]
  )

  if (loading) return <SectionSkeleton />
  if (error || !data || !activeCategory) return <SectionError />

  const chartRows = [...activeCategory.top10]
  if (activeCategory.non_top_10 > 0) {
    chartRows.push({ country: 'Non-top-10', count: activeCategory.non_top_10 })
  }

  return (
    <section>
      <div className="flex items-start justify-between gap-4 mb-6">
        <div>
          <h2 className="text-lg font-semibold text-slate-200">Country of Origin</h2>
          <p className="text-slate-500 text-sm mt-1">
            Category totals reconcile to top 10 countries plus a non-top-10 remainder.
          </p>
        </div>
        <Link to="/trends/country-of-origin" className="text-xs text-maple-500 hover:text-maple-400 transition-colors shrink-0 mt-1">
          View historical →
        </Link>
      </div>

      <div className="rounded-xl border border-slate-800 bg-slate-900 overflow-hidden">
        <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-px bg-slate-800">
          {data.categories.map((category) => (
            <button
              key={category.category}
              onClick={() => setSelectedCategory(category.category)}
              className={`bg-slate-900 p-4 text-left transition-colors ${
                selectedCategory === category.category ? 'ring-1 ring-inset ring-maple-500' : 'hover:bg-slate-950'
              }`}
            >
              <p className="text-slate-400 text-xs uppercase tracking-wide">{category.label}</p>
              <p className="text-slate-100 text-2xl font-bold mt-2">{formatCount(category.total)}</p>
            </button>
          ))}
        </div>

        <div className="p-6">
          <div className="mb-5">
            <h3 className="text-slate-100 text-base font-semibold">{activeCategory.label}</h3>
            <p className="text-slate-500 text-sm mt-1">{activeCategory.estimation_basis}</p>
          </div>

          <div style={{ height: Math.max(chartRows.length * 38, 220) }}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartRows} layout="vertical" margin={{ top: 0, right: 80, left: 0, bottom: 0 }}>
                <XAxis type="number" tick={{ fill: '#475569', fontSize: 11 }} tickFormatter={formatCount} axisLine={false} tickLine={false} />
                <YAxis type="category" dataKey="country" width={170} tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false} />
                <Tooltip
                  formatter={(value: number) => [value.toLocaleString('en-CA'), 'People']}
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #1e293b', borderRadius: '8px' }}
                  labelStyle={{ color: '#cbd5e1', fontSize: 12 }}
                  itemStyle={{ color: '#94a3b8' }}
                />
                <Bar dataKey="count" radius={[0, 4, 4, 0]} label={{ position: 'right', fill: '#64748b', fontSize: 11, formatter: (value: number) => formatCount(value) }}>
                  {chartRows.map((_, index) => <Cell key={index} fill={COLORS[index % COLORS.length]} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="mt-5 pt-4 border-t border-slate-800 space-y-2">
            <DataBadge source={activeCategory.is_estimated ? 'Estimated category total' : 'Published category total'} period={activeCategory.source_period} />
            <p className="text-slate-600 text-xs">{data.methodology.summary}</p>
            <p className="text-slate-700 text-xs">{data.methodology.foreign_born_definition}</p>
          </div>
        </div>
      </div>
    </section>
  )
}

function SectionSkeleton() {
  return (
    <section>
      <div className="h-6 w-56 rounded bg-slate-800 animate-pulse mb-6" />
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6 h-96 animate-pulse" />
    </section>
  )
}

function SectionError() {
  return (
    <section>
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6">
        <p className="text-red-400 text-sm">Failed to load country of origin data.</p>
      </div>
    </section>
  )
}
