import { Link } from 'react-router-dom'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
} from 'recharts'
import { useData } from '../hooks/useData'
import DataBadge from './DataBadge'

interface VisaStreamsCurrent {
  ref_date: string
  total_npr: number
  streams: { stream: string; count: number; pct: number }[]
  source: string
  frequency: string
}

// ── metadata for all raw streams ────────────────────────────────────────────
const STREAM_META: Record<string, { label: string; color: string; group: 'work' | 'study' | 'other' | 'asylum' }> = {
  permit_work_only:        { label: 'Work Permit (only)',           color: '#60a5fa', group: 'work'   },
  permit_study_only:       { label: 'Study Permit (only)',          color: '#818cf8', group: 'study'  },
  permit_work_and_study:   { label: 'Work & Study Permit',          color: '#a78bfa', group: 'work'   },
  permit_other:            { label: 'Other Permit Holders',         color: '#c4b5fd', group: 'other'  },
  asylum_work_permit_only: { label: 'Asylum — Work Permit',         color: '#fb923c', group: 'asylum' },
  asylum_study_permit_only:{ label: 'Asylum — Study Permit',        color: '#fbbf24', group: 'asylum' },
  asylum_work_and_study:   { label: 'Asylum — Work & Study Permit', color: '#fcd34d', group: 'asylum' },
  asylum_no_permit:        { label: 'Asylum — No Permit',           color: '#fde68a', group: 'asylum' },
}

// ── breakdown groups shown below the main chart ──────────────────────────────
const BREAKDOWN_GROUPS = [
  { key: 'asylum', label: 'Asylum Claimants', streams: ['asylum_no_permit', 'asylum_work_permit_only', 'asylum_study_permit_only', 'asylum_work_and_study'] },
  { key: 'work',   label: 'Work Permit Holders', streams: ['permit_work_only', 'permit_work_and_study'] },
  { key: 'other',  label: 'Other Permit Holders', streams: ['permit_other'] },
]

function formatCount(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`
  if (n >= 1_000)     return `${(n / 1_000).toFixed(0)}K`
  return n.toLocaleString()
}

function formatRefDate(dateStr: string): string {
  return new Date(dateStr).toLocaleDateString('en-CA', { year: 'numeric', month: 'long' })
}

export default function VisaStreams() {
  const { data, loading, error } = useData<VisaStreamsCurrent>('visa_streams_current.json')

  if (loading) return <SectionSkeleton />
  if (error || !data) return <SectionError />

  const streamMap = Object.fromEntries(data.streams.map(s => [s.stream, s.count]))

  // ── main chart: non-asylum streams + one combined asylum bar ────────────────
  const asylumTotal = ['asylum_no_permit', 'asylum_work_permit_only', 'asylum_study_permit_only', 'asylum_work_and_study']
    .reduce((sum, k) => sum + (streamMap[k] ?? 0), 0)

  const mainChart = [
    ...data.streams
      .filter(s => STREAM_META[s.stream]?.group !== 'asylum')
      .map(s => ({ label: STREAM_META[s.stream]?.label ?? s.stream, count: s.count, color: STREAM_META[s.stream]?.color ?? '#64748b' })),
    { label: 'Asylum Claimants', count: asylumTotal, color: '#fb923c' },
  ].sort((a, b) => b.count - a.count)

  return (
    <section>
      <div className="flex items-start justify-between gap-4 mb-6">
        <div>
          <h2 className="text-lg font-semibold text-slate-200">
            Temporary Residents by Visa Stream
          </h2>
          <p className="text-slate-500 text-sm mt-1">
            {formatCount(data.total_npr)} non-permanent residents as of{' '}
            {formatRefDate(data.ref_date)}.
          </p>
        </div>
        <Link to="/trends/visa-streams" className="text-xs text-maple-500 hover:text-maple-400 transition-colors shrink-0 mt-1">
          View historical →
        </Link>
      </div>

      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6">
        {/* Main bar chart */}
        <div style={{ height: Math.max(mainChart.length * 44, 200) }}>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={mainChart}
              layout="vertical"
              margin={{ top: 0, right: 80, left: 0, bottom: 0 }}
            >
              <XAxis
                type="number"
                tick={{ fill: '#475569', fontSize: 11 }}
                tickFormatter={formatCount}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                type="category"
                dataKey="label"
                width={190}
                tick={{ fill: '#94a3b8', fontSize: 12 }}
                axisLine={false}
                tickLine={false}
              />
              <Tooltip
                formatter={(value: number) => [value.toLocaleString('en-CA'), 'Residents']}
                contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #1e293b', borderRadius: '8px' }}
                labelStyle={{ color: '#cbd5e1', fontSize: 12 }}
                itemStyle={{ color: '#94a3b8' }}
              />
              <Bar dataKey="count" radius={[0, 4, 4, 0]} label={{ position: 'right', fill: '#64748b', fontSize: 11, formatter: (v: number) => formatCount(v) }}>
                {mainChart.map((entry, i) => (
                  <Cell key={i} fill={entry.color} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Breakdown panels */}
        <div className="mt-6 pt-6 border-t border-slate-800">
          <p className="text-xs text-slate-500 mb-4">Sub-type breakdown</p>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-6">
            {BREAKDOWN_GROUPS.map(group => {
              const rows = group.streams
                .map(k => ({ label: STREAM_META[k]?.label ?? k, count: streamMap[k] ?? 0, color: STREAM_META[k]?.color ?? '#64748b' }))
                .sort((a, b) => b.count - a.count)
              const groupTotal = rows.reduce((s, r) => s + r.count, 0)

              return (
                <div key={group.key}>
                  <p className="text-xs font-medium text-slate-400 mb-3">{group.label}</p>
                  <div className="space-y-2">
                    {rows.map(row => (
                      <div key={row.label}>
                        <div className="flex justify-between text-xs text-slate-500 mb-1">
                          <span>{row.label}</span>
                          <span>{formatCount(row.count)}</span>
                        </div>
                        <div className="h-1.5 rounded-full bg-slate-800 overflow-hidden">
                          <div
                            className="h-full rounded-full"
                            style={{
                              width: `${groupTotal > 0 ? (row.count / groupTotal) * 100 : 0}%`,
                              backgroundColor: row.color,
                            }}
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )
            })}
          </div>
        </div>

        {/* Source */}
        <div className="mt-6 pt-4 border-t border-slate-800">
          <DataBadge source={data.source} period={`Q ending ${formatRefDate(data.ref_date)}`} />
        </div>
      </div>
    </section>
  )
}

function SectionSkeleton() {
  return (
    <section>
      <div className="h-6 w-64 rounded bg-slate-800 animate-pulse mb-6" />
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6 h-96 animate-pulse" />
    </section>
  )
}

function SectionError() {
  return (
    <section>
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6">
        <p className="text-red-400 text-sm">Failed to load visa stream data.</p>
      </div>
    </section>
  )
}
