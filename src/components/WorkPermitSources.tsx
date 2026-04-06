import { useData } from '../hooks/useData'
import DataBadge from './DataBadge'

interface WorkPermitSourceRow {
  stream: string
  count: number
  pct: number | null
}

interface WorkPermitSourcesCurrent {
  available: boolean
  ref_year: number | null
  total: number
  streams: WorkPermitSourceRow[]
  source: string
  frequency: string
  note: string
}

const LABELS: Record<string, string> = {
  tfwp: 'Temporary Foreign Worker Program',
  imp: 'International Mobility Program',
  other: 'Other / Unmapped',
}

function formatCount(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(0)}K`
  return n.toLocaleString('en-CA')
}

export default function WorkPermitSources() {
  const { data, loading, error } = useData<WorkPermitSourcesCurrent>('work_permit_sources_current.json')

  if (loading) return <SectionSkeleton />
  if (error || !data) return <SectionError />

  return (
    <section>
      <div className="mb-6">
        <h2 className="text-lg font-semibold text-slate-200">Work Permit Holders by Source</h2>
        <p className="text-slate-500 text-sm mt-1">
          Program split for work permit holders, refreshed automatically when IRCC program-level data is available.
        </p>
      </div>

      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6">
        {data.available ? (
          <>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
              {data.streams.map((row) => (
                <div key={row.stream} className="rounded-lg bg-slate-800/60 p-4">
                  <p className="text-slate-400 text-xs uppercase tracking-wide">
                    {LABELS[row.stream] ?? row.stream}
                  </p>
                  <p className="text-slate-100 text-2xl font-bold mt-2">{formatCount(row.count)}</p>
                  <p className="text-slate-500 text-xs mt-1">
                    {row.pct === null ? 'Share unavailable' : `${row.pct}% of reported work permit holders`}
                  </p>
                </div>
              ))}
            </div>
            <div className="mt-5 pt-4 border-t border-slate-800 flex flex-wrap gap-4">
              <DataBadge source={data.source} period={data.ref_year ? `${data.ref_year} annual rollup` : data.frequency} />
            </div>
          </>
        ) : (
          <div className="rounded-lg border border-slate-800 bg-slate-950/60 p-4">
            <p className="text-slate-300 text-sm">{data.note}</p>
          </div>
        )}
      </div>
    </section>
  )
}

function SectionSkeleton() {
  return (
    <section>
      <div className="h-6 w-64 rounded bg-slate-800 animate-pulse mb-6" />
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6 h-40 animate-pulse" />
    </section>
  )
}

function SectionError() {
  return (
    <section>
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-6">
        <p className="text-red-400 text-sm">Failed to load work permit source data.</p>
      </div>
    </section>
  )
}
