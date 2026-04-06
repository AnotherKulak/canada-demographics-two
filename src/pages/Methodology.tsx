import DataBadge from '../components/DataBadge'
import { useData } from '../hooks/useData'

interface StatusMethodology {
  methodology: {
    summary: string
    actual_definition: string
    estimated_definition: string
  }
}

interface OriginMethodology {
  methodology: {
    summary: string
    foreign_born_definition: string
  }
}

const sources = [
  { name: 'Statistics Canada — Population Estimates, Quarterly', table: '17-10-0009-01', frequency: 'Quarterly', usedFor: 'Total Canadian population and estimate anchor' },
  { name: 'Statistics Canada — Non-Permanent Residents by Type', table: '17-10-0121-01', frequency: 'Quarterly', usedFor: 'Current temporary resident totals and stream detail' },
  { name: 'Statistics Canada — Immigrant Status and Period of Immigration', table: '98-10-0302-01', frequency: 'Census (every 5 years)', usedFor: 'Resident status mix and naturalized country-of-birth distribution' },
  { name: 'IRCC — Study Permit Holders', table: 'IRCC-90115b00', frequency: 'Monthly', usedFor: 'Temporary resident country mix and pre-quarterly history fallback' },
  { name: 'IRCC — Work Permit Holders', table: 'IRCC-360024f2', frequency: 'Monthly', usedFor: 'Temporary resident country mix, pre-quarterly history fallback, and program splits' },
  { name: 'IRCC — Asylum Claimants', table: 'IRCC-b6cbcf4d', frequency: 'Monthly', usedFor: 'Temporary resident country mix and pre-quarterly history fallback' },
  { name: 'IRCC — Permanent Residents', table: 'IRCC-f7e5498e', frequency: 'Monthly', usedFor: 'Permanent resident country mix and historical admissions' },
]

export default function Methodology() {
  const { data: statusData } = useData<StatusMethodology>('status_breakdown.json')
  const { data: originData } = useData<OriginMethodology>('origin_overview.json')

  return (
    <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
      <h1 className="text-2xl font-bold text-slate-100 mb-2">Methodology & Data Sources</h1>
      <p className="text-slate-400 text-sm mb-10">
        The frontend reads stable JSON exports from the local pipeline. Every successful refresh fetches the newest available source releases, replaces older warehouse slices, and regenerates the latest payloads used by the home page.
      </p>

      <h2 className="text-base font-semibold text-slate-200 mb-4 uppercase tracking-wider text-xs">Automated Estimation</h2>
      <div className="space-y-3 mb-12">
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-5">
          <p className="text-slate-200 text-sm font-medium mb-2">Population by Status</p>
          <p className="text-slate-400 text-sm">{statusData?.methodology.summary}</p>
          <p className="text-slate-500 text-xs mt-3">{statusData?.methodology.estimated_definition}</p>
          <p className="text-slate-600 text-xs mt-1">{statusData?.methodology.actual_definition}</p>
        </div>
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-5">
          <p className="text-slate-200 text-sm font-medium mb-2">Country of Origin</p>
          <p className="text-slate-400 text-sm">{originData?.methodology.summary}</p>
          <p className="text-slate-500 text-xs mt-3">{originData?.methodology.foreign_born_definition}</p>
        </div>
      </div>

      <h2 className="text-base font-semibold text-slate-200 mb-4 uppercase tracking-wider text-xs">Data Sources</h2>
      <div className="space-y-3">
        {sources.map((source) => (
          <div key={source.table} className="rounded-lg border border-slate-800 bg-slate-900 p-5">
            <div className="flex items-start justify-between gap-4">
              <div>
                <p className="text-slate-200 text-sm font-medium">{source.name}</p>
                <p className="text-slate-500 text-xs mt-1">{source.usedFor}</p>
              </div>
              <DataBadge source={source.table} period={source.frequency} className="shrink-0" />
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
