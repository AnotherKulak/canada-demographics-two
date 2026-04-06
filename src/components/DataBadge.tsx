interface DataBadgeProps {
  source: string
  period: string
  className?: string
}

/**
 * Displays the data source and time period for any chart or stat.
 * Required on every displayed data point per project requirements.
 */
export default function DataBadge({ source, period, className = '' }: DataBadgeProps) {
  return (
    <div className={`flex items-center gap-2 flex-wrap ${className}`}>
      <span className="inline-flex items-center gap-1 text-xs text-slate-500">
        <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round"
            d="M12 6.042A8.967 8.967 0 0 0 6 3.75c-1.052 0-2.062.18-3 .512v14.25A8.987 8.987 0 0 1 6 18c2.305 0 4.408.867 6 2.292m0-14.25a8.966 8.966 0 0 1 6-2.292c1.052 0 2.062.18 3 .512v14.25A8.987 8.987 0 0 0 18 18a8.967 8.967 0 0 0-6 2.292m0-14.25v14.25" />
        </svg>
        {source}
      </span>
      <span className="text-slate-700 text-xs">·</span>
      <span className="inline-flex items-center gap-1 text-xs text-slate-500">
        <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round"
            d="M6.75 3v2.25M17.25 3v2.25M3 18.75V7.5a2.25 2.25 0 0 1 2.25-2.25h13.5A2.25 2.25 0 0 1 21 7.5v11.25m-18 0A2.25 2.25 0 0 0 5.25 21h13.5A2.25 2.25 0 0 0 21 18.75m-18 0v-7.5A2.25 2.25 0 0 1 5.25 9h13.5A2.25 2.25 0 0 1 21 11.25v7.5" />
        </svg>
        {period}
      </span>
    </div>
  )
}
