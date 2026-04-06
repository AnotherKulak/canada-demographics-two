import HeroTotal from '../components/HeroTotal'
import StatusBreakdown from '../components/StatusBreakdown'
import VisaStreams from '../components/VisaStreams'
import WorkPermitSources from '../components/WorkPermitSources'
import CountryOriginPanel from '../components/CountryOriginPanel'

export default function Home() {
  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12 space-y-16">
      <HeroTotal />
      <StatusBreakdown />
      <VisaStreams />
      <WorkPermitSources />
      <CountryOriginPanel />
    </div>
  )
}
