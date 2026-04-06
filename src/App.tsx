import { Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Home from './pages/Home'
import Methodology from './pages/Methodology'
import PopulationTrends from './pages/trends/Population'
import VisaStreamTrends from './pages/trends/VisaStreams'
import PermanentResidentTrends from './pages/trends/PermanentResidents'
import CountryOfOriginTrends from './pages/trends/CountryOfOrigin'
import StatusBreakdownTrends from './pages/trends/StatusBreakdown'

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<Home />} />
        <Route path="methodology" element={<Methodology />} />
        <Route path="trends/population" element={<PopulationTrends />} />
        <Route path="trends/visa-streams" element={<VisaStreamTrends />} />
        <Route path="trends/permanent-residents" element={<PermanentResidentTrends />} />
        <Route path="trends/country-of-origin" element={<CountryOfOriginTrends />} />
        <Route path="trends/status-breakdown" element={<StatusBreakdownTrends />} />
      </Route>
    </Routes>
  )
}
