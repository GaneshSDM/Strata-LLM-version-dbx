import { useEffect, useState } from 'react'
import { ArrowLeft, RefreshCcw, Trash2 } from 'lucide-react'
import { useNavigate } from 'react-router-dom'
import { WizardStepPath } from '../components/WizardContext'

type MigrationRecord = {
  id: number
  created_at?: string
  started_at?: string
  structure_started_at?: string
  data_completed_at?: string
  completed_at?: string
  duration_ms?: number | null
  structure_data_duration_ms?: number | null
  source_name?: string
  source_type?: string
  target_name?: string
  target_type?: string
  status: string
  migrated_rows?: number | null
  failed_rows?: number | null
  table_count?: number | null
}

function formatDuration(ms?: number | null) {
  if (!ms && ms !== 0) return '—'
  if (ms < 1000) return `${ms} ms`
  const seconds = Math.round(ms / 1000)
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60)
  const rem = seconds % 60
  return `${minutes}m ${rem}s`
}

function getStructureToDataDurationMs(run: MigrationRecord): number | null {
  if (run.structure_data_duration_ms || run.structure_data_duration_ms === 0) {
    return run.structure_data_duration_ms
  }
  if (!run.structure_started_at || !run.data_completed_at) return null
  const start = Date.parse(run.structure_started_at)
  const end = Date.parse(run.data_completed_at)
  if (!Number.isFinite(start) || !Number.isFinite(end)) return null
  const ms = end - start
  return ms >= 0 ? ms : null
}

export default function MigrationHistory({ lastWizardPath }: { lastWizardPath: WizardStepPath }) {
  const navigate = useNavigate()
  const [records, setRecords] = useState<MigrationRecord[]>([])
  const [loading, setLoading] = useState(false)
  const [clearingAll, setClearingAll] = useState(false)
  const [clearingId, setClearingId] = useState<number | null>(null)

  const fetchHistory = async () => {
    setLoading(true)
    try {
      const res = await fetch('/api/migrations/history')
      const data = await res.json()
      if (data.ok) {
        setRecords(data.data || [])
      }
    } catch (err) {
      console.error('Failed to load migration history', err)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchHistory()
  }, [])

  const clearAll = async () => {
    setClearingAll(true)
    try {
      const res = await fetch('/api/migrations/history', { method: 'DELETE' })
      const data = await res.json()
      if (data.ok) {
        setRecords([])
      } else {
        console.error('Failed to clear migration history', data?.message)
      }
    } catch (err) {
      console.error('Failed to clear migration history', err)
    } finally {
      setClearingAll(false)
    }
  }

  const clearOne = async (id: number) => {
    setClearingId(id)
    try {
      const res = await fetch(`/api/migrations/history/${id}`, { method: 'DELETE' })
      const data = await res.json()
      if (data.ok) {
        setRecords(prev => prev.filter(r => r.id !== id))
      } else {
        console.error('Failed to delete migration history entry', data?.message)
      }
    } catch (err) {
      console.error('Failed to delete migration history entry', err)
    } finally {
      setClearingId(null)
    }
  }

  // Live refresh while a run is still in progress
  useEffect(() => {
    const hasActive = records.some((r) =>
      ['started', 'structure_in_progress', 'structure_complete', 'data_in_progress'].includes((r.status || '').toLowerCase())
    )
    if (!hasActive) return
    const id = setInterval(fetchHistory, 3000)
    return () => clearInterval(id)
  }, [records])

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-3xl font-bold text-[#085690] mb-1">MIGRATION HISTORY</h1>
        </div>
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={clearAll}
            disabled={clearingAll || records.length === 0}
            className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border border-rose-200 bg-rose-50 text-sm font-semibold text-rose-700 hover:bg-rose-100 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <Trash2 size={16} className={clearingAll ? 'animate-pulse' : ''} />
            Clear All
          </button>
          <button
            type="button"
            onClick={() => navigate(lastWizardPath)}
            className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border-2 border-[#085690] bg-white text-sm font-semibold text-[#085690] hover:bg-[#085690] hover:text-white transition-all"
          >
            <ArrowLeft size={16} />
            Back to Workflow
          </button>
          <button
            onClick={fetchHistory}
            className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 bg-white text-sm font-semibold text-gray-700 hover:bg-gray-50"
          >
            <RefreshCcw size={16} />
            Refresh
          </button>
        </div>
      </div>

      <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
        <div className="max-h-[70vh] overflow-auto">
          <table className="min-w-full">
            <thead className="bg-gray-50 sticky top-0 z-10">
            <tr>
              <th className="py-3 px-4 text-left text-sm font-semibold text-gray-700 border-b border-gray-200 w-16">SL.No</th>
              <th className="py-3 px-4 text-left text-sm font-semibold text-gray-700 border-b border-gray-200">SOURCE DETAILS</th>
              <th className="py-3 px-4 text-left text-sm font-semibold text-gray-700 border-b border-gray-200">TARGET DETAILS</th>
              <th className="py-3 px-4 text-left text-sm font-semibold text-gray-700 border-b border-gray-200">NO.OF TABLE</th>
              <th className="py-3 px-4 text-left text-sm font-semibold text-gray-700 border-b border-gray-200">NO.OF RECORDS</th>
              <th className="py-3 px-4 text-left text-sm font-semibold text-gray-700 border-b border-gray-200">TIME</th>
              <th className="py-3 px-4 text-left text-sm font-semibold text-gray-700 border-b border-gray-200">TIME TAKEN</th>
              <th className="py-3 px-4 text-left text-sm font-semibold text-gray-700 border-b border-gray-200">STATUS</th>
              <th className="py-3 px-4 text-left text-sm font-semibold text-gray-700 border-b border-gray-200 w-20">ACTION</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200">
            {records.map((run, index) => (
              <tr key={run.id} className="hover:bg-gray-50">
                <td className="py-3 px-4 text-sm text-gray-700">{index + 1}</td>
                <td className="py-3 px-4 text-sm text-gray-700">
                  <div className="font-medium">{run.source_name || 'N/A'}</div>
                  <div className="text-xs text-gray-500">{run.source_type || 'N/A'}</div>
                </td>
                <td className="py-3 px-4 text-sm text-gray-700">
                  <div className="font-medium">{run.target_name || 'N/A'}</div>
                  <div className="text-xs text-gray-500">{run.target_type || 'N/A'}</div>
                </td>
                <td className="py-3 px-4 text-sm text-gray-700">{run.table_count ?? 'N/A'}</td>
                <td className="py-3 px-4 text-sm text-gray-700">{run.migrated_rows ?? 0}</td>
                <td className="py-3 px-4 text-sm text-gray-700">{run.started_at ? new Date(run.started_at).toLocaleDateString('en-GB', { day: '2-digit', month: '2-digit', year: 'numeric' }) + ' ' + new Date(run.started_at).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', hour12: false }) : 'N/A'}</td>
                <td className="py-3 px-4 text-sm text-gray-700">{formatDuration(getStructureToDataDurationMs(run))}</td>
                <td className="py-3 px-4 text-sm">
                  <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                    run.status?.toLowerCase() === 'success' || run.status?.toLowerCase().includes('success') ? 'bg-emerald-100 text-emerald-700' :
                    'bg-rose-100 text-rose-700'
                  }`}>
                    {(run.status?.toLowerCase() === 'success' || run.status?.toLowerCase().includes('success')) ? 'SUCCESS' : 'FAILED'}
                  </span>
                </td>
                <td className="py-3 px-4 text-sm">
                  <button
                    type="button"
                    onClick={() => clearOne(run.id)}
                    disabled={clearingId === run.id || clearingAll}
                    className="inline-flex items-center justify-center w-8 h-8 rounded-md border border-gray-200 bg-white text-gray-600 hover:bg-rose-50 hover:text-rose-700 disabled:opacity-50 disabled:cursor-not-allowed"
                    title="Clear"
                    aria-label="Clear"
                  >
                    <Trash2 size={16} className={clearingId === run.id ? 'animate-pulse' : ''} />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        </div>
      </div>

      {records.length === 0 && !loading && (
        <div className="text-center text-sm text-gray-600 mt-10">No migrations recorded yet.</div>
      )}
    </div>
  )
}
