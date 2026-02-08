import { useEffect, useState, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { useWizard } from '../components/WizardContext'
import { ArrowLeft } from 'lucide-react'

type MappingRow = {
  source: string
  defaultTarget: string
  activeTarget: string
  description: string
}

export default function DataTypes() {
  const navigate = useNavigate()
  const { notify } = useWizard()
  const [rows, setRows] = useState<MappingRow[]>([])
  const [overrides, setOverrides] = useState<Record<string, string>>({})
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [resetting, setResetting] = useState(false)

  const fetchMappings = useCallback(async () => {
    setLoading(true)
    try {
      const res = await fetch('/api/datatype/mappings')
      const data = await res.json()
      if (data.ok) {
        setRows(data.rows || [])
        setOverrides(data.overrides || {})
        setError(null)
      } else {
        setError(data.message || 'Unable to load datatype mappings')
      }
    } catch (err: any) {
      setError(err?.message || 'Unable to load datatype mappings')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchMappings()
  }, [fetchMappings])

  const handleOverrideChange = (source: string, value: string) => {
    setOverrides(prev => ({
      ...prev,
      [source]: value
    }))
  }

  const handleSave = async () => {
    setSaving(true)
    try {
      const payload: Record<string, string> = {}
      Object.entries(overrides).forEach(([key, val]) => {
        if (val && val.trim()) {
          payload[key] = val.trim()
        }
      })
      const res = await fetch('/api/session/set-datatype-overrides', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ overrides: payload })
      })
      const data = await res.json()
      if (data.ok) {
        setOverrides(data.overrides || {})
        notify('Datatype overrides saved')
        window.dispatchEvent(new Event('datatypeOverridesUpdated'))
      } else {
        setError(data.message || 'Unable to save overrides')
      }
    } catch (err: any) {
      setError(err?.message || 'Unable to save overrides')
    } finally {
      setSaving(false)
    }
  }

  const handleReset = async () => {
    setResetting(true)
    try {
      const res = await fetch('/api/session/clear-datatype-overrides', {
        method: 'POST'
      })
      const data = await res.json()
      if (data.ok) {
        setOverrides({})
        notify('Datatype overrides reset to defaults')
        window.dispatchEvent(new Event('datatypeOverridesUpdated'))
      } else {
        setError(data.message || 'Unable to reset overrides')
      }
    } catch (err: any) {
      setError(err?.message || 'Unable to reset overrides')
    } finally {
      setResetting(false)
    }
  }

  return (
    <div className="max-w-6xl space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold text-[#085690]">Datatype Mappings</h1>
          <p className="text-sm text-gray-600">
            Review and override Oracle → Databricks datatype mappings before running any DDL conversions.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => navigate('/extract')}
            className="flex items-center gap-2 px-4 py-2 rounded-lg border border-gray-300 text-gray-600 hover:bg-gray-100 transition"
          >
            <ArrowLeft size={16} />
            Back to extraction
          </button>
        </div>
      </div>

      <div className="bg-white rounded-lg shadow p-6 border border-gray-100">
        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
          <div className="text-xs uppercase tracking-wide text-gray-500">
            {rows.length} mappings · {Object.keys(overrides).length} overrides active
          </div>
          <div className="flex flex-wrap gap-2">
            <button
              onClick={handleSave}
              disabled={saving}
              className="px-4 py-2 bg-[#085690] text-white rounded-lg hover:bg-[#064475] transition disabled:opacity-60"
            >
              {saving ? 'Saving...' : 'Save overrides'}
            </button>
            <button
              onClick={handleReset}
              disabled={resetting}
              className="px-4 py-2 border border-[#085690] text-[#085690] rounded-lg hover:bg-[#085690] hover:text-white transition disabled:opacity-60"
            >
              {resetting ? 'Resetting...' : 'Reset to defaults'}
            </button>
          </div>
        </div>
        {error && (
          <p className="mt-2 text-xs text-rose-600">{error}</p>
        )}
      </div>

      <div className="bg-white rounded-lg shadow overflow-hidden border border-gray-100">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm text-gray-700">
            <thead className="bg-gray-50 text-xs uppercase tracking-wide text-gray-500">
              <tr>
                <th className="px-4 py-3 text-left">Source Type</th>
                <th className="px-4 py-3 text-left">Default Target</th>
                <th className="px-4 py-3 text-left">Override (optional)</th>
                <th className="px-4 py-3 text-left">Active Target</th>
                <th className="px-4 py-3 text-left">Description</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {rows.map(row => (
                <tr key={row.source}>
                  <td className="px-4 py-3 font-mono text-xs text-gray-900">{row.source}</td>
                  <td className="px-4 py-3 font-medium text-gray-800">{row.defaultTarget}</td>
                  <td className="px-4 py-3">
                    <input
                      value={overrides[row.source] || ''}
                      onChange={e => handleOverrideChange(row.source, e.target.value)}
                      placeholder={row.defaultTarget}
                      className="w-full px-3 py-2 border border-gray-200 rounded-lg text-xs focus:outline-none focus:border-[#085690]"
                    />
                  </td>
                  <td className="px-4 py-3 font-semibold text-[#085690]">{row.activeTarget}</td>
                  <td className="px-4 py-3 text-xs text-gray-600">{row.description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {loading && (
          <p className="p-4 text-[11px] font-semibold text-gray-500">Loading rows…</p>
        )}
      </div>
    </div>
  )
}
