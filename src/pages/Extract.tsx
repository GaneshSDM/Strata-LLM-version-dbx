import { useState, useEffect, useMemo, useCallback } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import { ArrowLeft, FileText, FileSpreadsheet, FileJson, Database, Table, Eye, Zap, Hash, Box, Lock, Code, RefreshCw } from 'lucide-react'
import { useWizard } from '../components/WizardContext'
import { ensureSessionId, getSessionHeaders } from '../utils/session'

type ExtractProps = {
  onExtractionComplete: () => void
}

export default function Extract({ onExtractionComplete }: ExtractProps) {
  const navigate = useNavigate()
  const location = useLocation()
  const isVisible = location.pathname === '/extract'
  const { wizardResetId, analyzeMetrics } = useWizard()
  const [extracting, setExtracting] = useState(false)
  const [status, setStatus] = useState<any>(null)
  const [activeTab, setActiveTab] = useState('overview')
  const [hasReportedCompletion, setHasReportedCompletion] = useState(false)
  const [activeDdl, setActiveDdl] = useState<{ title: string; ddl: string; cacheKey: string } | null>(null)
  const [targetDdlCache, setTargetDdlCache] = useState<Record<string, { ddl: string; loading: boolean; error?: string }>>({})
  const [sessionInfo, setSessionInfo] = useState<{ sourceType?: string; targetType?: string } | null>(null)
  // New UI flag – when true we ask the backend to execute the translated DDL in the target DB.
  const [runInTarget, setRunInTarget] = useState<boolean>(false)
  const [datatypeRows, setDatatypeRows] = useState<any[]>([])
  const [datatypeOverrides, setDatatypeOverrides] = useState<Record<string, string>>({})
  const [datatypeLoading, setDatatypeLoading] = useState(false)
  const [datatypeError, setDatatypeError] = useState<string | null>(null)
  const [structureStatus, setStructureStatus] = useState<any>(null)
  const [startingStructure, setStartingStructure] = useState(false)
  const [structureNotification, setStructureNotification] = useState<string | null>(null)
  const [autoTriggerState, setAutoTriggerState] = useState<'preparing' | 'triggered' | 'idle'>('preparing')
  const [autoTriggerCountdown, setAutoTriggerCountdown] = useState(8)
  const datatypesSignature = useMemo(() => JSON.stringify(datatypeOverrides), [datatypeOverrides])

  const startExtraction = async () => {
    await ensureSessionId()
    await fetch('/api/extract/start', {
      method: 'POST',
      headers: getSessionHeaders()
    })
    setExtracting(true)
    setHasReportedCompletion(false)
  }

  useEffect(() => {
    if (!extracting) return
    const interval = setInterval(async () => {
      const res = await fetch('/api/extract/status')
      const data = await parseJsonResponse(res)
      setStatus(data)
      if (data.done) {
        setExtracting(false)
        clearInterval(interval)
      }
    }, 1000)
    return () => clearInterval(interval)
  }, [extracting])

  const exportReport = async (format: 'pdf' | 'excel' | 'json') => {
    const endpoint = `/api/extract/export/${format}`
    window.open(endpoint, '_blank')
  }

  const results = status?.results
  const backendSummary = results?.extraction_summary || {}
  // Use analyze-page filtered metrics when available, falling back to backend summary
  const summary = analyzeMetrics
    ? { ...backendSummary, ...analyzeMetrics }
    : backendSummary
  const extractionSteps = [
    { label: 'Connecting to source', start: 0, end: 10 },
    { label: 'Reading schema metadata', start: 10, end: 35 },
    { label: 'Extracting DDL scripts', start: 35, end: 60 },
    { label: 'Packaging results', start: 60, end: 85 },
    { label: 'Finalizing extraction', start: 85, end: 100 }
  ]
  const extractionPercent = status?.percent ?? 0

  useEffect(() => {
    if (status?.done && !hasReportedCompletion) {
      onExtractionComplete()
      setHasReportedCompletion(true)
    }
  }, [status?.done, hasReportedCompletion, onExtractionComplete])

  useEffect(() => {
    setExtracting(false)
    setStatus(null)
    setActiveTab('overview')
    setHasReportedCompletion(false)
    // Reset auto-trigger state
    setAutoTriggerState('preparing')
    setAutoTriggerCountdown(8)
  }, [wizardResetId])

  // Auto-trigger extraction only when the page is actually visible
  useEffect(() => {
    if (!isVisible) return
    if (extracting || status?.done || autoTriggerState !== 'preparing') return

    setAutoTriggerCountdown(8)

    // Countdown interval (updates UI every second)
    const countdownInterval = setInterval(() => {
      setAutoTriggerCountdown((prev) => {
        if (prev <= 1) {
          clearInterval(countdownInterval)
          return 0
        }
        return prev - 1
      })
    }, 1000)

    // Auto-trigger timeout (8 seconds)
    const autoTriggerTimeout = setTimeout(() => {
      startExtraction()
      setAutoTriggerState('triggered')
    }, 8000)

    // Cleanup function prevents memory leaks
    return () => {
      clearInterval(countdownInterval)
      clearTimeout(autoTriggerTimeout)
    }
  }, [isVisible]) // Only trigger when page becomes visible

  // Scroll to top when Extract page becomes visible
  useEffect(() => {
    if (isVisible) {
      // The scrollable container is the <main> element in Layout, not the window
      const mainEl = document.querySelector('main')
      if (mainEl) {
        mainEl.scrollTo(0, 0)
      }
      window.scrollTo(0, 0)
    }
  }, [isVisible])

  useEffect(() => {
    const loadSession = async () => {
      try {
        const res = await fetch('/api/session')
        const data = await parseJsonResponse(res)
        const sourceType = data?.data?.source?.db_type
        const targetType = data?.data?.target?.db_type
        setSessionInfo({ sourceType, targetType })
      } catch (err) {
        setSessionInfo(null)
      }
    }
    loadSession()
  }, [])

  const parseJsonResponse = useCallback(async (res: Response) => {
    const text = await res.text()
    if (!text) {
      return { ok: false, message: `HTTP ${res.status}` }
    }
    try {
      return JSON.parse(text)
    } catch {
      return { ok: false, message: text }
    }
  }, [])
  const loadDatatypeMappings = useCallback(async () => {
    setDatatypeLoading(true)
    try {
      const res = await fetch('/api/datatype/mappings')
      const data = await parseJsonResponse(res)
      if (data.ok) {
        setDatatypeRows(data.rows || [])
        setDatatypeOverrides(data.overrides || {})
        setDatatypeError(null)
      } else {
        setDatatypeError(data.message || 'Unable to load datatype mappings')
      }
    } catch (err: any) {
      setDatatypeError(err?.message || 'Unable to load datatype mappings')
    } finally {
      setDatatypeLoading(false)
    }
  }, [parseJsonResponse])

  const fetchStructureStatus = useCallback(async () => {
    try {
      const res = await fetch('/api/migrate/structure-status')
      const data = await parseJsonResponse(res)
      if (data) {
        setStructureStatus(data)
      }
    } catch {
      // keep last known status if fetching fails
    }
  }, [parseJsonResponse])

  useEffect(() => {
    loadDatatypeMappings()
    const handler = () => {
      loadDatatypeMappings()
    }
    window.addEventListener('datatypeOverridesUpdated', handler)
    return () => window.removeEventListener('datatypeOverridesUpdated', handler)
  }, [loadDatatypeMappings])

  useEffect(() => {
    fetchStructureStatus()
    const interval = setInterval(() => {
      fetchStructureStatus()
    }, 2500)
    return () => clearInterval(interval)
  }, [fetchStructureStatus])

  const startStructureMigration = async () => {
    if (structureStatus?.status === 'running') return
    setStructureNotification(null)
    setStartingStructure(true)
    try {
      const res = await fetch('/api/migrate/structure', {
        method: 'POST'
      })
      const data = await parseJsonResponse(res)
      if (!data.ok) {
        setStructureNotification(data.message || 'Unable to start structure migration')
      }
      await fetchStructureStatus()
    } catch (err: any) {
      setStructureNotification(err?.message || 'Unable to start structure migration')
    } finally {
      setStartingStructure(false)
    }
  }
  const fetchTargetDdl = async (params: { cacheKey: string; sourceDdl: string; name?: string; kind?: string; schema?: string }) => {
    console.log('Starting conversion for:', params.cacheKey);
    setTargetDdlCache(prev => ({
      ...prev,
      [params.cacheKey]: { ddl: prev[params.cacheKey]?.ddl || '', loading: true }
    }))
    try {
      const res = await fetch('/api/ddl/convert', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sourceDialect: sessionInfo?.sourceType || 'Oracle',
          targetDialect: sessionInfo?.targetType || 'Databricks',
          sourceDdl: params.sourceDdl,
          objectName: params.name || params.cacheKey,
          objectKind: params.kind,
          datatypeOverrides: datatypeOverrides,
          // Pass the execution flag – backend will run the DDL if true.
          execute: runInTarget
        })
      })
      
      // Check if response is ok before parsing
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${res.statusText}`)
      }
      
      const data = await parseJsonResponse(res)
        if (data.ok) {
          // Preserve execution status in the cache for UI display.
          const execInfo = {
            executed: data.executed ?? false,
            execution_error: data.execution_error ?? null
          }
          console.log('Conversion successful for:', params.cacheKey, 'Target SQL length:', (data.target_sql || '').length);
          setTargetDdlCache(prev => ({
            ...prev,
            [params.cacheKey]: {
              ddl: data.target_sql || '',
              loading: false,
              // Attach execution info as optional fields (will be read elsewhere).
              ...execInfo
            }
          }))
        } else {
          setTargetDdlCache(prev => ({ ...prev, [params.cacheKey]: { ddl: '', loading: false, error: data.message || 'Conversion failed' } }))
        }
    } catch (err: any) {
      console.error('Target DDL conversion error for:', params.cacheKey, err)
      setTargetDdlCache(prev => ({ ...prev, [params.cacheKey]: { ddl: '', loading: false, error: err?.message || 'Conversion failed' } }))
    }
  }
  useEffect(() => {
    if (!activeDdl?.ddl) return
    const key = activeDdl.cacheKey
    const cached = targetDdlCache[key]
    if (cached?.ddl || cached?.loading) return
    fetchTargetDdl({ cacheKey: key, sourceDdl: activeDdl.ddl, name: activeDdl.title })
  }, [activeDdl, targetDdlCache, sessionInfo, datatypeOverrides])

  useEffect(() => {
    setTargetDdlCache({})
  }, [datatypesSignature])

  // Automatically pre-convert visible DDLs in the current tab so Target DDL Preview
  // shows up without requiring a button click.
  useEffect(() => {
    if (!results?.ddl_scripts || !sessionInfo) return
    const scripts = results.ddl_scripts
    const itemsByTab: Record<string, any[]> = {
      tables: scripts.tables || [],
      views: scripts.views || [],
      triggers: scripts.triggers || [],
      procedures: scripts.procedures || [],
      functions: scripts.functions || [],
      constraints: scripts.constraints || [],
      indexes: scripts.indexes || [],
      types: scripts.user_types || [],
      sequences: scripts.sequences || [],
      grants: scripts.grants || [],
      validations: scripts.validation_scripts || []
    }

    const items = itemsByTab[activeTab] || []
    items.forEach((item: any) => {
      const schema = item.schema
      const name =
        activeTab === 'validations'
          ? item.table
          : item.name || item.object || item.table
      const ddl = item.ddl || item.sql || ''
      if (!ddl || !schema || !name) return

      const kind = ddlKindMap[activeTab] || 'object'
      const cacheKey = buildCacheKey(kind, schema, name)
      const cached = targetDdlCache[cacheKey]
      if (cached?.ddl || cached?.loading) return

      fetchTargetDdl({
        cacheKey,
        sourceDdl: ddl,
        name,
        kind,
        schema
      })
    })
  }, [activeTab, results, targetDdlCache, sessionInfo])

  const MetricCard = ({ icon: Icon, label, value, color }: any) => (
    <div className="bg-white rounded-lg p-4 border-l-4 shadow-sm" style={{ borderColor: color }}>
      <div className="flex items-center gap-3">
        <div className="p-2 rounded-lg" style={{ backgroundColor: `${color}15` }}>
          <Icon size={24} style={{ color }} />
        </div>
        <div>
          <div className="text-2xl font-bold" style={{ color }}>{value || 0}</div>
          <div className="text-sm text-gray-600">{label}</div>
        </div>
      </div>
    </div>
  )

  const ddlKindMap: Record<string, string> = {
    tables: 'table',
    views: 'view',
    triggers: 'trigger',
    procedures: 'procedure',
    functions: 'function',
    constraints: 'constraint',
    indexes: 'index',
    types: 'type',
    sequences: 'sequence',
    grants: 'grant',
    validations: 'validation'
  }
  const buildCacheKey = (kind?: string, schema?: string, name?: string) => {
    const safeKind = (kind || 'object').toLowerCase()
    const safeSchema = schema || 'public'
    const safeName = name || 'object'
    return `${safeKind}:${safeSchema}.${safeName}`
  }
  const renderDdlPreview = (title: string, ddl?: string, cacheKey?: string) => {
    const content = ddl || ''
    if (!content) {
      return <span className="text-gray-400 font-mono">�</span>
    }
    return (
      <div className="space-y-2">
        <div className="max-h-40 overflow-auto border border-gray-200 rounded-md bg-gray-50 p-2">
          <pre className="text-xs font-mono text-gray-900 whitespace-pre-wrap">{content}</pre>
        </div>
        <button
          type="button"
          onClick={() => setActiveDdl({ title, ddl: content, cacheKey: cacheKey || title })}
          className="text-xs text-blue-600 hover:underline"
        >
          Open in modal
        </button>
      </div>
    )
  }
  const renderTargetPreview = (cacheKey: string, ddl?: string, name?: string, kind?: string, schema?: string) => {
    const content = ddl || ''
    const cached = targetDdlCache[cacheKey]
    if (!content) {
      return <span className="text-gray-400 font-mono">�</span>
    }
    if (cached?.loading) {
      return <span className="text-xs text-gray-600">Converting...</span>
    }
    if (cached?.ddl) {
      return (
        <div className="max-h-40 overflow-auto border border-gray-200 rounded-md bg-gray-50 p-2">
          <pre className="text-xs font-mono text-gray-900 whitespace-pre-wrap">{cached.ddl}</pre>
        </div>
      )
    }
    if (cached?.error) {
      return <span className="text-xs text-rose-600">{cached.error}</span>
    }
    return (
      <button
        type="button"
        onClick={() => fetchTargetDdl({ cacheKey, sourceDdl: content, name, kind, schema })}
        className="text-xs text-blue-600 hover:underline"
      >
        Convert
      </button>
    )
  }

  return (
    <div className="max-w-7xl">
      <div className="flex items-start justify-between gap-4 mb-6">
        <div>
          <h1 className="text-3xl font-bold text-[#085690]">DDL Extraction</h1>
        </div>
        <button
          type="button"
          onClick={() => navigate('/')}
          className="flex items-center gap-2 px-4 py-2 rounded-lg border-2 border-[#085690] text-[#085690] bg-white hover:bg-[#085690] hover:text-white transition-all font-medium"
        >
          <ArrowLeft size={18} />
          Back
        </button>
      </div>

      <div className="bg-white rounded-lg shadow p-6 mb-6 border-t-4 border-[#ec6225]">
        <div className="flex gap-3 items-center flex-wrap">
          <button
            disabled={true}
            className="btn-primary shadow-lg opacity-50 cursor-not-allowed"
          >
            {autoTriggerState === 'preparing'
              ? `Preparing Extraction... (${autoTriggerCountdown}s)`
              : status?.done
              ? 'Completed'
              : 'Auto Extraction in Progress...'}
          </button>

          {status?.done && (
            <button
              onClick={() => {
                setStatus(null)
                startExtraction()
                setAutoTriggerState('triggered')
              }}
              className="flex items-center gap-2 px-4 py-2 rounded-lg border-2 border-[#085690] text-[#085690] bg-white hover:bg-[#085690] hover:text-white transition-all font-medium"
            >
              <RefreshCw size={18} />
              Re-Extract
            </button>
          )}

          {status && status.done && (
            <>
              <button
                onClick={() => exportReport('pdf')}
                className="btn-export flex items-center gap-2 border-2 border-[#085690] text-[#085690] hover:bg-[#085690] hover:text-white"
              >
                <FileText size={18} />
                Export PDF
              </button>
              <button
                onClick={() => exportReport('excel')}
                className="btn-export flex items-center gap-2 border-2 border-[#085690] text-[#085690] hover:bg-[#085690] hover:text-white"
              >
                <FileSpreadsheet size={18} />
                Export Excel
              </button>
              <button
                onClick={() => exportReport('json')}
                className="btn-export flex items-center gap-2 border-2 border-[#085690] text-[#085690] hover:bg-[#085690] hover:text-white"
              >
                <FileJson size={18} />
                Export JSON
              </button>
            </>
          )}
        </div>
      </div>

      {(extracting || status?.done) && (
        <div className="bg-white rounded-lg shadow p-6 mb-6">
          <div className="flex flex-col items-center gap-6 text-center">
            <div className="max-w-xl">
              <h3 className="font-semibold mb-2 text-[#085690]">
                {status?.done ? 'Extraction Complete' : 'Extraction in Progress'}
              </h3>
              <p className="text-sm text-gray-600">
                {status?.done
                  ? 'All extraction steps completed.'
                  : 'Extracting DDL scripts. Steps update automatically.'}
              </p>
            </div>

            <div className="w-full max-w-5xl relative overflow-hidden rounded-2xl border border-white/60 bg-gradient-to-br from-white/80 to-white/60 shadow-glass-lg px-6 py-5 text-left">
              <div className="absolute inset-0 bg-gradient-to-br from-primary-500/5 via-transparent to-accent-500/10" />
              <div className="relative mb-3 text-center">
                <div className="text-sm font-semibold text-[#085690]">Extraction Steps</div>
              </div>
              <div className="space-y-2">
                {extractionSteps.map((step) => {
                  const isComplete = extractionPercent >= step.end
                  const isActive = extractionPercent >= step.start && extractionPercent < step.end
                  const dotClass = isComplete
                    ? 'bg-primary-400'
                    : isActive
                      ? 'bg-[#ec6225] animate-pulse'
                      : 'bg-gray-300'
                  const textClass = isComplete
                    ? 'text-primary-700'
                    : isActive
                      ? 'text-[#ec6225]'
                      : 'text-gray-500'
                  const percentClass = isComplete
                    ? 'text-primary-700'
                    : isActive
                      ? 'text-[#ec6225]'
                      : 'text-gray-400'
                  const stepSpan = step.end - step.start
                  const raw = stepSpan > 0 ? ((extractionPercent - step.start) / stepSpan) * 100 : 0
                  const stepPercent = Math.max(0, Math.min(100, Math.round(raw)))
                  return (
                    <div key={step.label} className="flex items-center gap-3">
                      <div className="flex items-center gap-2 w-48">
                        <span className={`w-2.5 h-2.5 rounded-full ${dotClass}`} />
                        <span className={`text-xs font-semibold ${textClass}`}>{step.label}</span>
                      </div>
                      <div className="flex-1">
                        <div className="w-full bg-white/70 rounded-full h-2.5 shadow-inner">
                          <div
                            className={`h-2.5 rounded-full transition-all ${
                              isComplete
                                ? 'bg-gradient-to-r from-primary-300 to-primary-500'
                                : 'bg-gradient-to-r from-[#ec6225] to-[#ff7a3d]'
                            }`}
                            style={{ width: `${stepPercent}%` }}
                          />
                        </div>
                      </div>
                      <div className={`text-xs font-semibold ${percentClass} w-12 text-right`}>
                        {stepPercent}%
                      </div>
                    </div>
                  )
                })}
              </div>
              <div className="mt-4">
                <div className="w-full bg-white/70 rounded-full h-2.5 shadow-inner">
                  <div
                    className="bg-gradient-to-r from-[#ec6225] to-[#ff7a3d] h-2.5 rounded-full transition-all shadow-sm"
                    style={{ width: `${extractionPercent}%` }}
                  />
                </div>
                <div className="mt-2 text-[11px] font-semibold text-gray-500 text-right">
                  Overall {extractionPercent}%
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {((status && status.done && results) || analyzeMetrics) && (
        <>
          <div className="grid grid-cols-4 gap-4 mb-6">
            <MetricCard icon={Database} label="User Types" value={summary.user_types} color="#085690" />
            <MetricCard icon={Hash} label="Sequences" value={summary.sequences} color="#ec6225" />
            <MetricCard icon={Table} label="Tables" value={summary.tables} color="#085690" />
            <MetricCard icon={Lock} label="Constraints" value={summary.constraints} color="#ec6225" />
          </div>

          <div className="grid grid-cols-4 gap-4 mb-6">
            <MetricCard icon={Zap} label="Indexes" value={summary.indexes} color="#085690" />
            <MetricCard icon={Eye} label="Views" value={summary.views} color="#ec6225" />
            <MetricCard icon={Eye} label="Materialized Views" value={summary.materialized_views} color="#085690" />
            <MetricCard icon={Zap} label="Triggers" value={summary.triggers} color="#ec6225" />
          </div>

          <div className="grid grid-cols-4 gap-4 mb-6">
            <MetricCard icon={Code} label="Procedures" value={summary.procedures} color="#085690" />
            <MetricCard icon={Code} label="Functions" value={summary.functions} color="#ec6225" />
            <MetricCard icon={Lock} label="Grants" value={summary.grants} color="#085690" />
            <MetricCard icon={Box} label="Validation Scripts" value={summary.validation_scripts} color="#ec6225" />
          </div>
        </>
      )}

      {status && status.done && results && (
        <>
          <div className="bg-white rounded-lg shadow border-t-4 border-[#085690]">
            <div className="border-b border-gray-200">
              <div className="flex gap-1 p-2 overflow-x-auto">
                {['overview', 'tables', 'views', 'triggers', 'procedures', 'functions', 'constraints', 'indexes', 'types', 'sequences', 'grants', 'validations'].map(tab => (
                  <button
                    key={tab}
                    onClick={() => setActiveTab(tab)}
                    className={`px-4 py-2 rounded-lg font-medium transition-all whitespace-nowrap ${activeTab === tab
                        ? 'bg-gradient-to-r from-[#085690] to-[#ec6225] text-white'
                        : 'text-gray-600 hover:bg-gray-100'
                      }`}
                  >
                    {tab.charAt(0).toUpperCase() + tab.slice(1)}
                  </button>
                ))}
              </div>
            </div>

            <div className="p-6">
              {activeTab === 'overview' && (
                <div>
                  <h2 className="text-xl font-bold mb-4 text-[#085690]">Extraction Summary</h2>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="p-4 bg-gray-50 rounded-lg border border-gray-200">
                      <h3 className="font-semibold text-gray-700 mb-2">Total Objects Extracted</h3>
                      <p className="text-3xl font-bold text-[#085690]">{results.object_count}</p>
                    </div>
                    <div className="p-4 bg-gray-50 rounded-lg border border-gray-200">
                      <h3 className="font-semibold text-gray-700 mb-2">Extraction Status</h3>
                      <p className="text-lg font-semibold text-green-600">✓ Complete</p>
                    </div>
                  </div>
                  <div className="mt-6 p-4 bg-blue-50 border-l-4 border-[#085690] rounded">
                    <p className="text-sm text-gray-700">
                      <strong>Next Step:</strong> Open logs to monitor connector activity and backend execution details.
                    </p>
                  </div>
                </div>
              )}

              {activeTab === 'tables' && (
                <div>
                  <h2 className="text-xl font-bold mb-4 text-[#085690]">Tables ({results.ddl_scripts?.tables?.length || 0})</h2>
                  <div className="overflow-auto max-h-96">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Schema</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Table Name</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Source DDL Preview</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Target DDL Preview</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {results.ddl_scripts?.tables?.map((table: any, idx: number) => (
                          <tr key={idx} className="hover:bg-gray-50">
                            <td className="px-4 py-3 text-sm text-gray-900">{table.schema}</td>
                            <td className="px-4 py-3 text-sm font-medium text-[#085690]">{table.name}</td>
                            <td className="px-4 py-3 text-sm">{renderDdlPreview(`${table.schema}.${table.name}`, table.ddl, buildCacheKey('table', table.schema, table.name))}</td>
                            <td className="px-4 py-3 text-sm">{renderTargetPreview(buildCacheKey('table', table.schema, table.name), table.ddl, table.name, 'table', table.schema)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {activeTab === 'views' && (
                <div>
                  <h2 className="text-xl font-bold mb-4 text-[#085690]">Views ({results.ddl_scripts?.views?.length || 0})</h2>
                  <div className="overflow-auto max-h-96">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Schema</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">View Name</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Source DDL Preview</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Target DDL Preview</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {results.ddl_scripts?.views?.map((view: any, idx: number) => (
                          <tr key={idx} className="hover:bg-gray-50">
                            <td className="px-4 py-3 text-sm text-gray-900">{view.schema}</td>
                            <td className="px-4 py-3 text-sm font-medium text-[#085690]">{view.name}</td>
                            <td className="px-4 py-3 text-sm">{renderDdlPreview(`${view.schema}.${view.name}`, view.ddl, buildCacheKey('view', view.schema, view.name))}</td>
                            <td className="px-4 py-3 text-sm">{renderTargetPreview(buildCacheKey('view', view.schema, view.name), view.ddl, view.name, 'view', view.schema)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {activeTab === 'triggers' && (
                <div>
                  <h2 className="text-xl font-bold mb-4 text-[#085690]">Triggers ({results.ddl_scripts?.triggers?.length || 0})</h2>
                  <div className="overflow-auto max-h-96">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Schema</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Trigger Name</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Table</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Source DDL Preview</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Target DDL Preview</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {results.ddl_scripts?.triggers?.map((trigger: any, idx: number) => (
                          <tr key={idx} className="hover:bg-gray-50">
                            <td className="px-4 py-3 text-sm text-gray-900">{trigger.schema}</td>
                            <td className="px-4 py-3 text-sm font-medium text-[#085690]">{trigger.name}</td>
                            <td className="px-4 py-3 text-sm text-gray-600">{trigger.table}</td>
                            <td className="px-4 py-3 text-sm">{renderDdlPreview(`${trigger.schema}.${trigger.name}`, trigger.ddl, buildCacheKey('trigger', trigger.schema, trigger.name))}</td>
                            <td className="px-4 py-3 text-sm">{renderTargetPreview(buildCacheKey('trigger', trigger.schema, trigger.name), trigger.ddl, trigger.name, 'trigger', trigger.schema)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {activeTab === 'procedures' && (
                <div>
                  <h2 className="text-xl font-bold mb-4 text-[#085690]">Procedures ({results.ddl_scripts?.procedures?.length || 0})</h2>
                  <div className="overflow-auto max-h-96">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Schema</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Procedure Name</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Source DDL Preview</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Target DDL Preview</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {results.ddl_scripts?.procedures?.map((proc: any, idx: number) => (
                          <tr key={idx} className="hover:bg-gray-50">
                            <td className="px-4 py-3 text-sm text-gray-900">{proc.schema}</td>
                            <td className="px-4 py-3 text-sm font-medium text-[#085690]">{proc.name}</td>
                            <td className="px-4 py-3 text-sm">{renderDdlPreview(`${proc.schema}.${proc.name}`, proc.ddl, buildCacheKey('procedure', proc.schema, proc.name))}</td>
                            <td className="px-4 py-3 text-sm">{renderTargetPreview(buildCacheKey('procedure', proc.schema, proc.name), proc.ddl, proc.name, 'procedure', proc.schema)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {activeTab === 'functions' && (
                <div>
                  <h2 className="text-xl font-bold mb-4 text-[#085690]">Functions ({results.ddl_scripts?.functions?.length || 0})</h2>
                  <div className="overflow-auto max-h-96">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Schema</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Function Name</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Source DDL Preview</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Target DDL Preview</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {results.ddl_scripts?.functions?.map((func: any, idx: number) => (
                          <tr key={idx} className="hover:bg-gray-50">
                            <td className="px-4 py-3 text-sm text-gray-900">{func.schema}</td>
                            <td className="px-4 py-3 text-sm font-medium text-[#085690]">{func.name}</td>
                            <td className="px-4 py-3 text-sm">{renderDdlPreview(`${func.schema}.${func.name}`, func.ddl, buildCacheKey('function', func.schema, func.name))}</td>
                            <td className="px-4 py-3 text-sm">{renderTargetPreview(buildCacheKey('function', func.schema, func.name), func.ddl, func.name, 'function', func.schema)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {activeTab === 'constraints' && (
                <div>
                  <h2 className="text-xl font-bold mb-4 text-[#085690]">Constraints ({results.ddl_scripts?.constraints?.length || 0})</h2>
                  <div className="overflow-auto max-h-96">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Schema</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Constraint Name</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Table</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Source DDL Preview</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Target DDL Preview</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {results.ddl_scripts?.constraints?.map((cons: any, idx: number) => (
                          <tr key={idx} className="hover:bg-gray-50">
                            <td className="px-4 py-3 text-sm text-gray-900">{cons.schema}</td>
                            <td className="px-4 py-3 text-sm font-medium text-[#085690]">{cons.name}</td>
                            <td className="px-4 py-3 text-sm text-gray-600">{cons.table}</td>
                            <td className="px-4 py-3 text-sm">{renderDdlPreview(`${cons.schema}.${cons.name}`, cons.ddl, buildCacheKey('constraint', cons.schema, cons.name))}</td>
                            <td className="px-4 py-3 text-sm">{renderTargetPreview(buildCacheKey('constraint', cons.schema, cons.name), cons.ddl, cons.name, 'constraint', cons.schema)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {activeTab === 'indexes' && (
                <div>
                  <h2 className="text-xl font-bold mb-4 text-[#085690]">Indexes ({results.ddl_scripts?.indexes?.length || 0})</h2>
                  <div className="overflow-auto max-h-96">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Schema</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Index Name</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Table</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Source DDL Preview</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Target DDL Preview</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {results.ddl_scripts?.indexes?.map((idx: any, i: number) => (
                          <tr key={i} className="hover:bg-gray-50">
                            <td className="px-4 py-3 text-sm text-gray-900">{idx.schema}</td>
                            <td className="px-4 py-3 text-sm font-medium text-[#085690]">{idx.name}</td>
                            <td className="px-4 py-3 text-sm text-gray-600">{idx.table}</td>
                            <td className="px-4 py-3 text-sm">{renderDdlPreview(`${idx.schema}.${idx.name}`, idx.ddl, buildCacheKey('index', idx.schema, idx.name))}</td>
                            <td className="px-4 py-3 text-sm">{renderTargetPreview(buildCacheKey('index', idx.schema, idx.name), idx.ddl, idx.name, 'index', idx.schema)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {activeTab === 'types' && (
                <div>
                  <h2 className="text-xl font-bold mb-4 text-[#085690]">User-Defined Types ({results.ddl_scripts?.user_types?.length || 0})</h2>
                  <div className="overflow-auto max-h-96">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Schema</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Type Name</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Source DDL Preview</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Target DDL Preview</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {results.ddl_scripts?.user_types?.map((type: any, idx: number) => (
                          <tr key={idx} className="hover:bg-gray-50">
                            <td className="px-4 py-3 text-sm text-gray-900">{type.schema}</td>
                            <td className="px-4 py-3 text-sm font-medium text-[#085690]">{type.name}</td>
                            <td className="px-4 py-3 text-sm">{renderDdlPreview(`${type.schema}.${type.name}`, type.ddl, buildCacheKey('type', type.schema, type.name))}</td>
                            <td className="px-4 py-3 text-sm">{renderTargetPreview(buildCacheKey('type', type.schema, type.name), type.ddl, type.name, 'type', type.schema)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {activeTab === 'sequences' && (
                <div>
                  <h2 className="text-xl font-bold mb-4 text-[#085690]">Sequences ({results.ddl_scripts?.sequences?.length || 0})</h2>
                  <div className="overflow-auto max-h-96">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Schema</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Sequence Name</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Source DDL Preview</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Target DDL Preview</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {results.ddl_scripts?.sequences?.map((seq: any, idx: number) => (
                          <tr key={idx} className="hover:bg-gray-50">
                            <td className="px-4 py-3 text-sm text-gray-900">{seq.schema}</td>
                            <td className="px-4 py-3 text-sm font-medium text-[#085690]">{seq.name}</td>
                            <td className="px-4 py-3 text-sm">{renderDdlPreview(`${seq.schema}.${seq.name}`, seq.ddl, buildCacheKey('sequence', seq.schema, seq.name))}</td>
                            <td className="px-4 py-3 text-sm">{renderTargetPreview(buildCacheKey('sequence', seq.schema, seq.name), seq.ddl, seq.name, 'sequence', seq.schema)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {activeTab === 'grants' && (
                <div>
                  <h2 className="text-xl font-bold mb-4 text-[#085690]">Grant Statements ({results.ddl_scripts?.grants?.length || 0})</h2>
                  <div className="overflow-auto max-h-96">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Schema</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Grantee</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Object</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Source DDL Preview</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Target DDL Preview</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {results.ddl_scripts?.grants?.map((grant: any, idx: number) => (
                          <tr key={idx} className="hover:bg-gray-50">
                            <td className="px-4 py-3 text-sm text-gray-900">{grant.schema}</td>
                            <td className="px-4 py-3 text-sm font-medium text-[#085690]">{grant.grantee}</td>
                            <td className="px-4 py-3 text-sm text-gray-600">{grant.object}</td>
                            <td className="px-4 py-3 text-sm">{renderDdlPreview(`${grant.schema}.${grant.object}`, grant.ddl, buildCacheKey('grant', grant.schema, grant.object))}</td>
                            <td className="px-4 py-3 text-sm">{renderTargetPreview(buildCacheKey('grant', grant.schema, grant.object), grant.ddl, grant.object, 'grant', grant.schema)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {activeTab === 'validations' && (
                <div>
                  <h2 className="text-xl font-bold mb-4 text-[#085690]">Validation Scripts ({results.ddl_scripts?.validation_scripts?.length || 0})</h2>
                  <div className="overflow-auto max-h-96">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Schema</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Table</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Source SQL Preview</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Target SQL Preview</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {results.ddl_scripts?.validation_scripts?.map((script: any, idx: number) => (
                          <tr key={idx} className="hover:bg-gray-50">
                            <td className="px-4 py-3 text-sm text-gray-900">{script.schema}</td>
                            <td className="px-4 py-3 text-sm font-medium text-[#085690]">{script.table}</td>
                            <td className="px-4 py-3 text-sm">{renderDdlPreview(`${script.schema}.${script.table}`, script.sql, buildCacheKey('validation', script.schema, script.table))}</td>
                            <td className="px-4 py-3 text-sm">{renderTargetPreview(buildCacheKey('validation', script.schema, script.table), script.sql, script.table, 'validation', script.schema)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          </div>

          <div className="mt-6 flex flex-col items-center space-y-4">
            {/* New checkbox to request execution of translated DDL */}
            <label className="flex items-center gap-2 text-sm text-gray-700">
              <input
                type="checkbox"
                checked={runInTarget}
                onChange={e => setRunInTarget(e.target.checked)}
                className="form-checkbox h-4 w-4 text-blue-600"
              />
              Run translated DDL in target database
            </label>
            <button
              onClick={() => {
                onExtractionComplete()
                navigate('/logs')
              }}
              className="flex items-center gap-2 px-8 py-3 bg-gradient-to-r from-[#ec6225] to-[#ff7a3d] text-white rounded-lg hover:shadow-lg transition-all font-medium text-lg"
            >
              Open Logs
            </button>
          </div>
        </>
      )}

      <div className="bg-white rounded-lg shadow border-t-4 border-[#085690] p-6 mb-6">
        <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div>
            <h3 className="text-lg font-semibold text-[#085690]">Run translated DDL in Databricks</h3>
            <p className="text-sm text-gray-600">
              {datatypeLoading
                ? 'Loading datatype overrides...'
                : `Active overrides applied: ${Object.keys(datatypeOverrides).length}.`}
            </p>
            {!status?.done && (
              <p className="text-xs text-gray-500 mt-1">
                Extraction must finish before the target run can start.
              </p>
            )}
          </div>
          <div className="flex flex-wrap gap-3">
            <button
              onClick={startStructureMigration}
              disabled={startingStructure || structureStatus?.status === 'running' || !status?.done}
              className="px-4 py-2 bg-gradient-to-r from-[#ec6225] to-[#ff7a3d] text-white rounded-lg hover:shadow-lg transition-all disabled:opacity-60"
            >
              {structureStatus?.status === 'running' || startingStructure
                ? 'Running in target...'
                : 'Start target run'}
            </button>
            <button
              onClick={() => navigate('/datatypes')}
              className="px-4 py-2 border border-[#085690] text-[#085690] rounded-lg hover:bg-[#085690] hover:text-white transition-colors"
            >
              Review datatype mappings
            </button>
          </div>
        </div>
        <div className="mt-4 space-y-2">
          <div className="flex items-center justify-between text-xs uppercase tracking-wide text-gray-500">
            <span>Status: {structureStatus?.message || structureStatus?.status || 'Not started'}</span>
            <span>Phase: {structureStatus?.progress?.phase || 'Waiting'}</span>
          </div>
          <div className="w-full h-2 rounded-full bg-gray-200 overflow-hidden">
            <div
              className="h-full bg-gradient-to-r from-[#085690] to-[#ec6225]"
              style={{
                width: `${Math.min(100, Math.max(0, structureStatus?.progress?.percent ?? 0))}%`
              }}
            />
          </div>
          {structureNotification && (
            <p className="text-xs text-rose-600">{structureNotification}</p>
          )}
          {datatypeError && (
            <p className="text-xs text-rose-600">
              Unable to refresh datatype mappings: {datatypeError}
            </p>
          )}
          {structureStatus?.status === 'error' && (
            <p className="text-xs text-rose-600">
              Target execution error: {structureStatus?.error || 'check logs for details'}
            </p>
          )}
        </div>
      </div>

      {activeDdl && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl shadow-2xl max-w-6xl w-full mx-4 max-h-[80vh] overflow-hidden">
            <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
              <div>
                <h3 className="text-lg font-semibold text-gray-900">{activeDdl.title}</h3>
                <p className="text-xs text-gray-500">
                  Source: {sessionInfo?.sourceType || 'Oracle'} | Target: {sessionInfo?.targetType || 'Databricks'}
                </p>
              </div>
              <button
                type="button"
                onClick={() => setActiveDdl(null)}
                className="text-sm font-semibold text-gray-600 hover:text-gray-900"
              >
                Close
              </button>
            </div>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 p-6 overflow-auto max-h-[70vh]">
              <div className="border border-gray-200 rounded-lg overflow-hidden">
                <div className="px-4 py-2 bg-gray-50 text-xs font-semibold text-gray-700">Source DDL</div>
                <pre className="p-4 text-xs font-mono text-gray-900 whitespace-pre-wrap">{activeDdl.ddl || 'Source DDL unavailable'}</pre>
              </div>
              <div className="border border-gray-200 rounded-lg overflow-hidden">
                <div className="px-4 py-2 bg-gray-50 text-xs font-semibold text-gray-700">Target DDL</div>
                <pre className="p-4 text-xs font-mono text-gray-900 whitespace-pre-wrap">
                  {targetDdlCache[activeDdl.cacheKey]?.loading
                    ? 'Converting...'
                    : targetDdlCache[activeDdl.cacheKey]?.error
                      ? targetDdlCache[activeDdl.cacheKey]?.error
                      : targetDdlCache[activeDdl.cacheKey]?.ddl || 'Target DDL unavailable'}
                </pre>
                {/* Show execution outcome if the backend attempted to run the DDL */}
                {targetDdlCache[activeDdl.cacheKey]?.executed !== undefined && (
                  <div className="mt-2 text-sm">
                    {targetDdlCache[activeDdl.cacheKey]?.executed ? (
                      <span className="text-green-600 font-medium">✅ Executed in target</span>
                    ) : (
                      <span className="text-red-600 font-medium">
                        ❌ Execution failed: {targetDdlCache[activeDdl.cacheKey]?.execution_error || 'unknown error'}
                      </span>
                    )}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
