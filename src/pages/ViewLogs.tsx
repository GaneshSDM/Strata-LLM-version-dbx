import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { AlertCircle, Clock, Terminal } from 'lucide-react'
import { ensureSessionId } from '../utils/session'

type SessionLogEntry = {
  session_id?: string
  run_id?: number | string | null
  timestamp?: string
  step?: string
  level?: string
  message?: string
  tables?: string[]
}

type DisplayRow = SessionLogEntry & {
  id: string
  displayTimestamp: string
  session_display: string
  timestamp_ms: number
  run_id: number | null
  cleaned_message: string
  display_level: string
}

const REFRESH_INTERVAL_MS = 3000

const parseTimestamp = (timestamp?: string) => {
  if (!timestamp) return 0
  const ms = Date.parse(timestamp)
  return Number.isFinite(ms) ? ms : 0
}

const formatTimestamp = (timestamp?: string) => {
  if (!timestamp) return '-'
  const date = new Date(timestamp)
  return Number.isFinite(date.getTime()) ? date.toLocaleString() : '-'
}

const formatDateOnly = (timestamp?: string) => {
  if (!timestamp) return 'Unknown date'
  const date = new Date(timestamp)
  return Number.isFinite(date.getTime()) ? date.toLocaleDateString() : 'Unknown date'
}

const normalizeLevel = (level?: string) => {
  const normalized = (level || 'info').toLowerCase()
  if (normalized === 'success' || normalized === 'warning' || normalized === 'error' || normalized === 'info') {
    return normalized
  }
  if (normalized === 'warn') return 'warning'
  if (normalized === 'err') return 'error'
  return 'info'
}

const stripRunId = (message?: string) => {
  if (!message) return ''
  return message
    .replace(/\s*run_id\s*[:=]?\s*\d+\b/gi, '')
    .replace(/\s+/g, ' ')
    .trim()
}

const deriveDisplayLevel = (level: string, message: string) => {
  if (level !== 'info') return level
  const lower = message.toLowerCase()
  const statusMatch = lower.match(/status\s*=\s*([a-z_]+)/)
  if (statusMatch) {
    const status = statusMatch[1]
    if (status === 'success') return 'success'
    if (status === 'partial') return 'warning'
    if (status === 'failed' || status === 'error') return 'error'
  }

  const failedMatch = lower.match(/failed_rows\s*=\s*(\d+)/)
  const failedRows = failedMatch ? Number.parseInt(failedMatch[1], 10) : null
  if (typeof failedRows === 'number' && !Number.isNaN(failedRows) && failedRows > 0) {
    return 'error'
  }

  if (lower.includes('exception') || lower.includes('error')) return 'error'
  const hasFail = lower.includes('failed') || lower.includes('fail')
  if (hasFail && !(failedRows === 0)) return 'error'
  if (lower.includes('warn')) return 'warning'
  return 'success'
}

const levelStyles: Record<string, string> = {
  info: 'bg-blue-100 text-blue-700',
  success: 'bg-emerald-100 text-emerald-700',
  warning: 'bg-amber-100 text-amber-700',
  error: 'bg-rose-100 text-rose-700'
}

export default function ViewLogs() {
  const [sessionId, setSessionId] = useState<string | null>(null)
  const [logs, setLogs] = useState<SessionLogEntry[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [showAllRuns, setShowAllRuns] = useState(false)
  const [clearing, setClearing] = useState(false)
  const [expandedTables, setExpandedTables] = useState<Record<string, boolean>>({})
  const hasFetchedRef = useRef(false)

  const fetchLogs = useCallback(async (currentSessionId: string) => {
    if (!currentSessionId) return
    const isInitial = !hasFetchedRef.current
    if (isInitial) setLoading(true)
    try {
      setError(null)
      const response = await fetch(`/api/logs/session?session_id=${encodeURIComponent(currentSessionId)}`)
      const data = await response.json()
      if (!response.ok || data?.ok === false) {
        throw new Error(data?.message || 'Failed to fetch logs')
      }
      setLogs(Array.isArray(data?.data) ? data.data : [])
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to fetch logs'
      setError(message)
    } finally {
      if (isInitial) {
        setLoading(false)
        hasFetchedRef.current = true
      }
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    const initSession = async () => {
      try {
        const newSessionId = await ensureSessionId()
        if (!newSessionId) {
          throw new Error('Failed to start session')
        }
        if (!cancelled) setSessionId(newSessionId)
      } catch (err) {
        const message = err instanceof Error ? err.message : 'Failed to initialize session'
        if (!cancelled) {
          setError(message)
          setLoading(false)
        }
      }
    }

    initSession()
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (!sessionId) return
    fetchLogs(sessionId)
    const intervalId = window.setInterval(() => {
      fetchLogs(sessionId)
    }, REFRESH_INTERVAL_MS)
    return () => window.clearInterval(intervalId)
  }, [sessionId, fetchLogs])

  const handleClearLogs = async () => {
    if (!sessionId) return
    setClearing(true)
    setError(null)
    try {
      const response = await fetch('/api/logs/session/clear', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ session_id: sessionId })
      })
      const data = await response.json()
      if (!response.ok || data?.ok === false) {
        throw new Error(data?.message || 'Failed to clear logs')
      }
      setLogs([])
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to clear logs'
      setError(message)
    } finally {
      setClearing(false)
    }
  }

  const rows = useMemo<DisplayRow[]>(() => {
    return logs
      .map((entry, index) => {
        const rawRunId = entry.run_id
        const normalizedRunId = (() => {
          if (rawRunId === null || rawRunId === undefined || rawRunId === '') return null
          const candidate = typeof rawRunId === 'number' ? rawRunId : Number(rawRunId)
          return Number.isFinite(candidate) ? candidate : null
        })()
        const timestampMs = parseTimestamp(entry.timestamp)
        const sessionDisplay = entry.session_id || sessionId || '-'
        const cleanedMessage = stripRunId(entry.message)
        const baseLevel = normalizeLevel(entry.level)
        const displayLevel = deriveDisplayLevel(baseLevel, cleanedMessage)
        return {
          ...entry,
          run_id: normalizedRunId,
          timestamp_ms: timestampMs,
          displayTimestamp: formatTimestamp(entry.timestamp),
          session_display: sessionDisplay,
          cleaned_message: cleanedMessage || '-',
          display_level: displayLevel,
          id: `${sessionDisplay}-${normalizedRunId ?? 'none'}-${timestampMs}-${index}`
        }
      })
      .sort((a, b) => b.timestamp_ms - a.timestamp_ms)
  }, [logs, sessionId])

  const latestRunId = useMemo(() => {
    const runIds = rows
      .map((row) => row.run_id)
      .filter((runId): runId is number => typeof runId === 'number')
    return runIds.length > 0 ? Math.max(...runIds) : null
  }, [rows])

  const visibleRows = useMemo(() => {
    if (showAllRuns || latestRunId === null) return rows
    return rows.filter((row) => row.run_id === latestRunId || row.run_id == null)
  }, [rows, showAllRuns, latestRunId])

  const groupedRuns = useMemo(() => {
    if (!showAllRuns) return []
    const grouped = new Map<string, { key: string; runId: number | null; rows: DisplayRow[] }>()
    rows.forEach((row) => {
      const key = row.run_id == null ? 'none' : String(row.run_id)
      if (!grouped.has(key)) {
        grouped.set(key, { key, runId: row.run_id ?? null, rows: [] })
      }
      grouped.get(key)?.rows.push(row)
    })
    return Array.from(grouped.values())
  }, [rows, showAllRuns])

  const renderRow = (row: DisplayRow, index: number) => {
    const level = row.display_level || 'info'
    const badgeClass = levelStyles[level] || levelStyles.info
    const tables = row.tables || []
    const isExpanded = !!expandedTables[row.id]
    const shownTables = isExpanded ? tables : tables.slice(0, 5)
    const remainingTables = tables.length - shownTables.length
    return (
      <motion.tr
        key={row.id}
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.2, delay: Math.min(index * 0.02, 0.3) }}
        className="border-b border-gray-200 hover:bg-gray-50"
      >
        <td className="px-4 py-3 text-xs font-mono text-gray-600 max-w-[200px] truncate" title={row.session_display}>
          {row.session_display}
        </td>
        <td className="px-4 py-3 text-xs text-gray-600">{row.run_id ?? '-'}</td>
        <td className="px-4 py-3 text-xs text-gray-600 whitespace-nowrap">{row.displayTimestamp}</td>
        <td className="px-4 py-3 text-xs text-gray-700">{row.step || '-'}</td>
        <td className="px-4 py-3 text-xs text-gray-700">
          <div className="flex items-start gap-2">
            <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-semibold ${badgeClass}`}>
              {level.toUpperCase()}
            </span>
            <span className="text-xs text-gray-700 break-words">{row.cleaned_message}</span>
          </div>
          {tables.length > 0 && (
            <div className="mt-1 text-[11px] text-gray-500 break-words">
              {shownTables.join(', ')}
              {remainingTables > 0 && !isExpanded && (
                <>
                  {' '}
                  <button
                    type="button"
                    onClick={() => setExpandedTables(prev => ({ ...prev, [row.id]: true }))}
                    className="text-primary-600 hover:underline"
                  >
                    +{remainingTables} more
                  </button>
                </>
              )}
              {isExpanded && tables.length > 5 && (
                <>
                  {' '}
                  <button
                    type="button"
                    onClick={() => setExpandedTables(prev => ({ ...prev, [row.id]: false }))}
                    className="text-primary-600 hover:underline"
                  >
                    Show less
                  </button>
                </>
              )}
            </div>
          )}
        </td>
      </motion.tr>
    )
  }

  return (
    <div className="max-w-7xl mx-auto">
      <motion.div initial={{ opacity: 0, y: -20 }} animate={{ opacity: 1, y: 0 }} className="mb-6">
        <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
          <div className="flex items-start gap-3">
            <div className="p-3 rounded-xl bg-gradient-to-br from-primary-100 to-accent-100">
              <Terminal className="text-primary-600" size={24} />
            </div>
            <div>
              <h1 className="text-3xl font-bold text-gray-900">View Logs</h1>
              <p className="text-sm text-gray-600">Session-scoped backend activity</p>
              <div className="flex items-center gap-2 text-xs text-gray-500 mt-2">
                <Clock size={12} />
                <span>Logs reset when the session is cleared.</span>
              </div>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <div className="px-3 py-2 rounded-lg bg-gray-100 text-xs text-gray-600">
              <span className="font-semibold text-gray-700">Session:</span>{' '}
              <span className="font-mono">{sessionId || '-'}</span>
            </div>
            <button
              type="button"
              onClick={() => setShowAllRuns((prev) => !prev)}
              className="px-4 py-2 rounded-lg border border-gray-200 bg-white text-sm font-semibold text-gray-700 hover:bg-gray-50"
            >
              {showAllRuns ? 'View Latest Log' : 'View Previous Log'}
            </button>
            <button
              type="button"
              onClick={handleClearLogs}
              disabled={clearing || !sessionId}
              className="px-4 py-2 rounded-lg border border-rose-200 bg-rose-50 text-sm font-semibold text-rose-700 hover:bg-rose-100 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {clearing ? 'Clearing...' : 'Clear Logs'}
            </button>
          </div>
        </div>
      </motion.div>

      <AnimatePresence>
        {error && (
          <motion.div
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            className="mb-4 glass-card rounded-xl p-4 border-l-4 border-rose-500 flex items-start gap-3"
          >
            <div className="p-2 rounded-lg bg-rose-100">
              <AlertCircle className="text-rose-600" size={18} />
            </div>
            <div className="flex-1">
              <h4 className="font-bold text-rose-900 mb-1">Log Error</h4>
              <p className="text-sm text-rose-700">{error}</p>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} className="card-glass p-6">
        <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
          <div>
            <h2 className="text-lg font-bold text-gray-900">Session Logs</h2>
            <p className="text-xs text-gray-500">Auto-refresh every {REFRESH_INTERVAL_MS / 1000}s</p>
          </div>
          <div className="text-xs text-gray-600">
            <span className="font-semibold text-gray-700">Latest Run:</span>{' '}
            {latestRunId ?? '-'}
          </div>
        </div>

        <div className="border border-gray-200 rounded-xl overflow-hidden bg-white">
          <div className="max-h-[600px] overflow-y-auto">
            <table className="min-w-full">
              <thead className="bg-gray-50 sticky top-0 z-10">
                <tr>
                  <th className="py-3 px-4 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">
                    Session ID
                  </th>
                  <th className="py-3 px-4 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">
                    Run ID
                  </th>
                  <th className="py-3 px-4 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">
                    Timestamp
                  </th>
                  <th className="py-3 px-4 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">
                    Process
                  </th>
                  <th className="py-3 px-4 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">
                    Status
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {loading ? (
                  <tr>
                    <td colSpan={5} className="py-10 text-center text-sm text-gray-500">
                      <motion.div
                        animate={{ opacity: [0.5, 1, 0.5] }}
                        transition={{ duration: 1.2, repeat: Infinity }}
                        className="inline-flex items-center gap-2"
                      >
                        Loading logs...
                      </motion.div>
                    </td>
                  </tr>
                ) : visibleRows.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="py-10 text-center text-sm text-gray-500">
                      No log entries found.
                    </td>
                  </tr>
                ) : showAllRuns ? (
                  groupedRuns.flatMap((group, groupIndex) => {
                    const firstDate = formatDateOnly(group.rows[0]?.timestamp)
                    const header = (
                      <tr key={`group-${group.key}`}>
                        <td colSpan={5} className="px-4 py-2 bg-gray-100 text-xs font-semibold text-gray-600">
                          Run {group.runId ?? '-'} - {firstDate}
                        </td>
                      </tr>
                    )
                    const rowsForGroup = group.rows.map((row, rowIndex) =>
                      renderRow(row, groupIndex + rowIndex)
                    )
                    return [header, ...rowsForGroup]
                  })
                ) : (
                  visibleRows.map((row, index) => renderRow(row, index))
                )}
              </tbody>
            </table>
          </div>
        </div>
      </motion.div>
    </div>
  )
}


