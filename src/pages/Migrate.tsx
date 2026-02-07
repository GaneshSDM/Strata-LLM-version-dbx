import { useState, useEffect, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { ArrowLeft, Loader2, Database, FileCode, CheckCircle, ArrowRight, FileText, FileSpreadsheet, FileJson, AlertCircle } from 'lucide-react'
import { useWizard } from '../components/WizardContext'
import CircularProgress from '../components/CircularProgress'
import { ensureSessionId, getSessionHeaders } from '../utils/session'

interface TranslatedObject {
  name: string
  kind: string
  target_sql: string
  source_ddl?: string
  notes?: string[]
}

interface TranslationResult {
  objects: TranslatedObject[]
  warnings?: string[]
  error?: string
}

interface MigrationResults {
  translation: TranslationResult
}

interface DataMigrationResult {
  table: string
  rows_copied: number
  status: string
  total_rows?: number | string
  error?: string
}

type TableProgressEntry = {
  percent: number
  rowsCopied?: number
  totalRows?: number
}

interface MigrateProps {
  onMigrationComplete: () => void
}

export default function Migrate({ onMigrationComplete }: MigrateProps) {
  const navigate = useNavigate()
  const { wizardResetId } = useWizard()
  const [structureRunning, setStructureRunning] = useState(false)
  const [structureDone, setStructureDone] = useState(false)
  const [dataRunning, setDataRunning] = useState(false)
  const [dataDone, setDataDone] = useState(false)
  const [migrationResults, setMigrationResults] = useState<MigrationResults | null>(null)
  const [dataMigrationResults, setDataMigrationResults] = useState<DataMigrationResult[]>([])
  const [totalRows, setTotalRows] = useState(0)
  const [error, setError] = useState<string | null>(null)
  const [hasReportedCompletion, setHasReportedCompletion] = useState(false)
  const [showSuccessModal, setShowSuccessModal] = useState(false)
  const [modalAcknowledged, setModalAcknowledged] = useState(false)
  const [structureProgress, setStructureProgress] = useState(0)
  const [, setDataProgress] = useState(0)
  const [tableProgress, setTableProgress] = useState<Record<string, TableProgressEntry>>({})
  const [selectedColumnsMap, setSelectedColumnsMap] = useState<Record<string, string[]>>({})
  const [renameRunning, setRenameRunning] = useState(false)
  const [renameDone, setRenameDone] = useState(false)
  const [renameError, setRenameError] = useState<string | null>(null)
  const [hasPendingRenames, setHasPendingRenames] = useState(false)

  useEffect(() => {
    setStructureRunning(false)
    setStructureDone(false)
    setDataRunning(false)
    setDataDone(false)
    setMigrationResults(null)
    setDataMigrationResults([])
    setTotalRows(0)
    setError(null)
    setHasReportedCompletion(false)
    setShowSuccessModal(false)
    setModalAcknowledged(false)
    setTableProgress({})
    setSelectedColumnsMap({})
    setHasPendingRenames(false)
  }, [wizardResetId])

  const totalTables = dataMigrationResults.length
  const successfulTables = dataMigrationResults.filter(r => (r.status || '').toLowerCase() !== 'error' && !r.error).length
  const successRate = totalTables ? Math.round((successfulTables / totalTables) * 100) : 0
  const hasMigrationErrors = dataMigrationResults.some(r => (r.status || '').toLowerCase() === 'error' || !!r.error)

  // Watch for dataDone to trigger the success modal (only after renames if required)
  useEffect(() => {
    const hasErrors = dataMigrationResults.some(r => (r.status || '').toLowerCase() === 'error' || !!r.error)
    const renameGateOk = !hasPendingRenames || renameDone
    if (dataDone && dataMigrationResults.length > 0 && !hasErrors && renameGateOk && !modalAcknowledged) {
      setShowSuccessModal(true)
    }
  }, [dataDone, dataMigrationResults, modalAcknowledged, hasPendingRenames, renameDone])

  // Fetch pending renames (to decide whether to show rename card / gating)
  useEffect(() => {
    const loadRenames = async () => {
      try {
        const res = await fetch('/api/session/get-column-renames')
        if (!res.ok) return
        const data = await res.json()
        const map = data?.columnRenames || {}
        setHasPendingRenames(!!map && Object.keys(map).length > 0)
      } catch (e) {
        setHasPendingRenames(false)
      }
    }
    loadRenames()
  }, [])

  const parseJsonResponse = async (res: Response) => {
    const text = await res.text()
    if (!text) {
      return { ok: false, message: `HTTP ${res.status}` }
    }
    try {
      return JSON.parse(text)
    } catch {
      return { ok: false, message: text }
    }
  }

  // Helper to apply results from status response (defined outside migrateStructure so it can be used in pollStatus)
  const applyMigrationResultsFromData = (data: any) => {
    if (data.translation?.objects?.length) {
      setMigrationResults({ translation: data.translation })
    }
  }

  const migrateStructure = async () => {
    setStructureRunning(true)
    setStructureProgress(0)
    setError(null)
    await ensureSessionId()

    let pollInterval: ReturnType<typeof setInterval> | null = null
    let statusComplete = false
    let hasAppliedResults = false
    let migrationResponse: any = null

    const stopPolling = () => {
      if (pollInterval) {
        clearInterval(pollInterval)
        pollInterval = null
      }
    }

    const applyMigrationResults = () => {
      if (!statusComplete || !migrationResponse || hasAppliedResults) return
      hasAppliedResults = true
      if (migrationResponse.data) {
        setMigrationResults(migrationResponse.data)
      }
    }

    const finalizeStructure = () => {
      if (!statusComplete) {
        statusComplete = true
        setStructureProgress(100)
        setStructureDone(true)
        setStructureRunning(false)
      }
      stopPolling()
      applyMigrationResults()
    }

    const handleError = (message: string) => {
      stopPolling()
      setError(message)
      setStructureProgress(0)
      setStructureRunning(false)
    }

    const pollStatus = async () => {
      try {
        const statusRes = await fetch('/api/migrate/structure-status')
        const statusData = await parseJsonResponse(statusRes)
        if (statusData.status === 'complete') {
          // Auto-apply results when complete - this ensures target DDL shows automatically
          finalizeStructure()
          if (statusData.data) {
            // Results are included in the status response, apply them directly
            applyMigrationResultsFromData(statusData.data)
          } else {
            // Fallback: fetch results from separate endpoint
            const resultsRes = await fetch('/api/migrate/structure-status')
            const resultsData = await parseJsonResponse(resultsRes)
            if (resultsData.data) {
              applyMigrationResultsFromData(resultsData.data)
            }
          }
        } else if (statusData.status === 'running' && statusData.progress) {
          setStructureProgress(statusData.progress.percent || 0)
        } else if (statusData.status === 'error') {
          handleError(statusData.message || 'Structure migration error')
        }
      } catch (err) {
        console.error('Error polling structure migration status:', err)
        handleError('Failed to get migration status')
      }
    }



    pollInterval = setInterval(pollStatus, 500)
    void pollStatus()

    try {
      const res = await fetch('/api/migrate/structure', {
        method: 'POST',
        headers: getSessionHeaders()
      })
      migrationResponse = await parseJsonResponse(res)

      if (!migrationResponse.ok) {
        handleError(migrationResponse.message || 'Structure migration failed')
        return
      }
    } catch (err: any) {
      handleError(err.message || 'Network error during structure migration')
    }
  }

  const migrateData = async () => {
    setDataRunning(true)
    setDataProgress(0)
    setTableProgress({})
    setError(null)
    await ensureSessionId()
    try {
      // Simulate initial progress for data migration
      const interval = setInterval(() => {
        setDataProgress(prev => {
          if (prev >= 30) {
            clearInterval(interval);
            return prev;
          }
          return prev + 2;
        });
      }, 200);

      // Start the data migration process
      const res = await fetch('/api/migrate/data', {
        method: 'POST',
        headers: getSessionHeaders()
      })
      const data = await parseJsonResponse(res)
      clearInterval(interval);

      if (!data.ok) {
        setError(data.message || 'Data migration failed to start')
        setDataRunning(false)
        setDataProgress(0);
      }
      // The actual migration will be monitored by the polling useEffect
    } catch (err: any) {
      setError(err.message || 'Network error during data migration')
      setDataRunning(false)
      setDataProgress(0);
    }
  }

  useEffect(() => {
    let interval: ReturnType<typeof setInterval> | null = null

    const applyProgressPayload = (progressPayload: Record<string, any>) => {
      const entries = Object.entries(progressPayload)
      if (!entries.length) return

      setTableProgress(prev => {
        const next: Record<string, TableProgressEntry> = { ...prev }
        entries.forEach(([tableName, progressValue]) => {
          next[tableName] = normalizeProgressValue(progressValue, prev[tableName])
        })
        return next
      })

      let latestResults: DataMigrationResult[] = []
      setDataMigrationResults(prev => {
        const currentResults = [...prev]
        entries.forEach(([tableName, progressInfoRaw]) => {
          const progressInfo = progressInfoRaw as {
            percent?: number
            rows_copied?: number
            total_rows?: number | string
          }
          if (progressInfo && typeof progressInfo === 'object') {
            const existingIndex = currentResults.findIndex(r => r.table === tableName)
            if (existingIndex !== -1) {
              currentResults[existingIndex] = {
                ...currentResults[existingIndex],
                rows_copied: progressInfo.rows_copied ?? currentResults[existingIndex].rows_copied,
                total_rows: progressInfo.total_rows ?? currentResults[existingIndex].total_rows,
                status: progressInfo.percent === 100 ? 'Success' : currentResults[existingIndex].status
              }
            } else {
              currentResults.push({
                table: tableName,
                rows_copied: progressInfo.rows_copied ?? 0,
                total_rows: progressInfo.total_rows,
                status: progressInfo.percent === 100 ? 'Success' : 'In Progress'
              })
            }
          }
        })
        latestResults = currentResults
        return currentResults
      })

      const totalRowsSum = latestResults.reduce((sum, result) => {
        return sum + (typeof result.rows_copied === 'number' ? result.rows_copied : 0)
      }, 0)
      setTotalRows(totalRowsSum)
    }

    // Poll for data migration status when data migration is running
    if (dataRunning) {
      interval = setInterval(async () => {
        try {
          // Check if data migration is complete
          const response = await fetch('/api/migrate/data-status')
          const data = await parseJsonResponse(response)

          if (data.status === 'complete') {
            if (interval) clearInterval(interval)
            setDataDone(true)
            setDataRunning(false)
            setDataProgress(100); // Ensure progress reaches 100%

            // Update all table progress to 100% when complete
            const selectedTables = getSelectedTables();
            setTableProgress(prev => {
              const next: Record<string, TableProgressEntry> = { ...prev };
              selectedTables.forEach(table => {
                const tableName = `${table.schema}.${table.name}`;
                const existing = prev[tableName];
                next[tableName] = {
                  percent: 100,
                  rowsCopied: existing?.rowsCopied,
                  totalRows: existing?.totalRows
                };
              });
              return next;
            });

            // Fetch final results
            const resultsResponse = await fetch('/api/migrate/data-results')
            const results = await parseJsonResponse(resultsResponse)
            if (results.ok) {
              setDataMigrationResults(results.tables || [])
              setTotalRows(results.total_rows || 0)
              updateProgressFromResults(results.tables || [])
              const errorTables = (results.tables || []).filter((r: DataMigrationResult) => (r.status || '').toLowerCase() === 'error' || !!r.error)
              if (errorTables.length) {
                setError(`Data migration finished with errors in ${errorTables.length} table(s). See report below.`)
              }
            }

            if (!hasReportedCompletion) {
              setHasReportedCompletion(true)
              onMigrationComplete()
            }
          } else if (data.status === 'failed') {
            if (interval) clearInterval(interval)
            setDataRunning(false)
            setError(data.error || 'Data migration failed')
          }

          if (data.progress && typeof data.progress === 'object') {
            applyProgressPayload(data.progress)
          }
        } catch (err) {
          console.error('Error fetching data migration status:', err)
          // Don't clear interval on error, keep polling
        }
      }, 500) // Poll every 500ms for more responsive table progress updates
    }

    return () => {
      if (interval) clearInterval(interval)
    }
  }, [dataRunning, hasReportedCompletion, onMigrationComplete])

  // Also check for completion when structure is done and data is marked as done
  useEffect(() => {
    if (structureDone && dataDone && !hasReportedCompletion) {
      // Fetch final results when we know data migration is complete
      const fetchResults = async () => {
        try {
          const resultsResponse = await fetch('/api/migrate/data-results')
          const results = await parseJsonResponse(resultsResponse)
          if (results.ok) {
            setDataMigrationResults(results.tables || [])
            setTotalRows(results.total_rows || 0)
            updateProgressFromResults(results.tables || [])
          }

          setHasReportedCompletion(true)
          onMigrationComplete()
        } catch (err) {
          console.error('Error fetching data migration results:', err)
        }
      }

      fetchResults()
    }
  }, [structureDone, dataDone, hasReportedCompletion, onMigrationComplete])

  // Helper function to get selected tables
  const [selectedTablesList, setSelectedTablesList] = useState<{ schema: string; name: string }[]>([]);

  useEffect(() => {
    const fetchSelectedTables = async () => {
      try {
        const response = await fetch('/api/session/get-selected-tables');
        const data = await response.json();

        if (data.ok && data.selectedTables) {
          const tables = data.selectedTables.map((fullName: string) => {
            const parts = fullName.split('.');
            if (parts.length === 2) {
              return { schema: parts[0], name: parts[1] };
            } else {
              return { schema: 'public', name: fullName };
            }
          });
          setSelectedTablesList(tables);
          return;
        }
      } catch (error) {
        console.error('Error fetching selected tables:', error);
      }

      // Fallback to migration results if available
      if (migrationResults?.translation?.objects) {
        const tables = migrationResults.translation.objects.map((obj: TranslatedObject) => {
          const [schema, name] = obj.name.includes('.') ? obj.name.split('.') : ['public', obj.name];
          return { schema, name };
        });
        setSelectedTablesList(tables);
      }
    };

    const fetchSelectedColumns = async () => {
      try {
        const response = await fetch('/api/session/get-selected-columns');
        const data = await response.json();
        if (data.ok && data.selectedColumns) {
          setSelectedColumnsMap(data.selectedColumns || {});
        }
      } catch (error) {
        console.error('Error fetching selected columns:', error);
      }
    };

    fetchSelectedTables();
    fetchSelectedColumns();
  }, [migrationResults, wizardResetId]);

  const getSelectedTables = () => {
    if (selectedTablesList.length > 0) return selectedTablesList;
    // Fallback to any tables present in progress map
    const progressTables = Object.keys(tableProgress).map(key => {
      const parts = key.split('.');
      if (parts.length === 2) {
        return { schema: parts[0], name: parts[1] };
      }
      return { schema: 'public', name: key };
    });
    return progressTables;
  };

  const selectedColumnsLookup = useMemo(() => {
    const next = new Map<string, string[]>();
    Object.entries(selectedColumnsMap).forEach(([tableRef, cols]) => {
      if (cols && cols.length) {
        next.set(tableRef.toLowerCase(), cols);
      }
    });
    return next;
  }, [selectedColumnsMap]);

  const getSelectedColumnsForTable = (tableName: string) => {
    const normalized = tableName.toLowerCase();
    const byFull = selectedColumnsLookup.get(normalized);
    if (byFull) return byFull;
    const nameOnly = normalized.split('.').pop() || normalized;
    return selectedColumnsLookup.get(nameOnly) || [];
  };

  const normalizeTotalRows = (value: number | string | undefined): number | null => {
    if (typeof value === 'number') return value;
    if (typeof value === 'string') {
      const parsed = parseInt(value, 10);
      if (!isNaN(parsed) && isFinite(parsed)) return parsed;
    }
    return null;
  };

  const computeProgressFromRows = (rowsCopied?: number, totalRows?: number | null): number | null => {
    if (typeof rowsCopied !== 'number') return null;
    if (typeof totalRows === 'number' && totalRows > 0) {
      const percent = Math.round((rowsCopied / totalRows) * 100);
      return Math.max(0, Math.min(100, percent));
    }
    return null;
  };

  const normalizeProgressValue = (progressValue: any, existing?: TableProgressEntry): TableProgressEntry => {
    const existingRows = existing?.rowsCopied;
    const existingTotal = existing?.totalRows;
    const existingPercent = existing?.percent ?? 0;

    if (progressValue && typeof progressValue === 'object' && !Array.isArray(progressValue)) {
      const rowsCopied = typeof progressValue.rows_copied === 'number'
        ? progressValue.rows_copied
        : typeof progressValue.rowsCopied === 'number'
          ? progressValue.rowsCopied
          : existingRows;
      const totalRows = typeof progressValue.total_rows === 'number'
        ? progressValue.total_rows
        : typeof progressValue.totalRows === 'number'
          ? progressValue.totalRows
          : existingTotal;
      const percentFromPayload = typeof progressValue.percent === 'number' ? progressValue.percent : undefined;
      const percentFromRows = computeProgressFromRows(rowsCopied, totalRows ?? null);
      const percent = percentFromRows ?? percentFromPayload ?? existingPercent;
      return {
        percent: Math.max(0, Math.min(100, percent)),
        rowsCopied,
        totalRows
      };
    }

    if (typeof progressValue === 'number') {
      return {
        percent: Math.max(0, Math.min(100, progressValue)),
        rowsCopied: existingRows,
        totalRows: existingTotal
      };
    }

    return {
      percent: existingPercent,
      rowsCopied: existingRows,
      totalRows: existingTotal
    };
  };

  const updateProgressFromResults = (results: DataMigrationResult[]) => {
    setTableProgress(prev => {
      const next: Record<string, TableProgressEntry> = { ...prev };
      results.forEach(result => {
        const normalizedTotal = normalizeTotalRows(result.total_rows);
        const percentFromRows = computeProgressFromRows(result.rows_copied, normalizedTotal);
        const previous = prev[result.table];
        const fallbackPercent = (result.status || '').toLowerCase() === 'error' ? 0 : 100;
        next[result.table] = {
          percent: Math.max(0, Math.min(100, percentFromRows ?? previous?.percent ?? fallbackPercent)),
          rowsCopied: typeof result.rows_copied === 'number' ? result.rows_copied : previous?.rowsCopied,
          totalRows: normalizedTotal ?? previous?.totalRows
        };
      });
      return next;
    });
  };

  const getTableProgress = (tableName: string): number => {
    const entry = tableProgress[tableName];
    if (!entry) return 0;
    const percentFromRows = computeProgressFromRows(entry.rowsCopied, entry.totalRows ?? null);
    const percent = percentFromRows ?? entry.percent ?? 0;
    return Math.max(0, Math.min(100, percent));
  };

  // Helper functions to get progress percentages
  const getStructureProgressPercentage = (): number => {
    if (structureDone) return 100;
    if (!structureRunning) return 0;
    return structureProgress;
  };

  const getOverallDataProgress = (): number => {
    const tables = getSelectedTables();
    if (!tables.length) {
      return dataDone ? 100 : dataRunning ? 0 : 0;
    }

    let total = 0;
    let count = 0;

    tables.forEach(tableObj => {
      const tableName = `${tableObj.schema}.${tableObj.name}`;
      const pct = getTableProgress(tableName);
      total += pct;
      count += 1;
    });

    if (count === 0) return dataDone ? 100 : 0;
    return Math.round(Math.max(0, Math.min(100, total / count)));
  };

  const renameColumns = async () => {
    setRenameRunning(true);
    setRenameError(null);
    
    try {
      const response = await fetch('/api/migrate/rename-columns', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
      });
      
      const result = await response.json();
      
      if (result.ok) {
        setRenameDone(true);
        setRenameRunning(false);
        setHasPendingRenames(false);
      } else {
        setRenameError(result.message || 'Failed to rename columns');
        setRenameRunning(false);
      }
    } catch (error: any) {
      setRenameError(error.message || 'Network error during column renaming');
      setRenameRunning(false);
    }
  };

  return (
    <div className="flex gap-6 p-6">
      <div className="flex-1">
        <div className="flex items-start justify-between gap-4 mb-6">
          <h1 className="text-3xl font-bold text-[#085690]">Database Migration</h1>
          <button
            type="button"
            onClick={() => navigate('/extract')}
            className="flex items-center gap-2 px-4 py-2 rounded-lg border-2 border-[#085690] text-[#085690] bg-white hover:bg-[#085690] hover:text-white transition-all font-medium"
          >
            <ArrowLeft size={18} />
            Back
          </button>
        </div>

        {error && (
          <div className="mb-6 p-4 bg-red-50 border-l-4 border-red-500 rounded-lg flex items-start gap-3">
            <AlertCircle className="text-red-500 mt-0.5" size={20} />
            <div>
              <h4 className="font-semibold text-red-900">Migration Error</h4>
              <p className="text-sm text-red-700">{error}</p>
            </div>
          </div>
        )}

        <div className="bg-white rounded-lg shadow-lg p-6 space-y-6 border-t-4 border-[#085690]">
          {/* Structure Migration */}
          <div className="p-6 bg-gradient-to-r from-blue-50 to-white rounded-lg border border-blue-100">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-[#085690] rounded-lg">
                  <FileCode className="text-white" size={24} />
                </div>
                <div>
                  <h3 className="font-bold text-[#085690] text-lg">Structure Migration</h3>
                  <p className="text-sm text-gray-600">AI translates DDL from source to target database dialect</p>
                </div>
              </div>
              {structureDone && (
                <CheckCircle className="text-green-500" size={32} />
              )}
            </div>

            <div>
              <button
                onClick={migrateStructure}
                disabled={structureRunning || structureDone}
                className="w-full bg-gradient-to-r from-[#ec6225] to-[#ff7a3d] text-white px-6 py-3 rounded-lg hover:shadow-xl transition-all disabled:opacity-50 disabled:cursor-not-allowed font-semibold flex items-center justify-center gap-2"
              >
                {structureRunning ? (
                  <>
                    <Loader2 className="animate-spin" size={20} />
                    Translating Schema with AI...
                  </>
                ) : structureDone ? (
                  <>
                    <CheckCircle size={20} />
                    Structure Migration Complete
                  </>
                ) : (
                  <>
                    <Database size={20} />
                    Migrate Structure
                  </>
                )}
              </button>

              {/* Structure Migration Progress Bar (matching Analyze style) */}
              {(structureRunning || structureDone) && (
                <div className="mt-3">
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-sm text-gray-600">Progress:</span>
                    <span className="text-sm font-medium text-[#085690]">{getStructureProgressPercentage()}%</span>
                  </div>
                  <div className="w-full bg-gray-200 rounded-full h-2">
                    <div
                      className="bg-gradient-to-r from-[#085690] to-[#0d6bb3] h-2 rounded-full transition-all duration-300"
                      style={{ width: `${getStructureProgressPercentage()}%` }}
                    />
                  </div>
                </div>
              )}
            </div>

            {structureDone && migrationResults?.translation && (
              <div className="mt-4 p-4 bg-green-50 border-l-4 border-green-500 rounded">
                {migrationResults.translation.warnings && migrationResults.translation.warnings.some((w: string) => w.includes('fallback')) ? (
                  <>
                    <p className="text-sm text-yellow-800 font-medium">
                      ⚠ Using fallback translation engine - AI translation unavailable
                    </p>
                    <p className="text-xs text-yellow-700 mt-1">
                      Fallback performs basic syntax conversions only, Please review DDL before production deployment
                    </p>
                  </>
                ) : (
                  <p className="text-sm text-green-800 font-medium">
                    ✓ Successfully translated {migrationResults.translation.objects.length} database objects using AI
                  </p>
                )}
                {migrationResults.translation.warnings && migrationResults.translation.warnings.length > 0 && !migrationResults.translation.warnings.some((w: string) => w.includes('fallback')) && (
                  <p className="text-xs text-yellow-700 mt-1">
                    ⚠ {migrationResults.translation.warnings.join(', ')}
                  </p>
                )}
              </div>
            )}
          </div>

          {/* Data Migration */}
          <div className={`p-6 bg-gradient-to-r from-orange-50 to-white rounded-lg border border-orange-100 ${!structureDone ? 'opacity-50' : ''}`}>
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-[#ec6225] rounded-lg">
                  <Database className="text-white" size={24} />
                </div>
                <div>
                  <h3 className="font-bold text-[#ec6225] text-lg">Data Migration</h3>
                  <p className="text-sm text-gray-600">Copy table data from source to target database</p>
                </div>
              </div>
              <div className="flex items-center gap-3">
                {(dataRunning || dataDone) && (
                  <CircularProgress
                    percentage={getOverallDataProgress()}
                    size={52}
                    strokeWidth={6}
                    color={dataDone ? '#10b981' : '#ec6225'}
                    label={`${getOverallDataProgress()}%`}
                  />
                )}
                {dataDone && (
                  <CheckCircle className="text-green-500" size={32} />
                )}
              </div>
            </div>

            <div>
              <button
                onClick={migrateData}
                disabled={!structureDone || dataRunning || dataDone}
                className="w-full bg-gradient-to-r from-[#ec6225] to-[#ff7a3d] text-white px-6 py-3 rounded-lg hover:shadow-xl transition-all disabled:opacity-50 disabled:cursor-not-allowed font-semibold flex items-center justify-center gap-2"
              >
                {dataRunning ? (
                  <>
                    <Loader2 className="animate-spin" size={20} />
                    Migrating Data...
                  </>
                ) : dataDone ? (
                  <>
                    <CheckCircle size={20} />
                    Data Migration Complete
                  </>
                ) : (
                  <>
                    <Database size={20} />
                    Migrate Data
                  </>
                )}
              </button>

              {/* Data Migration Progress - Individual table progress only */}
              {dataRunning && (
                <div className="mt-3 space-y-2">
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-sm font-medium text-[#ec6225]">Table Migration Progress:</span>
                    <span className="text-sm text-gray-600">
                      {getSelectedTables().filter(table => getTableProgress(`${table.schema}.${table.name}`) >= 99).length}/{Math.max(getSelectedTables().length, Object.keys(tableProgress).length || 0)} tables completed
                    </span>
                  </div>
                  {/* Individual table progress */}
                  <div className="mt-2 max-h-60 overflow-y-auto pr-2 border border-gray-200 rounded-lg bg-gray-50 p-2">
                    {getSelectedTables().map((tableObj, idx) => {
                      const tableName = `${tableObj.schema}.${tableObj.name}`;
                      const tableResult = dataMigrationResults.find(result => result.table === tableName);
                      const progressEntry = tableProgress[tableName];
                      const selectedCols = getSelectedColumnsForTable(tableName);
                      const selectedColsLabel = selectedCols.length > 3
                        ? `${selectedCols.slice(0, 3).join(', ')} +${selectedCols.length - 3} more`
                        : selectedCols.join(', ');
                      const rowsCopiedValue = typeof (progressEntry?.rowsCopied) === 'number'
                        ? progressEntry?.rowsCopied
                        : (tableResult ? tableResult.rows_copied : 0);
                      const totalRowsNumeric = progressEntry?.totalRows ?? normalizeTotalRows(tableResult?.total_rows);
                      const progress = computeProgressFromRows(rowsCopiedValue, totalRowsNumeric ?? null) ?? getTableProgress(tableName);
                      const safeRowsCopied = typeof rowsCopiedValue === 'number' ? rowsCopiedValue : 0;
                      const roundedProgress = Math.round(Math.max(0, Math.min(100, progress)));

                      // Use actual values for display, with sensible defaults
                      const displayRowsCopied = safeRowsCopied;
                      const showFetching = dataRunning && (totalRowsNumeric === null || typeof totalRowsNumeric !== 'number') && safeRowsCopied === 0;
                      const displayTotalRows = totalRowsNumeric !== null ? totalRowsNumeric : null;
                      const displayTotalRowsStr = displayTotalRows !== null
                        ? displayTotalRows.toLocaleString()
                        : (showFetching ? 'fetching row count…' : 'unknown');

                      return (
                        <div key={idx} className="flex items-center justify-between py-2 px-2 hover:bg-white rounded transition-colors">
                          <div className="flex flex-col min-w-0">
                            <span className="text-sm font-mono text-gray-800 truncate" title={tableName}>
                              {tableName}
                            </span>
                            <span className="text-xs text-gray-600">
                              {displayRowsCopied.toLocaleString()}/{displayTotalRowsStr} rows
                            </span>
                            {selectedCols.length > 0 && (
                              <span className="text-[11px] text-gray-500 truncate" title={selectedCols.join(', ')}>
                                Columns: {selectedColsLabel}
                              </span>
                            )}
                          </div>
                          <div className="flex items-center gap-2">
                            <CircularProgress
                              percentage={roundedProgress}
                              size={36}
                              strokeWidth={3}
                              color={
                                roundedProgress === 100 ? '#10b981' : // green for 100%
                                  roundedProgress >= 75 ? '#f59e0b' : // amber for 75%
                                    roundedProgress >= 50 ? '#f97316' : // orange for 50%
                                      roundedProgress >= 25 ? '#ea580c' : // red-orange for 25%
                                        '#ef4444' // red for <25%
                              }
                              label={roundedProgress <= 0 ? '' : `${roundedProgress}%`}
                            />
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}
            </div>

            {dataDone && dataMigrationResults.length > 0 && !hasMigrationErrors && (
              <div className="mt-4 p-4 bg-green-50 border-l-4 border-green-500 rounded">
                <p className="text-sm text-green-800 font-medium">
                  ✓ Successfully migrated {dataMigrationResults.length} tables with {totalRows.toLocaleString()} total rows
                </p>
              </div>
            )}
            {dataDone && dataMigrationResults.length > 0 && hasMigrationErrors && (
              <div className="mt-4 p-4 bg-red-50 border-l-4 border-red-500 rounded">
                <p className="text-sm text-red-800 font-medium">
                  Migration completed with errors in {dataMigrationResults.length - successfulTables} table(s)
                </p>
              </div>
            )}
          </div>

          {/* Column Rename Section - Only shown after data migration is complete AND pending renames exist */}
          {dataDone && hasPendingRenames && (
            <div className={`p-6 bg-gradient-to-r from-yellow-50 to-white rounded-lg border border-yellow-100 ${renameDone ? 'opacity-75' : ''}`}>
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-3">
                  <div className="p-2 bg-[#f59e0b] rounded-lg">
                    <Database className="text-white" size={24} />
                  </div>
                  <div>
                    <h3 className="font-bold text-[#f59e0b] text-lg">Rename Columns</h3>
                    <p className="text-sm text-gray-600">Apply column name changes to target database after data migration</p>
                  </div>
                </div>
                {renameDone && (
                  <CheckCircle className="text-green-500" size={32} />
                )}
              </div>

              <div>
                <button
                  onClick={renameColumns}
                  disabled={renameRunning || renameDone}
                  className="w-full bg-gradient-to-r from-[#f59e0b] to-[#fbbf24] text-white px-6 py-3 rounded-lg hover:shadow-xl transition-all disabled:opacity-50 disabled:cursor-not-allowed font-semibold flex items-center justify-center gap-2"
                >
                  {renameRunning ? (
                    <>
                      <Loader2 className="animate-spin" size={20} />
                      Renaming Columns...
                    </>
                  ) : renameDone ? (
                    <>
                      <CheckCircle size={20} />
                      Column Renames Complete
                    </>
                  ) : (
                    <>
                      <Database size={20} />
                      Rename Columns
                    </>
                  )}
                </button>

                {renameError && (
                  <div className="mt-3 p-3 bg-red-50 border-l-4 border-red-500 rounded-lg flex items-start gap-3">
                    <AlertCircle className="text-red-500 mt-0.5" size={16} />
                    <div className="text-sm text-red-700">{renameError}</div>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Enhanced Metrics Dashboard */}
          {dataDone && dataMigrationResults.length > 0 && (!hasPendingRenames || renameDone) && (
            <div className="mt-6 grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="bg-gradient-to-br from-green-50 to-green-100 p-4 rounded-lg border border-green-200">
                <div className="text-3xl font-bold text-green-800">{dataMigrationResults.length}</div>
                <div className="text-green-700 font-medium">Tables Migrated</div>
              </div>
              <div className="bg-gradient-to-br from-blue-50 to-blue-100 p-4 rounded-lg border border-blue-200">
                <div className="text-3xl font-bold text-blue-800">{totalRows.toLocaleString()}</div>
                <div className="text-blue-700 font-medium">Total Rows</div>
              </div>
              <div className="bg-gradient-to-br from-purple-50 to-purple-100 p-4 rounded-lg border border-purple-200">
                <div className="text-3xl font-bold text-purple-800">{successRate}%</div>
                <div className="text-purple-700 font-medium">Success Rate</div>
              </div>
            </div>
          )}

          {/* Migration Report */}
          {dataDone && dataMigrationResults.length > 0 && (!hasPendingRenames || renameDone) && (
            <div className="mt-6 p-6 bg-gray-50 rounded-lg border-2 border-[#085690]">
              <h3 className="font-bold text-[#085690] text-lg mb-4 flex items-center gap-2">
                <CheckCircle className="text-green-500" size={24} />
                Migration Report
              </h3>

              <div className="bg-white rounded-lg overflow-hidden border border-gray-200">
                <table className="w-full">
                  <thead>
                    <tr className="bg-gradient-to-r from-[#085690] to-[#0d6bb3] text-white">
                      <th className="px-4 py-3 text-left font-semibold">Table Name</th>
                      <th className="px-4 py-3 text-right font-semibold">Rows Migrated</th>
                      <th className="px-4 py-3 text-center font-semibold">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dataMigrationResults.map((result, idx) => (
                      <tr key={idx} className={idx % 2 === 0 ? 'bg-gray-50' : 'bg-white'}>
                        <td className="px-4 py-3 font-medium text-gray-900">{result.table}</td>
                        <td className="px-4 py-3 text-right text-[#085690] font-semibold">
                          {result.rows_copied.toLocaleString()}
                        </td>
                        <td className="px-4 py-3 text-center">
                          {((result.status || '').toLowerCase() === 'error' || result.error) ? (
                            <span
                              className="inline-flex items-center gap-1 px-3 py-1 bg-red-100 text-red-700 rounded-full text-sm font-medium"
                              title={result.error || 'Error'}
                            >
                              <AlertCircle size={14} />
                              Error
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1 px-3 py-1 bg-green-100 text-green-700 rounded-full text-sm font-medium">
                              <CheckCircle size={14} />
                              {result.status}
                            </span>
                          )}
                          {result.error && (
                            <div className="mt-1 text-left text-xs text-red-700 break-words px-2">
                              {result.error}
                            </div>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                  <tfoot>
                    <tr className="bg-[#085690] text-white font-bold">
                      <td className="px-4 py-3">Total</td>
                      <td className="px-4 py-3 text-right">{totalRows.toLocaleString()}</td>
                      <td className="px-4 py-3 text-center">
                        {dataMigrationResults.length} tables
                      </td>
                    </tr>
                  </tfoot>
                </table>
              </div>
            </div>
          )}

          {/* Export Options */}
          {dataDone && (!hasPendingRenames || renameDone) && (
            <div className="mt-6 p-4 bg-blue-50 rounded-lg border border-blue-200">
              <h4 className="font-semibold text-[#085690] mb-3">Export Migration Report</h4>
              <div className="flex gap-3">
                <a
                  href="/api/migrate/export/pdf"
                  download
                  className="btn-export bg-white border-2 border-[#085690] text-[#085690] hover:bg-[#085690] hover:text-white flex items-center justify-center gap-2"
                >
                  <FileText size={20} />
                  Export PDF
                </a>
                <a
                  href="/api/migrate/export/excel"
                  download
                  className="btn-export bg-white border-2 border-[#085690] text-[#085690] hover:bg-[#085690] hover:text-white flex items-center justify-center gap-2"
                >
                  <FileSpreadsheet size={20} />
                  Export Excel
                </a>
                <a
                  href="/api/migrate/export/json"
                  download
                  className="btn-export bg-white border-2 border-[#085690] text-[#085690] hover:bg-[#085690] hover:text-white flex items-center justify-center gap-2"
                >
                  <FileJson size={20} />
                  Export JSON
                </a>
              </div>
            </div>
          )}

          {/* Proceed to Validation Button */}
          {dataDone && (!hasPendingRenames || renameDone) && (
            <div className="mt-8 flex justify-center">
              <button
                onClick={() => {
                  onMigrationComplete()
                  navigate('/reconcile')
                }}
                className="bg-gradient-to-r from-[#ec6225] to-[#ff7a3d] text-white px-12 py-4 rounded-lg hover:shadow-2xl transition-all font-bold text-lg flex items-center gap-3"
              >
                Proceed to Validation
                <ArrowRight size={24} />
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Success Modal */}
      {showSuccessModal && (!hasPendingRenames || renameDone) && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl p-8 max-w-md w-full mx-4 shadow-2xl">
            <div className="text-center">
              <div className="w-16 h-16 bg-green-100 rounded-full flex items-center justify-center mx-auto mb-4">
                <CheckCircle className="text-green-600" size={32} />
              </div>
              <h3 className="text-2xl font-bold text-gray-900 mb-2">Migration Complete!</h3>
              <p className="text-gray-600 mb-6">
                Successfully migrated {dataMigrationResults.length} tables with {totalRows.toLocaleString()} total rows
              </p>
              <div className="flex gap-3">
                <button
                  onClick={() => {
                    setShowSuccessModal(false)
                    setModalAcknowledged(true)
                  }}
                  className="flex-1 bg-gradient-to-r from-[#ec6225] to-[#ff7a3d] text-white py-3 rounded-lg font-semibold"
                >
                  Continue
                </button>
                <button
                  onClick={() => {
                    setShowSuccessModal(false)
                    setModalAcknowledged(true)
                    navigate('/reconcile')
                  }}
                  className="flex-1 bg-white border-2 border-[#085690] text-[#085690] py-3 rounded-lg font-semibold"
                >
                  Validate Now
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Sidebar: Translated Queries (removed per requirements). The structure
          migration still runs and uses deterministic rule-based DDL, but we no
          longer show the AI/translated DDL sidebar on the Migrate tab. */}
    </div>
  )
}
