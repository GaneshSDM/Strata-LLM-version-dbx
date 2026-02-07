import { Fragment, useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Connection } from '../App'
import { FileText, FileSpreadsheet, FileJson, Database, Table, Eye, Zap, Hash, Box, Lock, Code, ArrowRight, RefreshCw, Clock, ChevronDown, ChevronRight, Info, CheckCircle2 } from 'lucide-react'
import DataLineageLive from '../components/lineage/DataLineageLive'
import { useWizard } from '../components/WizardContext'
import { ensureSessionId, getSessionHeaders } from '../utils/session'

type Props = {
  connections: Connection[]
  onAnalysisComplete: () => void
  onAnalysisRestart: () => void
}

// Add type definitions for our new data structures
type DatabaseDetails = {
  connection?: {
    id: number
    name: string
    db_type: string
  }
  location?: {
    database?: string
    schema?: string
    warehouse?: string
    account?: string
    host?: string
  }
  database_info: {
    type: string
    version: string
    schemas: string[]
    encoding: string
    collation: string
  }
  tables: Array<{
    schema: string
    name: string
    type: string
    row_count: number
    engine?: string
    data_length?: number
    index_length?: number
    total_size?: number
  }>
  columns: Array<{
    schema: string
    table: string
    name: string
    type: string
    nullable: boolean
    default: string | null
    collation: string | null
  }>
  views: any[]
  storage_info?: {
    database_size: {
      total_size?: number
      data_size?: number
      index_size?: number
    }
    tables: Array<{
      schema: string
      name: string
      table?: string
      table_name?: string
      table_schema?: string
      data_length?: number
      index_length?: number
      total_size?: number
      data_size?: number
      index_size?: number
    }>
  }
  driver_unavailable?: boolean
}

type StorageEntry = NonNullable<NonNullable<DatabaseDetails['storage_info']>['tables'][number]>
type StorageEntryList = StorageEntry[]


export default function Analyze({ connections, onAnalysisComplete, onAnalysisRestart }: Props) {
  const navigate = useNavigate()
  const { notify, resetWizardState, wizardResetId } = useWizard()
  const [sourceId, setSourceId] = useState('')
  const [targetId, setTargetId] = useState('')
  const [analyzing, setAnalyzing] = useState(false)
  const [analysisStatus, setAnalysisStatus] = useState<any>(null)
  // Persist the last successful results so re-runs never blank the UI
  const [analysisResults, setAnalysisResults] = useState<any | null>(null)
  const [activeTab, setActiveTab] = useState('overview')
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const [autoRefresh, setAutoRefresh] = useState(false)
  const [refreshInterval, setRefreshInterval] = useState(30)
  const [hasReportedCompletion, setHasReportedCompletion] = useState(false)
  
  // Add new state variables for database details
  const [sourceDetails, setSourceDetails] = useState<DatabaseDetails | null>(null)
  const [targetDetails, setTargetDetails] = useState<DatabaseDetails | null>(null)
  const [loadingSourceDetails, setLoadingSourceDetails] = useState(false)
  const [loadingTargetDetails, setLoadingTargetDetails] = useState(false)
  const [showSourceDetails, setShowSourceDetails] = useState(false)
  const [showTargetDetails, setShowTargetDetails] = useState(false)
  // Add state to track expanded tables
  const [expandedSourceTables, setExpandedSourceTables] = useState<Record<string, boolean>>({})
  const [expandedTargetTables, setExpandedTargetTables] = useState<Record<string, boolean>>({})
  // Add state for expanded databases/schemas
  const [expandedSourceSchemas, setExpandedSourceSchemas] = useState<Record<string, boolean>>({})
  const [expandedTargetSchemas, setExpandedTargetSchemas] = useState<Record<string, boolean>>({})
  // Add state for selected tables
  const [selectedTables, setSelectedTables] = useState<Record<string, boolean>>({})
  const selectedTablesPersistTimer = useRef<number | null>(null)
  const [selectedColumns, setSelectedColumns] = useState<Record<string, Record<string, boolean>>>({});
  const selectedColumnsPersistTimer = useRef<number | null>(null);
  const selectedColumnsEndpointAvailable = useRef(true);
    
  // Add state for column renames
  const [columnRenames, setColumnRenames] = useState<Record<string, Record<string, string>>>({ });
  const columnRenamesPersistTimer = useRef<number | null>(null);
  const columnRenamesEndpointAvailable = useRef(true);
  
  // Add state for rename column dialog
  const [renameDialog, setRenameDialog] = useState({
    open: false,
    tableName: '',
    columnName: '',
    newColumnName: '',
    error: ''
  });
  const [replacePrompt, setReplacePrompt] = useState<{ open: boolean; tables: string[] }>({
    open: false,
    tables: []
  })
  const [replaceStep, setReplaceStep] = useState<'warn' | 'confirm'>('warn')
  const [droppingConflictingTables, setDroppingConflictingTables] = useState(false)
  const [droppedConflictingTables, setDroppedConflictingTables] = useState(false)
  // Add error states
  const [sourceDetailsError, setSourceDetailsError] = useState<string | null>(null)
  const [targetDetailsError, setTargetDetailsError] = useState<string | null>(null)
  const [refreshingSourceMetadata, setRefreshingSourceMetadata] = useState(false)
  const [refreshingTargetMetadata, setRefreshingTargetMetadata] = useState(false)
  const [refreshingAnalyzeMetadata, setRefreshingAnalyzeMetadata] = useState(false)
  const [invalidSelectedTables, setInvalidSelectedTables] = useState<string[]>([])
  const [modifiedSelectedTables, setModifiedSelectedTables] = useState<string[]>([])
  const [oracleSchemaInput, setOracleSchemaInput] = useState<string>('')
  const [showOracleSchemaDialog, setShowOracleSchemaDialog] = useState<boolean>(false)
  const sourceColumnSignatureRef = useRef<Record<string, string>>({})

  const ANALYZE_RESET_MESSAGE =
    'Source or target configuration changed. Previous analysis and downstream steps have been reset.'

  const normalizeTableRef = (schema?: string, name?: string) => {
    const normalizedSchema = (schema || 'default').toLowerCase().trim()
    const normalizedName = (name || '').toLowerCase().trim()
    return `${normalizedSchema}.${normalizedName}`
  }

  const targetSchemaForComparison = (sourceSchema?: string) => {
    const preferred = targetDetails?.location?.schema
    if (preferred && preferred.trim()) return preferred

    // If the target has no explicit schema configured, fall back to the source schema.
    // This keeps comparisons working for engines where the selected schema maps 1:1.
    return sourceSchema || 'default'
  }

  const normalizeTargetComparisonRef = (sourceSchema?: string, name?: string) => {
    return normalizeTableRef(targetSchemaForComparison(sourceSchema), name)
  }

  const targetTableRefSet = useMemo(() => {
    const tables = targetDetails?.tables || []
    return new Set(tables.map(t => normalizeTableRef(t.schema, t.name)))
  }, [targetDetails])

  const DetailField = ({
    label,
    value,
    mono = false,
    wrap = false
  }: {
    label: string
    value?: string
    mono?: boolean
    wrap?: boolean
  }) => {
    if (!value) return null
    const normalizedValue = value.toLowerCase()
    const valueClasses = [
      'flex-1',
      'min-w-0',
      'text-[11px]',
      'font-semibold',
      'text-gray-800',
      mono ? 'font-mono' : '',
      wrap ? 'break-words whitespace-normal' : 'truncate'
    ].join(' ')

    return (
      <div className="min-w-0 rounded-xl border border-gray-200 bg-white px-5 py-3 shadow-sm transition hover:border-gray-300 flex items-center gap-4">
        <span className="text-[11px] font-semibold uppercase tracking-[0.2em] text-gray-500 flex-shrink-0">
          {label} :
        </span>
        <span className={`${valueClasses} text-[12px]`}>{normalizedValue}</span>
      </div>
    )
  }
  
  useEffect(() => {
    onAnalysisRestart()
    setAnalysisStatus(null)
    setAnalyzing(false)
    setHasReportedCompletion(false)
    setLastUpdated(null)
  }, [onAnalysisRestart])

  // Persist selected tables to the backend whenever the selection changes.
  // On unmount/navigation, flush any pending debounce so the selection isn't lost.
  useEffect(() => {
    if (!sourceId) return

    const persist = async () => {
      const selectedTableList = Object.entries(selectedTables)
        .filter(([_, isSelected]) => isSelected)
        .map(([tableName]) => tableName)
      try {
        await fetch('/api/session/set-selected-tables', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', ...getSessionHeaders() },
          body: JSON.stringify({ selectedTables: selectedTableList })
        })
      } catch (err) {
        console.error('Failed to persist selected tables', err)
      }
    }

    if (selectedTablesPersistTimer.current) {
      clearTimeout(selectedTablesPersistTimer.current)
      selectedTablesPersistTimer.current = null
    }

    selectedTablesPersistTimer.current = window.setTimeout(persist, 300)

    return () => {
      if (selectedTablesPersistTimer.current) {
        clearTimeout(selectedTablesPersistTimer.current)
        selectedTablesPersistTimer.current = null
        // Flush immediately on unmount/navigation so selection is saved before leaving
        persist()
      }
    }
  }, [selectedTables, sourceId])

  // Persist selected columns to the backend whenever the selection changes.
  useEffect(() => {
    if (!sourceId) return

    const persist = async () => {
      if (!selectedColumnsEndpointAvailable.current) return
      const selectedColumnsPayload = Object.entries(selectedColumns).reduce<Record<string, string[]>>((acc, [tableRef, cols]) => {
        const selected = Object.entries(cols)
          .filter(([_, isSelected]) => isSelected)
          .map(([name]) => name)
        if (selected.length > 0) {
          acc[tableRef] = selected
        }
        return acc
      }, {})

      try {
        const response = await fetch('/api/session/set-selected-columns', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', ...getSessionHeaders() },
          body: JSON.stringify({ selectedColumns: selectedColumnsPayload })
        })
        if (response.status === 405) {
          selectedColumnsEndpointAvailable.current = false
        }
      } catch (err) {
        console.error('Failed to persist selected columns', err)
      }
    }

    if (selectedColumnsPersistTimer.current) {
      clearTimeout(selectedColumnsPersistTimer.current)
      selectedColumnsPersistTimer.current = null
    }

    selectedColumnsPersistTimer.current = window.setTimeout(persist, 300)

    return () => {
      if (selectedColumnsPersistTimer.current) {
        clearTimeout(selectedColumnsPersistTimer.current)
        selectedColumnsPersistTimer.current = null
        persist()
      }
    }
  }, [selectedColumns, sourceId]);
  
  // Clear column renames on initial load (refresh should drop any previous renames).
  useEffect(() => {
    void fetch('/api/session/clear-column-renames', { method: 'POST' }).catch(() => {});
  }, []);

  // Persist column renames only when explicitly set (debounced)
  useEffect(() => {
    if (!sourceId) return;
    const persist = async () => {
      if (!columnRenamesEndpointAvailable.current) return;
      try {
        const response = await fetch('/api/session/set-column-renames', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ columnRenames })
        });
        if (response.status === 405) {
          columnRenamesEndpointAvailable.current = false;
        }
      } catch (err) {
        console.error('Failed to persist column renames', err);
      }
    };
    if (columnRenamesPersistTimer.current) {
      clearTimeout(columnRenamesPersistTimer.current);
      columnRenamesPersistTimer.current = null;
    }
    columnRenamesPersistTimer.current = window.setTimeout(persist, 300);
    return () => {
      if (columnRenamesPersistTimer.current) {
        clearTimeout(columnRenamesPersistTimer.current);
        columnRenamesPersistTimer.current = null;
        persist();
      }
    };
  }, [columnRenames, sourceId]);

  // Add useEffect hooks to fetch database details when connections are selected
  useEffect(() => {
    console.log('Source ID changed:', sourceId)
    if (sourceId) {
      fetchDatabaseDetails(sourceId, true)
    } else {
      setSourceDetails(null)
      setSourceDetailsError(null)
    }
  }, [sourceId])

  useEffect(() => {
    console.log('Target ID changed:', targetId)
    if (targetId) {
      fetchDatabaseDetails(targetId, false)
    } else {
      setTargetDetails(null)
      setTargetDetailsError(null)
    }
  }, [targetId])

  useEffect(() => {
    setAnalysisStatus(null)
    setAnalysisResults(null)
    setAnalyzing(false)
    setHasReportedCompletion(false)
    setLastUpdated(null)
    setAutoRefresh(false)
    setSelectedTables({})
    setSelectedColumns({})
    setInvalidSelectedTables([])
    setModifiedSelectedTables([])
    sourceColumnSignatureRef.current = {}
    setExpandedSourceTables({})
    setExpandedTargetTables({})
    setExpandedSourceSchemas({})
    setExpandedTargetSchemas({})
    setReplacePrompt({ open: false, tables: [] })
    setActiveTab('overview')
  }, [wizardResetId]);
  
  // Load existing column renames when sourceId changes
  useEffect(() => {
    if (!sourceId) return;
      
    const loadColumnRenames = async () => {
      try {
        const response = await fetch('/api/session/get-column-renames');
        if (response.ok) {
          const data = await response.json();
          if (data.ok && data.columnRenames) {
            setColumnRenames(data.columnRenames);
          }
        }
      } catch (err) {
        console.error('Failed to load column renames', err);
      }
    };
      
    loadColumnRenames();
  }, [sourceId]);
  
  // Function to handle column rename
  const handleRenameColumn = () => {
    // Validate column name
    if (!renameDialog.newColumnName.trim()) {
      setRenameDialog(prev => ({
        ...prev,
        error: 'Column name cannot be empty'
      }));
      return;
    }
      
    // Basic validation for target database naming conventions
    if (/^\s/.test(renameDialog.newColumnName)) {
      setRenameDialog(prev => ({
        ...prev,
        error: 'Column name cannot start with a space'
      }));
      return;
    }
      
    if (/^[0-9]/.test(renameDialog.newColumnName)) {
      setRenameDialog(prev => ({
        ...prev,
        error: 'Column name cannot start with a number'
      }));
      return;
    }
      
    // Update column renames state
    setColumnRenames(prev => {
      const tableRenames = prev[renameDialog.tableName] || {};
      const newTableRenames = {
        ...tableRenames,
        [renameDialog.columnName]: renameDialog.newColumnName
      };
        
      return {
        ...prev,
        [renameDialog.tableName]: newTableRenames
      };
    });
      
    // Close dialog
    setRenameDialog({
      open: false,
      tableName: '',
      columnName: '',
      newColumnName: '',
      error: ''
    });
  };
  
  const buildSourceColumnSignatureMap = (details: DatabaseDetails) => {
    const map: Record<string, string> = {}
    const columns = details.columns || []
    for (const column of columns) {
      const schema = (column.schema || 'default').toLowerCase()
      const table = (column.table || '').toLowerCase()
      const key = `${schema}.${table}`
      const sigPart = `${(column.name || '').toLowerCase()}:${(column.type || '').toLowerCase()}:${column.nullable ? '1' : '0'}`
      map[key] = map[key] ? `${map[key]}|${sigPart}` : sigPart
    }
    return map
  }

  const reconcileSourceSelections = (updated: DatabaseDetails) => {
    const available = new Set(
      (updated.tables || []).map(t => `${(t.schema || 'default').toLowerCase()}.${(t.name || '').toLowerCase()}`)
    )

    const selected = Object.entries(selectedTables)
      .filter(([_, isSelected]) => isSelected)
      .map(([table]) => table)

    const invalid = selected.filter((table) => !available.has(table.toLowerCase()))
    setInvalidSelectedTables(invalid)

    const nextSignatures = buildSourceColumnSignatureMap(updated)
    const prevSignatures = sourceColumnSignatureRef.current

    const modified = selected.filter((table) => {
      const key = table.toLowerCase()
      const prev = prevSignatures[key]
      const next = nextSignatures[key]
      return Boolean(prev && next && prev !== next)
    })

    setModifiedSelectedTables(modified)
    sourceColumnSignatureRef.current = nextSignatures
  }

  const clearInvalidSelections = () => {
    if (invalidSelectedTables.length === 0) return
    setSelectedTables(prev => {
      const next = { ...prev }
      invalidSelectedTables.forEach(table => {
        next[table] = false
      })
      return next
    })
    setInvalidSelectedTables([])
  }

  const refreshSectionMetadata = async (isSource: boolean) => {
    const connectionId = isSource ? sourceId : targetId
    if (!connectionId) return

    if (isSource) {
      setRefreshingSourceMetadata(true)
    } else {
      setRefreshingTargetMetadata(true)
    }

    const updated = await fetchDatabaseDetails(connectionId, isSource, { bypassCache: true })
    if (!updated) {
      notify('Failed to refresh metadata')
    } else if (isSource) {
      reconcileSourceSelections(updated)
    }

    if (isSource) {
      setRefreshingSourceMetadata(false)
    } else {
      setRefreshingTargetMetadata(false)
    }
  }

  const refreshAnalyzeSession = async () => {
    if (!sourceId && !targetId) return

    resetWizardState(ANALYZE_RESET_MESSAGE)
    setRefreshingAnalyzeMetadata(true)

    const sourceOk = sourceId ? await fetchDatabaseDetails(sourceId, true, { bypassCache: true }) : true
    const targetOk = targetId ? await fetchDatabaseDetails(targetId, false, { bypassCache: true }) : true

    if (!sourceOk || !targetOk) {
      notify('Failed to refresh metadata')
    }

    setRefreshingAnalyzeMetadata(false)
  }
  
  const handleOracleSchemaSubmit = async () => {
    if (!oracleSchemaInput.trim()) {
      notify('Please enter a schema name');
      return;
    }
    
    // Determine which connection needs the schema (source or target)
    if (sourceDetailsError?.includes('Oracle database introspection timed out')) {
      // Retry source database details with schema
      await fetchDatabaseDetails(sourceId, true, { schema: oracleSchemaInput });
    } else if (targetDetailsError?.includes('Oracle database introspection timed out')) {
      // Retry target database details with schema
      await fetchDatabaseDetails(targetId, false, { schema: oracleSchemaInput });
    }
    
    // Close dialog and clear input
    setShowOracleSchemaDialog(false);
    setOracleSchemaInput('');
  };

  const startAnalysis = async (skipConflictCheck = false) => {
    if (!sourceId || !targetId) return
    
    // Check if there are any conflicting tables that need to be addressed
    if (!skipConflictCheck) {
      const selectedTableList = Object.entries(selectedTables)
        .filter(([_, isSelected]) => isSelected)
        .map(([tableName, _]) => tableName);
      
      const conflictingTables = selectedTableList.filter(tableName => {
        const [schema, table] = tableName.split('.');
        return targetTableRefSet.has(normalizeTargetComparisonRef(schema, table));
      });
      
      // If there are conflicting tables, show the replace prompt
      if (conflictingTables.length > 0) {
        setReplacePrompt({ open: true, tables: conflictingTables });
        setReplaceStep('warn');
        return;
      }
    }
    
    setAnalysisStatus(null)
    setLastUpdated(null)
    
    try {
      await ensureSessionId()
      // If tables are selected, send them to backend
      const selectedTableList = Object.entries(selectedTables)
        .filter(([_, isSelected]) => isSelected)
        .map(([tableName, _]) => tableName);
      
      if (selectedTableList.length > 0) {
        const selectedTablesResponse = await fetch('/api/session/set-selected-tables', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', ...getSessionHeaders() },
          body: JSON.stringify({ selectedTables: selectedTableList })
        });
        
        if (!selectedTablesResponse.ok) {
          throw new Error('Failed to set selected tables');
        }
      }

      const selectedColumnsPayload = Object.entries(selectedColumns).reduce<Record<string, string[]>>((acc, [tableRef, cols]) => {
        const selected = Object.entries(cols)
          .filter(([_, isSelected]) => isSelected)
          .map(([name]) => name)
        if (selected.length > 0) {
          acc[tableRef] = selected
        }
        return acc
      }, {})

      if (Object.keys(selectedColumnsPayload).length > 0 && selectedColumnsEndpointAvailable.current) {
        const selectedColumnsResponse = await fetch('/api/session/set-selected-columns', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', ...getSessionHeaders() },
          body: JSON.stringify({ selectedColumns: selectedColumnsPayload })
        })

        if (selectedColumnsResponse.status === 405) {
          selectedColumnsEndpointAvailable.current = false
        } else if (!selectedColumnsResponse.ok) {
          throw new Error('Failed to set selected columns')
        }
      }
      
      const sessionResponse = await fetch('/api/session/set-source-target', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...getSessionHeaders() },
        body: JSON.stringify({ sourceId: parseInt(sourceId), targetId: parseInt(targetId) })
      })
      
      if (!sessionResponse.ok) {
        throw new Error('Failed to set session')
      }
      
      const analysisResponse = await fetch('/api/analyze/start', {
        method: 'POST',
        headers: getSessionHeaders()
      })
      const analysisData = await analysisResponse.json()
      
      if (!analysisResponse.ok || !analysisData.ok) {
        throw new Error(analysisData.message || 'Failed to start analysis')
      }
      
      onAnalysisRestart()
      setAnalyzing(true)
      setHasReportedCompletion(false)
    } catch (error) {
      console.error('Error starting analysis:', error)
      setAnalyzing(false)
      // Optionally show an error message to the user
    }
  }

  // Add new function to fetch database details
  const fetchDatabaseDetails = async (
    connectionId: string,
    isSource: boolean,
    opts?: { bypassCache?: boolean, schema?: string }
  ) => {
    if (!connectionId) return null
    
    try {
      await ensureSessionId()
      if (isSource) {
        setLoadingSourceDetails(true)
        setSourceDetailsError(null)
      } else {
        setLoadingTargetDetails(true)
        setTargetDetailsError(null)
      }
      
      console.log(`Fetching database details for connection ${connectionId}, isSource: ${isSource}`)
      
      const cacheBuster = opts?.bypassCache ? `?t=${Date.now()}` : ''
      const requestBody: any = {
        connectionId: parseInt(connectionId),
        role: isSource ? 'source' : 'target'
      };
      
      // Add schema parameter if provided
      if (opts?.schema) {
        requestBody.schema = opts.schema;
      }
      
      const response = await fetch(`/api/database/details${cacheBuster}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...getSessionHeaders(),
          ...(opts?.bypassCache ? { 'Cache-Control': 'no-cache' } : {})
        },
        cache: opts?.bypassCache ? 'no-store' : 'default',
        body: JSON.stringify(requestBody)
      })
      
      const data = await response.json()
      console.log(`Database details response:`, data)
      
      if (data.ok) {
        if (isSource) {
          setSourceDetails(data.data)
          setShowSourceDetails(true) // Auto-expand when data loads
        } else {
          setTargetDetails(data.data)
          setShowTargetDetails(true) // Auto-expand when data loads
        }
        return data.data as DatabaseDetails
      } else {
        const errorMsg = data.message || 'Failed to fetch database details'
        console.error('Error in response:', errorMsg)
        
        // Check if this is an Oracle timeout with schema suggestion
        if (data.timeout && data.suggest_schema_input) {
          if (isSource) {
            setSourceDetailsError('Oracle database introspection timed out. Please enter the schema name to limit the scope of introspection.');
            // Show schema input dialog for Oracle timeouts
            setShowOracleSchemaDialog(true);
          } else {
            setTargetDetailsError('Oracle database introspection timed out. Please enter the schema name to limit the scope of introspection.');
            // Show schema input dialog for Oracle timeouts
            setShowOracleSchemaDialog(true);
          }
          return null;
        } else {
          if (isSource) {
            setSourceDetailsError(errorMsg)
          } else {
            setTargetDetailsError(errorMsg)
          }
          return null
        }
      }
    } catch (error) {
      console.error('Error fetching database details:', error)
      const errorMsg = error instanceof Error ? error.message : 'Failed to connect to server'
      if (isSource) {
        setSourceDetailsError(errorMsg)
      } else {
        setTargetDetailsError(errorMsg)
      }
      return null
    } finally {
      if (isSource) {
        setLoadingSourceDetails(false)
      } else {
        setLoadingTargetDetails(false)
      }
    }
  }

  useEffect(() => {
    if (!analyzing) return
    
    const interval = setInterval(async () => {
      const res = await fetch('/api/analyze/status')
      const data = await res.json()
      setAnalysisStatus(data)
      if (data.done) {
        if (data.resultsSummary && Object.keys(data.resultsSummary).length > 0) {
          setAnalysisResults(data.resultsSummary)
        }
        setAnalyzing(false)
        setLastUpdated(new Date())
        clearInterval(interval)
      }
    }, 1000)
    
    return () => clearInterval(interval)
  }, [analyzing])

  useEffect(() => {
    if (!autoRefresh || !analysisStatus?.done || !sourceId || !targetId) return
    
    const interval = setInterval(() => {
      refreshAnalysis()
    }, refreshInterval * 1000)
    
    return () => clearInterval(interval)
  }, [autoRefresh, refreshInterval, analysisStatus?.done, sourceId, targetId])

  const refreshAnalysis = async () => {
    if (!sourceId || !targetId || analyzing) return
    setAnalysisStatus(null)
    await ensureSessionId()
    await fetch('/api/session/set-source-target', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...getSessionHeaders() },
      body: JSON.stringify({ sourceId: parseInt(sourceId), targetId: parseInt(targetId) })
    })
    
    await fetch('/api/analyze/start', {
      method: 'POST',
      headers: getSessionHeaders()
    })
    onAnalysisRestart()
    setAnalyzing(true)
    setHasReportedCompletion(false)
  }

  const exportReport = async (format: 'pdf' | 'excel' | 'json') => {
    const endpoint = `/api/analyze/export/${format}`
    window.open(endpoint, '_blank')
  }

  const results = analysisResults
  useEffect(() => {
    if (analysisStatus?.done && !hasReportedCompletion) {
      onAnalysisComplete()
      setHasReportedCompletion(true)
    }
  }, [analysisStatus?.done, hasReportedCompletion, onAnalysisComplete])

  const analysisSteps = [
    { label: 'Connecting to databases', start: 0, end: 10 },
    { label: 'Fetching schemas and tables', start: 10, end: 35 },
    { label: 'Analyzing structures', start: 35, end: 60 },
    { label: 'Calculating metrics', start: 60, end: 85 },
    { label: 'Finalizing report', start: 85, end: 100 }
  ]
  const analysisPercent = analysisStatus?.percent ?? 0

  const MetricCard = ({ icon: Icon, label, value, color }: any) => (
    <div className="bg-white rounded-lg p-4 border-l-4 shadow-sm" style={{ borderColor: color }}>
      <div className="flex items-center gap-3">
        <div className="p-2 rounded-lg" style={{ backgroundColor: `${color}15` }}>
          <Icon size={24} style={{ color }} />
        </div>
        <div>
          <div className="text-2xl font-bold" style={{ color }}>{value}</div>
          <div className="text-sm text-gray-600">{label}</div>
        </div>
      </div>
    </div>
  )

  // Helper function to format bytes into human readable format
  const formatBytes = (bytes: number, decimals = 2): string => {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
  };

  const formatLocationPath = (database?: string, schema?: string) => {
    const db = (database || 'DATABASE').toLowerCase()
    const sch = (schema || 'SCHEMA').toLowerCase()
    return `${db}.${sch}`
  }

  const toStorageNumber = (value: any) => {
    const num = Number(value)
    return Number.isFinite(num) ? num : 0
  }

  const getOverallStorageTotals = (storageInfo?: DatabaseDetails['storage_info']) => {
    if (!storageInfo) return null
    const dbSize = storageInfo.database_size || {}
    const tables = storageInfo.tables || []

    let total = toStorageNumber(dbSize.total_size ?? 0)
    let data = toStorageNumber(dbSize.data_size ?? 0)
    let index = toStorageNumber(dbSize.index_size ?? 0)

    // If db-level totals are missing, derive from tables
    if (!total && tables.length > 0) {
      tables.forEach(entry => {
        const dataSize = toStorageNumber(entry.data_size ?? entry.data_length ?? 0)
        const indexSize = toStorageNumber(entry.index_size ?? entry.index_length ?? 0)
        const totalSize = entry.total_size != null ? toStorageNumber(entry.total_size) : dataSize + indexSize
        total += totalSize
        data += dataSize
        index += indexSize
      })
    }

    if (!total && !data && !index) return null
    return { total, data, index }
  }

  const getStorageTablesPool = (preferAnalysisResults: boolean): StorageEntryList => {
    if (preferAnalysisResults) {
      return (analysisResults?.storage_info?.tables || sourceDetails?.storage_info?.tables || []) as StorageEntryList
    }
    return (sourceDetails?.storage_info?.tables || analysisResults?.storage_info?.tables || []) as StorageEntryList
  }

  // Return storage entries scoped to the selected tables (schema.table), or everything when nothing is selected.
  const getSelectedStorageTables = (preferAnalysisResults = false): StorageEntryList => {
    const pool = getStorageTablesPool(preferAnalysisResults)
    if (!pool || pool.length === 0) return []

    const selectedRefs = new Set(
      Object.entries(selectedTables)
        .filter(([_, isSelected]) => isSelected)
        .map(([tableName]) => tableName.toLowerCase())
    )
    Object.entries(selectedColumns).forEach(([tableName, cols]) => {
      if (Object.values(cols).some(Boolean)) {
        selectedRefs.add(tableName.toLowerCase())
      }
    })

    if (selectedRefs.size === 0) return pool
    return pool.filter((entry: StorageEntry) => {
      const schema = (entry.schema || entry.table_schema || 'default').toLowerCase()
      const name = (entry.name || entry.table || entry.table_name || '').toLowerCase()
      if (!name) return false
      const ref = `${schema}.${name}`
      return selectedRefs.has(ref) || selectedRefs.has(name)
    })
  }

  const aggregateStorageTotals = (preferAnalysisResults = false) => {
    const tables = getSelectedStorageTables(preferAnalysisResults)

    return tables.reduce(
      (acc: { total: number; data: number; index: number }, table: StorageEntry) => {
        const dataSize = toStorageNumber(table.data_size ?? table.data_length ?? 0)
        const indexSize = toStorageNumber(table.index_size ?? table.index_length ?? 0)
        const totalSize = table.total_size != null ? toStorageNumber(table.total_size) : dataSize + indexSize

        acc.total += totalSize
        acc.data += dataSize
        acc.index += indexSize
        return acc
      },
      { total: 0, data: 0, index: 0 }
    )
  }

  // Helper function to calculate storage size for selected objects
  const calculateSelectedObjectsSize = (type: 'total' | 'data' | 'index', preferAnalysisResults = false): string => {
    const totals = aggregateStorageTotals(preferAnalysisResults)

    if (type === 'total') {
      return formatBytes(totals.total)
    }
    if (type === 'data') {
      return formatBytes(totals.data)
    }
    return formatBytes(totals.index)
  }

  const selectedStorageTablesFromAnalysis = useMemo(
    () => getSelectedStorageTables(true),
    [analysisResults, sourceDetails, selectedTables, selectedColumns]
  )

  // Helper function to get filtered analysis results based on selected tables
  const getFilteredAnalysisResults = useMemo(() => {
    if (!analysisResults) return null;
    
    // Get selected table references
    const selectedRefs = new Set(
      Object.entries(selectedTables)
        .filter(([_, isSelected]) => isSelected)
        .map(([tableName]) => tableName.toLowerCase())
    );
    const selectedColumnRefs = new Map<string, Set<string>>();
    Object.entries(selectedColumns).forEach(([tableRef, cols]) => {
      const selected = Object.entries(cols)
        .filter(([_, isSelected]) => isSelected)
        .map(([name]) => name.toLowerCase());
      if (selected.length) {
        const normalized = tableRef.toLowerCase();
        selectedRefs.add(normalized);
        selectedColumnRefs.set(normalized, new Set(selected));
      }
    });
    
    // If no tables are selected, return all results
    if (selectedRefs.size === 0) return analysisResults;
    
    // Filter tables
    const filteredTables = (analysisResults.tables || []).filter((table: any) => {
      const schema = (table.schema || 'default').toLowerCase();
      const name = (table.name || '').toLowerCase();
      const ref = `${schema}.${name}`;
      return selectedRefs.has(ref) || selectedRefs.has(name);
    });
    
    // Filter views
    const filteredViews = (analysisResults.views || []).filter((view: any) => {
      const schema = (view.schema || 'default').toLowerCase();
      const name = (view.name || '').toLowerCase();
      const ref = `${schema}.${name}`;
      return selectedRefs.has(ref) || selectedRefs.has(name);
    });
    
    // Filter materialized views
    const filteredMaterializedViews = (analysisResults.materialized_views || []).filter((view: any) => {
      const schema = (view.schema || 'default').toLowerCase();
      const name = (view.name || '').toLowerCase();
      const ref = `${schema}.${name}`;
      return selectedRefs.has(ref) || selectedRefs.has(name);
    });
    
    // Filter columns for selected tables only
    const filteredColumns = (analysisResults.columns || []).filter((column: any) => {
      const schema = (column.schema || 'default').toLowerCase();
      const table = (column.table || '').toLowerCase();
      const ref = `${schema}.${table}`;
      if (!(selectedRefs.has(ref) || selectedRefs.has(table))) return false;
      const selectedCols = selectedColumnRefs.get(ref);
      if (!selectedCols || selectedCols.size === 0) return true;
      return selectedCols.has((column.name || '').toLowerCase());
    });
    
    // Filter data profiles for selected tables only
    const filteredDataProfiles = (analysisResults.data_profiles || []).filter((profile: any) => {
      const schema = (profile.schema || 'default').toLowerCase();
      const table = (profile.table || '').toLowerCase();
      const ref = `${schema}.${table}`;
      return selectedRefs.has(ref) || selectedRefs.has(table);
    });
    
    // Return filtered results
    return {
      ...analysisResults,
      tables: filteredTables,
      views: filteredViews,
      materialized_views: filteredMaterializedViews,
      columns: filteredColumns,
      data_profiles: filteredDataProfiles
    };
  }, [analysisResults, selectedTables, selectedColumns]);

  const sourceStorageTotals = useMemo(
    () => getOverallStorageTotals(sourceDetails?.storage_info),
    [sourceDetails]
  )

  const targetStorageTotals = useMemo(
    () => getOverallStorageTotals(targetDetails?.storage_info),
    [targetDetails]
  )

  const normalizeDisplay = (value?: string | Record<string, any>, fallback = 'N/A') => {
    if (value === null || value === undefined) return fallback
    if (typeof value === 'object') {
      // Special-case Databricks version object: prefer dbsql_version then dbr_version
      const dbsql = (value as any).dbsql_version
      const dbr = (value as any).dbr_version
      const chosen = dbsql || dbr
      return chosen ? String(chosen) : fallback
    }
    const str = String(value)
    return str.toLowerCase()
  }

  const buildColumnSelectionMap = (columns: Array<{ name: string }>, checked: boolean) => {
    return columns.reduce<Record<string, boolean>>((acc, col) => {
      if (col?.name) {
        acc[col.name] = checked
      }
      return acc
    }, {})
  }

  const countSelectedColumns = (tableRef: string, columns: Array<{ name: string }>) => {
    const selectedMap = selectedColumns[tableRef] || {}
    return columns.reduce((count, col) => count + (selectedMap[col.name] ? 1 : 0), 0)
  }

  const renderSourceTables = () => {
    if (!sourceDetails?.tables || sourceDetails.tables.length === 0) return null
    const groupedTables: Record<string, typeof sourceDetails.tables> = {}
    sourceDetails.tables.forEach(table => {
      const schema = table.schema || 'default'
      if (!groupedTables[schema]) {
        groupedTables[schema] = []
      }
      groupedTables[schema].push(table)
    })
    const getColumnsForTable = (schema: string, tableName: string) => {
      return sourceDetails.columns.filter(col =>
        col.table === tableName && col.schema === schema
      )
    }
    const totalColumnCount = sourceDetails.columns.length
    const selectedColumnCount = Object.values(selectedColumns).reduce(
      (sum, cols) => sum + Object.values(cols).filter(Boolean).length,
      0
    )
    const selectionPercent = totalColumnCount > 0
      ? Math.round((selectedColumnCount / totalColumnCount) * 100)
      : 0

    return (
      <div className="mt-3">
        <div className="font-bold text-sm mb-2 flex items-center justify-between text-gray-800">
          <div className="flex items-center gap-2">
            <Table size={16} className="text-blue-600" />
            Schemas & Tables
          </div>
          {totalColumnCount > 0 && (
            <div className="hidden md:flex items-center gap-2 text-[11px] text-gray-600">
              <span>{selectedColumnCount}/{totalColumnCount} columns selected</span>
              <div className="w-24 h-1.5 bg-gray-200 rounded-full">
                <div
                  className="h-1.5 bg-blue-600 rounded-full transition-all"
                  style={{ width: `${selectionPercent}%` }}
                />
              </div>
              <span className="font-semibold text-blue-700">{selectionPercent}%</span>
            </div>
          )}
          <button
            type="button"
            title="Refresh metadata"
            onClick={() => refreshSectionMetadata(true)}
            disabled={refreshingSourceMetadata || loadingSourceDetails}
            className="h-6 w-6 rounded-full flex items-center justify-center text-blue-500/70 hover:text-[#085690] hover:bg-blue-50 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            aria-label="Refresh metadata"
          >
            <RefreshCw size={16} className={refreshingSourceMetadata ? 'animate-spin' : ''} />
          </button>
        </div>
        {(sourceDetails.location?.database || sourceDetails.location?.schema) && (
          <div className="mb-2 rounded-lg border border-blue-100 bg-blue-50/70 px-4 py-3 space-y-1">
            <div className="text-[11px] font-semibold uppercase tracking-tight text-blue-900">Path</div>
            <div
              className="text-[12px] font-semibold font-mono text-blue-700 tracking-wide"
              title={formatLocationPath(sourceDetails.location?.database, sourceDetails.location?.schema)}
            >
              {formatLocationPath(sourceDetails.location?.database, sourceDetails.location?.schema)}
            </div>
          </div>
        )}
        <div className="border border-blue-200 rounded-lg overflow-hidden max-h-96 overflow-y-auto shadow-sm">
          {Object.entries(groupedTables).map(([schema, tables]) => {
            const schemaLabel = schema.toLowerCase()
            return (
              <div key={schema} className="border-b border-gray-200 last:border-b-0">
                <div
                  className="bg-gradient-to-r from-blue-100 to-blue-50 px-3 py-2 cursor-pointer hover:from-blue-200 hover:to-blue-100 transition-all flex items-center justify-between sticky top-0 z-10 border-b border-blue-200"
                  onClick={() => setExpandedSourceSchemas(prev => ({
                    ...prev,
                    [schema]: !prev[schema]
                  }))}
                >
                  <div className="flex items-center gap-2">
                    {expandedSourceSchemas[schema] ? 
                      <ChevronDown size={16} className="text-blue-700" /> : 
                      <ChevronRight size={16} className="text-blue-700" />
                    }
                    <Database size={16} className="text-blue-700" />
                    <span className="font-bold text-blue-900 text-sm">Schema:</span>
                    <span className="font-mono font-bold text-blue-900 text-sm">{schemaLabel}</span>
                    <span className="text-xs font-semibold text-blue-700 bg-blue-200 px-2 py-0.5 rounded-full">
                      {tables.length} {tables.length === 1 ? 'table' : 'tables'}
                    </span>
                  </div>
                  <button
                    onClick={(e) => {
                      e.stopPropagation()
                      const shouldSelect = !tables.every(table => selectedTables[`${schema}.${table.name}`])
                      setSelectedColumns(prev => {
                        const next = { ...prev }
                        tables.forEach(table => {
                          const fullTableName = `${schema}.${table.name}`
                          const tableCols = getColumnsForTable(table.schema || schema, table.name)
                          if (tableCols.length === 0) {
                            if (!shouldSelect) {
                              delete next[fullTableName]
                            }
                            return
                          }
                          if (shouldSelect) {
                            next[fullTableName] = buildColumnSelectionMap(tableCols, true)
                          } else {
                            delete next[fullTableName]
                          }
                        })
                        return next
                      })
                      setSelectedTables(prev => {
                        const nextSelectedTables = { ...prev }
                        const newlySelectedExisting: string[] = []

                        tables.forEach(table => {
                          const fullTableName = `${schema}.${table.name}`
                          const wasSelected = !!prev[fullTableName]
                          nextSelectedTables[fullTableName] = shouldSelect

                          if (shouldSelect && !wasSelected && targetTableRefSet.has(normalizeTargetComparisonRef(schema, table.name))) {
                          newlySelectedExisting.push(fullTableName)
                        }
                        })

                        // Defer showing the prompt until analyze is clicked
                        setReplacePrompt(prev => {
                          const nextTables = [...prev.tables, ...newlySelectedExisting];
                          return { open: false, tables: nextTables };
                        })
                        return nextSelectedTables
                      })
                    }}
                    className="text-xs font-semibold px-3 py-1 bg-blue-600 text-white rounded hover:bg-blue-700 transition-all shadow-sm hover:shadow-md"
                  >
                    {tables.every(table => selectedTables[`${schema}.${table.name}`]) ? 'Deselect All' : 'Select All'}
                  </button>
                </div>

                {expandedSourceSchemas[schema] && (
                  <div className="bg-white">
                    <table className="w-full text-xs">
                      <thead className="bg-gray-100 border-b border-gray-300">
                        <tr>
                          <th className="p-2 text-left font-bold text-gray-700">Select</th>
                          <th className="p-2 text-left font-bold text-gray-700">Table Name</th>
                          <th className="p-2 text-right font-bold text-gray-700">Rows</th>
                          <th className="p-2 text-left font-bold text-gray-700">Columns</th>
                          <th className="p-2 text-left font-bold text-gray-700">Actions</th>
                        </tr>
                      </thead>
                      <tbody>
                        {tables.map((table, index) => {
                          const isExpanded = expandedSourceTables[`${schema}.${table.name}`] || false;
                          const tableColumns = sourceDetails.columns.filter(col => 
                            col.table === table.name && col.schema === table.schema
                          );
                          const fullTableName = `${schema}.${table.name}`;
                          const selectedColumnCount = countSelectedColumns(fullTableName, tableColumns)
                          const columnPercent = tableColumns.length > 0
                            ? Math.round((selectedColumnCount / tableColumns.length) * 100)
                            : 0
                          
                          return (
                            <Fragment key={`${schema}-${table.name}-${index}`}>
                              <tr className="border-b border-gray-200 hover:bg-blue-50 transition-colors">
                                <td className="p-2">
                                  <input 
                                    type="checkbox" 
                                    className="w-4 h-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500"
                                    checked={selectedTables[fullTableName] || false}
                                    onChange={(e) => {
                                      const checked = e.target.checked
                                      if (tableColumns.length > 0) {
                                        setSelectedColumns(prev => {
                                          const next = { ...prev }
                                          if (checked) {
                                            next[fullTableName] = buildColumnSelectionMap(tableColumns, true)
                                          } else {
                                            delete next[fullTableName]
                                          }
                                          return next
                                        })
                                      } else if (!checked) {
                                        setSelectedColumns(prev => {
                                          if (!prev[fullTableName]) return prev
                                          const next = { ...prev }
                                          delete next[fullTableName]
                                          return next
                                        })
                                      }
                                      setSelectedTables(prev => {
                                        const wasSelected = !!prev[fullTableName]
                                        const next = {
                                          ...prev,
                                          [fullTableName]: checked
                                        }

                                        if (checked && !wasSelected && targetTableRefSet.has(normalizeTargetComparisonRef(schema, table.name))) {
                                          // Defer showing the prompt until analyze is clicked
                                          setReplacePrompt(prev => {
                                            const nextTables = [...prev.tables, fullTableName];
                                            return { open: false, tables: nextTables };
                                          });
                                        }

                                        return next
                                      })
                                    }}
                                  />
                                </td>
                                <td className="p-2">
                                  <span className="font-mono font-semibold text-gray-900">{table.name}</span>
                                </td>
                                <td className="p-2 text-right font-semibold text-blue-700">
                                  {table.row_count?.toLocaleString() || '0'}
                                </td>
                                <td className="p-2">
                                  {tableColumns.length > 0 ? (
                                    <div className="min-w-[120px]">
                                      <div className="flex items-center justify-between text-[10px] text-gray-600 mb-1">
                                        <span>{selectedColumnCount}/{tableColumns.length} selected</span>
                                        <span className="font-semibold text-blue-700">{columnPercent}%</span>
                                      </div>
                                      <div className="h-1.5 bg-gray-200 rounded-full">
                                        <div
                                          className="h-1.5 bg-blue-600 rounded-full transition-all"
                                          style={{ width: `${columnPercent}%` }}
                                        />
                                      </div>
                                    </div>
                                  ) : (
                                    <span className="text-xs text-gray-400">—</span>
                                  )}
                                </td>
                                <td className="p-2">
                                  {tableColumns.length > 0 && (
                                    <button 
                                      onClick={() => setExpandedSourceTables(prev => ({
                                        ...prev,
                                        [`${schema}.${table.name}`]: !isExpanded
                                      }))}
                                      className="text-blue-600 hover:text-blue-800 text-xs font-semibold hover:underline"
                                    >
                                      {isExpanded ? 'Hide Columns' : 'Show Columns'}
                                    </button>
                                  )}
                                </td>
                              </tr>
                              {isExpanded && tableColumns.length > 0 && (
                                <tr>
                                  <td colSpan={5} className="p-0">
                                    <div className="bg-gray-50 p-3 border-t border-gray-200">
                                      <div className="flex items-center justify-between mb-2">
                                        <div className="font-medium text-xs text-gray-700">Columns ({tableColumns.length}):</div>
                                        <div className="flex items-center gap-2">
                                          <button
                                            type="button"
                                            onClick={() => {
                                              setSelectedColumns(prev => ({
                                                ...prev,
                                                [fullTableName]: buildColumnSelectionMap(tableColumns, true)
                                              }))
                                              setSelectedTables(prev => ({ ...prev, [fullTableName]: true }))
                                            }}
                                            className="text-[10px] font-semibold text-blue-600 hover:text-blue-800"
                                          >
                                            Select All
                                          </button>
                                          <span className="text-gray-300 text-xs">|</span>
                                          <button
                                            type="button"
                                            onClick={() => {
                                              setSelectedColumns(prev => {
                                                const next = { ...prev }
                                                delete next[fullTableName]
                                                return next
                                              })
                                              setSelectedTables(prev => ({ ...prev, [fullTableName]: false }))
                                            }}
                                            className="text-[10px] font-semibold text-gray-500 hover:text-gray-700"
                                          >
                                            Clear
                                          </button>
                                        </div>
                                      </div>
                                      <div className="max-h-48 overflow-y-auto">
                                        <table className="w-full text-xs">
                                          <thead className="bg-gray-100">
                                            <tr>
                                              <th className="p-2 text-left">Select</th>
                                              <th className="p-2 text-left">Column Name</th>
                                              <th className="p-2 text-left">Data Type</th>
                                              <th className="p-2 text-left">Nullable</th>
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {tableColumns.map((col, colIndex) => (
                                              <tr key={colIndex} className="border-b border-gray-200">
                                                <td className="p-2">
                                                  <input
                                                    type="checkbox"
                                                    className="w-3.5 h-3.5 text-blue-600 rounded border-gray-300 focus:ring-blue-500"
                                                    checked={selectedColumns[fullTableName]?.[col.name] || false}
                                                    onChange={(e) => {
                                                      const checked = e.target.checked
                                                      let hasAny = false
                                                      setSelectedColumns(prev => {
                                                        const next = { ...prev }
                                                        const tableSelection = { ...(next[fullTableName] || {}) }
                                                        if (checked) {
                                                          tableSelection[col.name] = true
                                                        } else {
                                                          delete tableSelection[col.name]
                                                        }
                                                        hasAny = Object.values(tableSelection).some(Boolean)
                                                        if (hasAny) {
                                                          next[fullTableName] = tableSelection
                                                        } else {
                                                          delete next[fullTableName]
                                                        }
                                                        return next
                                                      })
                                                      setSelectedTables(prev => ({ ...prev, [fullTableName]: hasAny }))
                                                    }}
                                                  />
                                                </td>
                                                <td className="p-2 font-mono">
                                                  <div className="flex items-center gap-2">
                                                    {columnRenames[fullTableName]?.[col.name] ? (
                                                      <span className="font-mono">
                                                        <span className="line-through text-gray-500 mr-2">{col.name}</span>
                                                        <span className="text-green-700 font-semibold">{columnRenames[fullTableName][col.name]}</span>
                                                      </span>
                                                    ) : (
                                                      <span className="font-mono">{col.name}</span>
                                                    )}
                                                    <button
                                                      type="button"
                                                      onClick={() => setRenameDialog({
                                                        open: true,
                                                        tableName: fullTableName,
                                                        columnName: col.name,
                                                        newColumnName: columnRenames[fullTableName]?.[col.name] || col.name,
                                                        error: ''
                                                      })}
                                                      className="text-xs bg-blue-100 hover:bg-blue-200 text-blue-800 px-2 py-1 rounded border border-blue-300 transition-colors"
                                                      title="Rename column"
                                                    >
                                                      Rename
                                                    </button>
                                                  </div>
                                                </td>
                                                <td className="p-2 text-gray-600">{col.type}</td>
                                                <td className="p-2">
                                                  <span className={`px-2 py-0.5 rounded text-xs ${col.nullable ? 'bg-yellow-100 text-yellow-800' : 'bg-green-100 text-green-800'}`}>
                                                    {col.nullable ? 'Yes' : 'No'}
                                                  </span>
                                                </td>
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
                                      </div>
                                    </div>
                                  </td>
                                </tr>
                              )}
                            </Fragment>
                          )
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )
          })}
        </div>
      </div>
    )
  }

  const handleReplaceCancel = () => {
    setReplacePrompt({ open: false, tables: [] })
    setReplaceStep('warn')
    setDroppedConflictingTables(false)
  }

  const handleReplaceConfirm = () => {
    if (replaceStep !== 'confirm') {
      setReplacePrompt({ open: false, tables: [] })
      setReplaceStep('warn')
      setDroppedConflictingTables(false)
      // If user clicked confirm but step is not confirm, restart analysis
      startAnalysis();
      return
    }

    const tablesToDrop = (replacePrompt.tables || []).map((ref) => {
      const raw = String(ref || '').trim()
      // Backend will attach database/schema based on configured target connection.
      // Only send the table name (no schema) to avoid mismatches across targets.
      const parts = raw.split('.').filter(Boolean)
      return (parts[parts.length - 1] || raw).trim()
    })

    const dropTables = async () => {
      if (!tablesToDrop.length) return
      setDroppingConflictingTables(true)
      setDroppedConflictingTables(false)
      try {
        const res = await fetch('/api/target/drop-tables', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', ...getSessionHeaders() },
          body: JSON.stringify({ targetConnectionId: parseInt(targetId), tables: tablesToDrop })
        })
        const data = await res.json()
        if (!res.ok || !data.ok) {
          notify('Failed to drop tables')
          return
        }

        if (targetId) {
          setRefreshingTargetMetadata(true)
          await fetchDatabaseDetails(targetId, false, { bypassCache: true })
          setRefreshingTargetMetadata(false)
        }

        setDroppedConflictingTables(true)
        
        // After dropping tables, continue with analysis
        setTimeout(() => {
          setReplacePrompt({ open: false, tables: [] })
          setReplaceStep('warn')
          setDroppedConflictingTables(false)
          // Restart the analysis process, skipping conflict check since we just dropped the tables
          startAnalysis(true);
        }, 1000); // Small delay to allow UI to update
      } catch (e) {
        notify('Failed to drop tables')
      } finally {
        setDroppingConflictingTables(false)
      }
    }

    void dropTables()
  }

  const handleReplaceDrop = () => {
    setReplaceStep('confirm')
  }

  const renderTargetTables = () => {
    if (!targetDetails?.tables || targetDetails.tables.length === 0) return null
    const tablesBySchema = targetDetails.tables.reduce<Record<string, any[]>>((acc, t) => {
      const schema = t.schema || targetDetails.location?.schema || 'PUBLIC'
      if (!acc[schema]) acc[schema] = []
      acc[schema].push(t)
      return acc
    }, {})

    const schemas = Object.keys(tablesBySchema).sort()

    return (
      <div className="space-y-2">
        {schemas.map(schema => {
          const schemaLabel = schema.toLowerCase()
          return (
            <div key={schema} className="border border-orange-200 rounded-lg overflow-hidden shadow-sm">
              <div
                className="bg-gradient-to-r from-orange-100 to-orange-50 px-3 py-2 cursor-pointer hover:from-orange-200 hover:to-orange-100 transition-all flex items-center justify-between border-b border-orange-200"
                onClick={() =>
                  setExpandedTargetSchemas(prev => ({
                    ...prev,
                    [schema]: !prev[schema]
                  }))
                }
              >
                <div className="flex items-center gap-2">
                  {expandedTargetSchemas[schema] ? (
                    <ChevronDown size={16} className="text-orange-700" />
                  ) : (
                    <ChevronRight size={16} className="text-orange-700" />
                  )}
                  <Database size={16} className="text-orange-700" />
                  <span className="font-bold text-orange-900 text-sm">Schema:</span>
                  <span className="font-mono font-bold text-orange-900 text-sm">{schemaLabel}</span>
                  <span className="text-xs font-semibold text-orange-700 bg-orange-200 px-2 py-0.5 rounded-full">
                    {tablesBySchema[schema].length} {tablesBySchema[schema].length === 1 ? 'table' : 'tables'}
                  </span>
                </div>
              </div>

              {expandedTargetSchemas[schema] && (
                <div className="bg-white max-h-64 overflow-y-auto">
                  <table className="w-full text-xs">
                    <thead className="bg-gray-100 sticky top-0 border-b border-gray-300">
                      <tr>
                        <th className="p-2 text-left font-bold text-gray-700">Table</th>
                        <th className="p-2 text-right font-bold text-gray-700">Rows</th>
                        <th className="p-2 text-left font-bold text-gray-700">Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {tablesBySchema[schema].map((table: any, index: number) => {
                        const key = `${schema}.${table.name}`
                        const isExpanded = expandedTargetTables[key] || false
                        const tableColumns = targetDetails.columns.filter(
                          col => col.table === table.name && col.schema === table.schema
                        )

                        return (
                          <Fragment key={`${key}-${index}`}>
                            <tr className="border-b border-gray-200 hover:bg-orange-50 transition-colors">
                              <td className="p-2 font-mono font-semibold text-gray-900">{table.name}</td>
                              <td className="p-2 text-right font-semibold text-orange-700">
                                {table.row_count?.toLocaleString() || '0'}
                              </td>
                              <td className="p-2">
                                {tableColumns.length > 0 && (
                                  <button
                                    onClick={() =>
                                      setExpandedTargetTables(prev => ({
                                        ...prev,
                                        [key]: !isExpanded
                                      }))
                                    }
                                    className="text-blue-600 hover:text-blue-800 text-xs font-semibold hover:underline"
                                  >
                                    {isExpanded ? 'Hide Columns' : 'Show Columns'}
                                  </button>
                                )}
                              </td>
                            </tr>
                            {isExpanded && tableColumns.length > 0 && (
                              <tr>
                                <td colSpan={3} className="p-0">
                                  <div className="bg-gray-50 p-2">
                                    <div className="font-medium text-xs mb-1">Columns:</div>
                                    <div className="max-h-32 overflow-y-auto">
                                      <table className="w-full text-xs">
                                        <thead className="bg-gray-100">
                                          <tr>
                                            <th className="p-1 text-left">Name</th>
                                            <th className="p-1 text-left">Type</th>
                                            <th className="p-1 text-left">Nullable</th>
                                          </tr>
                                        </thead>
                                        <tbody>
                                          {tableColumns.map((col, colIndex) => (
                                            <tr key={colIndex} className="border-b border-gray-200">
                                              <td className="p-1">{col.name}</td>
                                              <td className="p-1">{col.type}</td>
                                              <td className="p-1">{col.nullable ? 'Yes' : 'No'}</td>
                                            </tr>
                                          ))}
                                        </tbody>
                                      </table>
                                    </div>
                                  </div>
                                </td>
                              </tr>
                            )}
                          </Fragment>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )
        })}
      </div>
    )
  }

  return (
    <div className="max-w-7xl">
      {replacePrompt.open && (
        replaceStep === 'warn' ? (
          <div className="fixed bottom-5 right-5 z-50 w-[380px] max-w-[calc(100vw-2.5rem)] rounded-xl border border-gray-200 bg-white shadow-2xl">
            <div className="p-4">
              <div className="text-sm font-semibold text-gray-900">
                The following tables already exist in the target database:
              </div>
              <div className="mt-2 max-h-40 overflow-auto rounded-lg border border-gray-200 bg-gray-50 px-3 py-2">
                <ul className="space-y-1">
                  {(replacePrompt.tables || []).map((t) => {
                    // Parse source table name (e.g., "demo_load.customer")
                    const sourceParts = t.split('.');
                    const sourceSchema = sourceParts[0];
                    const sourceTable = sourceParts.slice(1).join('.'); // Handle table names with dots
                    
                    // Determine target schema (from target config or fallback to source schema)
                    const targetSchema = targetDetails?.location?.schema || sourceSchema;
                    const targetTableRef = `${targetSchema}.${sourceTable}`;
                    
                    return (
                      <li key={t} className="text-xs font-mono text-gray-800">
                        Source: {t} → Target: {targetTableRef}
                      </li>
                    );
                  })}
                </ul>
              </div>
              <div className="mt-4 flex items-center justify-end gap-2">
                <button
                  onClick={handleReplaceCancel}
                  className="rounded-lg border border-gray-300 bg-white px-3 py-1.5 text-sm font-semibold text-gray-700 hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  onClick={handleReplaceDrop}
                  className="rounded-lg bg-red-600 px-3 py-1.5 text-sm font-semibold text-white hover:bg-red-700"
                >
                  Drop
                </button>
              </div>
            </div>
          </div>
        ) : (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
            <div className="w-full max-w-lg rounded-2xl border border-gray-200 bg-white shadow-2xl">
              <div className="p-6">
                <div className="text-base font-bold text-gray-900">
                  Are you sure you want to drop the following tables?
                </div>
                <div className="mt-3 max-h-56 overflow-auto rounded-lg border border-gray-200 bg-gray-50 px-4 py-3">
                  <ul className="space-y-1.5">
                    {(replacePrompt.tables || []).map((t) => {
                      // Parse source table name (e.g., "demo_load.customer")
                      const sourceParts = t.split('.');
                      const sourceSchema = sourceParts[0];
                      const sourceTable = sourceParts.slice(1).join('.'); // Handle table names with dots
                      
                      // Determine target schema (from target config or fallback to source schema)
                      const targetSchema = targetDetails?.location?.schema || sourceSchema;
                      const targetTableRef = `${targetSchema}.${sourceTable}`;
                      
                      return (
                        <li key={t} className="text-sm font-mono text-gray-800 flex items-center gap-2">
                          {droppedConflictingTables && (
                            <CheckCircle2 size={16} className="text-green-600 flex-shrink-0" />
                          )}
                          <span>Source: {t} → Target: {targetTableRef}</span>
                        </li>
                      );
                    })}
                  </ul>
                </div>
                <div className="mt-6 flex items-center justify-end gap-2">
                  {droppedConflictingTables ? (
                    <button
                      onClick={() => {
                        setReplacePrompt({ open: false, tables: [] })
                        setReplaceStep('warn')
                        setDroppedConflictingTables(false)
                      }}
                      className="rounded-lg border border-gray-300 bg-white px-4 py-2 text-sm font-semibold text-gray-700 hover:bg-gray-50"
                    >
                      Close
                    </button>
                  ) : (
                    <>
                      <button
                        onClick={handleReplaceCancel}
                        disabled={droppingConflictingTables}
                        className="rounded-lg border border-gray-300 bg-white px-4 py-2 text-sm font-semibold text-gray-700 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                      >
                        No
                      </button>
                      <button
                        onClick={handleReplaceConfirm}
                        disabled={droppingConflictingTables}
                        className="rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700 disabled:opacity-50 disabled:cursor-not-allowed"
                      >
                        {droppingConflictingTables ? 'Dropping...' : 'Yes'}
                      </button>
                    </>
                  )}
                </div>
              </div>
            </div>
              
            {/* Oracle Schema Input Dialog */}
            {showOracleSchemaDialog && (
              <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
                <div className="w-full max-w-md rounded-2xl border border-gray-200 bg-white shadow-2xl">
                  <div className="p-6">
                    <div className="text-lg font-bold text-gray-900 mb-2">Enter Oracle Schema</div>
                    <p className="text-gray-600 mb-4">
                      Your Oracle database is large and the introspection timed out. 
                      Please enter the specific schema name to limit the scope of introspection.
                    </p>
                    <input
                      type="text"
                      value={oracleSchemaInput}
                      onChange={(e) => setOracleSchemaInput(e.target.value)}
                      placeholder="Enter schema name..."
                      className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 mb-4"
                      autoFocus
                    />
                    <div className="flex items-center justify-end gap-2">
                      <button
                        onClick={() => {
                          setShowOracleSchemaDialog(false);
                          setOracleSchemaInput('');
                        }}
                        className="rounded-lg border border-gray-300 bg-white px-4 py-2 text-sm font-semibold text-gray-700 hover:bg-gray-50"
                      >
                        Cancel
                      </button>
                      <button
                        onClick={handleOracleSchemaSubmit}
                        className="rounded-lg bg-[#085690] px-4 py-2 text-sm font-semibold text-white hover:bg-[#064475]"
                      >
                        Submit
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        )
      )}
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-3xl font-bold text-[#085690]">Database Analysis</h1>
        <button
          type="button"
          title="Refresh metadata"
          onClick={refreshAnalyzeSession}
          disabled={refreshingAnalyzeMetadata || (!sourceId && !targetId)}
          className="h-7 w-7 rounded-full flex items-center justify-center text-gray-500 hover:text-[#085690] hover:bg-gray-100 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          aria-label="Refresh metadata"
        >
          <RefreshCw size={16} className={refreshingAnalyzeMetadata ? 'animate-spin' : ''} />
        </button>
      </div>
      
      <div className="bg-white rounded-lg shadow p-6 mb-6 border-t-4 border-[#085690]">
        <div className="grid grid-cols-2 gap-6 mb-6">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Source</label>
            <select 
              value={sourceId}
              onChange={(e) => {
                const next = e.target.value
                if (sourceId && sourceId !== next) {
                  resetWizardState(ANALYZE_RESET_MESSAGE)
                }
                setSourceId(next)
              }}
              className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
            >
              <option value="">Select source database</option>
              {connections.filter(c => c.id !== parseInt(targetId)).map(conn => (
                <option key={conn.id} value={conn.id.toString()}>
                  {conn.name} ({conn.db_type})
                </option>
              ))}
            </select>
          </div>
          
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Target</label>
            <select 
              value={targetId}
              onChange={(e) => {
                const next = e.target.value
                if (targetId && targetId !== next) {
                  resetWizardState(ANALYZE_RESET_MESSAGE)
                }
                setTargetId(next)
              }}
              className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
            >
              <option value="">Select target database</option>
              {connections.filter(c => c.id !== parseInt(sourceId)).map(conn => (
                <option key={conn.id} value={conn.id.toString()}>
                  {conn.name} ({conn.db_type})
                </option>
              ))}
            </select>
          </div>
        </div>
        
        {/* Table Selection Note */}
        <div className="mb-6 p-4 bg-blue-50 rounded-lg border border-blue-200">
          <div className="flex items-start">
            <Info className="text-blue-500 mr-2 mt-0.5 flex-shrink-0" size={16} />
            <div>
              <h4 className="text-sm font-medium text-blue-800">Table Selection</h4>
              <p className="text-xs text-blue-700 mt-1">
                After selecting source and target databases, you can choose specific tables to migrate. 
                Check the boxes next to table names to select individual tables. 
                You can also use the "Select All" button to choose all tables at once.
              </p>
              {invalidSelectedTables.length > 0 && (
                <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">
                  <div className="font-semibold">Some previously selected tables are no longer available:</div>
                  <div className="mt-1 font-mono text-amber-800 break-words">
                    {invalidSelectedTables.join(', ')}
                  </div>
                  <div className="mt-2">
                    <button
                      type="button"
                      onClick={clearInvalidSelections}
                      className="text-amber-900 underline font-semibold hover:text-amber-950"
                    >
                      Remove invalid selections
                    </button>
                  </div>
                </div>
              )}
              {modifiedSelectedTables.length > 0 && (
                <div className="mt-3 rounded-lg border border-blue-200 bg-white/70 px-3 py-2 text-xs text-blue-900">
                  <div className="font-semibold">Some selected tables have changed. Please review before continuing:</div>
                  <div className="mt-1 font-mono text-blue-800 break-words">
                    {modifiedSelectedTables.join(', ')}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
        
        {/* Add Database Details Preview Section */}
        {(sourceId || targetId) && (
          <div className="grid grid-cols-2 gap-6 mb-6">
            {/* Source Database Details */}
              {sourceId && (
            <div className="border border-gray-200 rounded-lg p-4">
              <div 
                className="flex items-center justify-between cursor-pointer"
                onClick={() => setShowSourceDetails(!showSourceDetails)}
              >
                <h3 className="font-semibold text-[#085690]">Source Database Details</h3>
                {showSourceDetails ? 
                  <ChevronDown size={20} className="text-gray-500" /> : 
                  <ChevronRight size={20} className="text-gray-500" />
                }
              </div>
              
              {loadingSourceDetails ? (
                <div className="mt-3 text-sm text-gray-500 flex items-center gap-2">
                  <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-[#085690]"></div>
                  Loading source database details...
                </div>
              ) : sourceDetailsError ? (
                <div className="mt-3 p-3 bg-red-50 border border-red-200 rounded-lg">
                  <p className="text-sm text-red-700">
                    <strong>Error:</strong> {sourceDetailsError}
                  </p>
                </div>
              ) : sourceDetails && showSourceDetails ? (
                <div className="mt-3 space-y-3">
                  {/* Primary Info Cards - Large and Prominent */}
                  <div className="grid grid-cols-2 gap-3">
                    <div className="min-w-0 rounded-lg bg-gradient-to-br from-blue-100 via-blue-50 to-white border border-blue-200 px-3 py-2 shadow-sm hover:shadow-md transition-all">
                      <div className="text-[10px] font-semibold uppercase tracking-wider text-blue-700 mb-1">DATABASE TYPE</div>
                      <div className="text-sm font-semibold text-[#085690] leading-snug truncate" title={normalizeDisplay(sourceDetails.database_info?.type)}>
                        {normalizeDisplay(sourceDetails.database_info?.type)}
                      </div>
                    </div>
                    <div className="min-w-0 rounded-lg bg-gradient-to-br from-blue-100 via-blue-50 to-white border border-blue-200 px-3 py-2 shadow-sm hover:shadow-md transition-all">
                      <div className="text-[10px] font-semibold uppercase tracking-wider text-blue-700 mb-1">CONNECTION NAME</div>
                      <div className="text-sm font-semibold text-[#085690] leading-snug truncate" title={normalizeDisplay(sourceDetails.connection?.name)}>
                        {normalizeDisplay(sourceDetails.connection?.name || undefined)}
                      </div>
                    </div>
                  </div>

                  {/* Secondary Info - Medium Size */}
                  {sourceDetails.database_info?.type === 'Databricks' ? (
                    // Inline layout for Databricks - Encoding label and value on same line
                    <div className="rounded-lg bg-blue-50 border border-blue-200 px-3 py-2">
                      <div className="flex items-center gap-2">
                        <div className="text-[10px] font-semibold uppercase tracking-wide text-gray-600 mb-0">ENCODING</div>
                        <div className="text-[11px] font-medium text-gray-700">{normalizeDisplay(sourceDetails.database_info?.encoding)}</div>
                      </div>
                    </div>
                  ) : (
                    // Two column layout for other databases
                    <div className="grid grid-cols-2 gap-3">
                      <div className="rounded-lg bg-blue-50 border border-blue-200 px-3 py-2">
                        <div className="text-[10px] font-semibold uppercase tracking-wide text-gray-600 mb-0.5">VERSION</div>
                        <div className="text-[11px] font-medium text-gray-700">{normalizeDisplay(sourceDetails.database_info?.version)}</div>
                      </div>
                      <div className="rounded-lg bg-blue-50 border border-blue-200 px-3 py-2">
                        <div className="text-[10px] font-semibold uppercase tracking-wide text-gray-600 mb-0.5">ENCODING</div>
                        <div className="text-[11px] font-medium text-gray-700">{normalizeDisplay(sourceDetails.database_info?.encoding)}</div>
                      </div>
                    </div>
                  )}

                  {/* Connection Location - Enhanced Hierarchy */}
                  {sourceDetails.location && (
                    <div className="rounded-xl border border-blue-100 bg-white/90 px-4 py-4 shadow-md space-y-3">
                      <div className="flex items-center gap-2 text-sm font-semibold text-gray-900">
                        <Database size={16} className="text-blue-600" />
                        <span>Connection Location</span>
                      </div>
                      
                      {(sourceDetails.location.database || sourceDetails.location.schema) && (
                        <div className="rounded-lg border border-blue-100 bg-blue-50/60 px-4 py-3 space-y-1">
                          <div className="text-[11px] font-semibold uppercase tracking-tight text-blue-900">Path</div>
                          <div
                            className="text-[12px] font-semibold font-mono text-blue-700 tracking-wide"
                            title={formatLocationPath(sourceDetails.location.database, sourceDetails.location.schema)}
                          >
                            {formatLocationPath(sourceDetails.location.database, sourceDetails.location.schema)}
                          </div>
                        </div>
                      )}
                      
                      <div className="space-y-2">
                        <DetailField label="Database" value={sourceDetails.location.database} mono />
                        <DetailField label="Schema" value={sourceDetails.location.schema} mono />
                        <DetailField label="Host" value={sourceDetails.location.host} mono wrap />
                        {sourceDetails.location.account && <DetailField label="Account" value={sourceDetails.location.account} mono />}
                        {sourceDetails.location.warehouse && <DetailField label="Warehouse" value={sourceDetails.location.warehouse} mono />}
                      </div>
                    </div>
                  )}
                  
                  {/* Overall Storage Information for Source */}
                  {sourceStorageTotals && (
                    <div className="mt-3 p-3 bg-gradient-to-br from-blue-50 to-blue-100 rounded-lg border border-blue-300 shadow-sm">
                      <h4 className="font-bold text-blue-900 mb-2 text-xs flex items-center gap-2">
                        <Box size={16} className="text-blue-600" />
                        Storage Information (All Objects)
                      </h4>
                      <div className="grid grid-cols-3 gap-2">
                        <div className="bg-white p-2 rounded border border-blue-200 shadow-sm hover:shadow-md transition-all">
                          <div className="text-[10px] font-semibold text-gray-600 mb-0.5">Total Size</div>
                          <div className="font-bold text-blue-600 text-sm">
                            {formatBytes(sourceStorageTotals.total)}
                          </div>
                        </div>
                        <div className="bg-white p-2 rounded border border-green-200 shadow-sm hover:shadow-md transition-all">
                          <div className="text-[10px] font-semibold text-gray-600 mb-0.5">Data Size</div>
                          <div className="font-bold text-green-600 text-sm">
                            {formatBytes(sourceStorageTotals.data)}
                          </div>
                        </div>
                        <div className="bg-white p-2 rounded border border-purple-200 shadow-sm hover:shadow-md transition-all">
                          <div className="text-[10px] font-semibold text-gray-600 mb-0.5">Index Size</div>
                          <div className="font-bold text-purple-600 text-sm">
                            {formatBytes(sourceStorageTotals.index)}
                          </div>
                        </div>
                      </div>
                    </div>
                  )}
                  
                  {/* Storage Information on Selected Objects for Source */}
                  {sourceDetails.storage_info && (Object.keys(selectedTables).some(key => selectedTables[key]) || Object.values(selectedColumns).some(cols => Object.values(cols).some(Boolean))) && (
                    <div className="mt-3 p-3 bg-gradient-to-br from-indigo-50 to-indigo-100 rounded-lg border border-indigo-300 shadow-sm">
                      <h4 className="font-bold text-indigo-900 mb-2 text-xs flex items-center gap-2">
                        <Box size={16} className="text-indigo-600" />
                        Storage Information on Selected Objects
                      </h4>
                      <div className="grid grid-cols-3 gap-2">
                        <div className="bg-white p-2 rounded border border-indigo-200 shadow-sm hover:shadow-md transition-all">
                          <div className="text-[10px] font-semibold text-gray-600 mb-0.5">Total Size</div>
                          <div className="font-bold text-indigo-600 text-sm">
                            {calculateSelectedObjectsSize('total')}
                          </div>
                        </div>
                        <div className="bg-white p-2 rounded border border-green-200 shadow-sm hover:shadow-md transition-all">
                          <div className="text-[10px] font-semibold text-gray-600 mb-0.5">Data Size</div>
                          <div className="font-bold text-green-600 text-sm">
                            {calculateSelectedObjectsSize('data')}
                          </div>
                        </div>
                        <div className="bg-white p-2 rounded border border-purple-200 shadow-sm hover:shadow-md transition-all">
                          <div className="text-[10px] font-semibold text-gray-600 mb-0.5">Index Size</div>
                          <div className="font-bold text-purple-600 text-sm">
                            {calculateSelectedObjectsSize('index')}
                          </div>
                        </div>
                      </div>
                    </div>
                  )}
                  
                  {renderSourceTables()}
                </div>
              ) : null}
            </div>
            )}
            
            {/* Target Database Details */}
            {targetId && (
            <div className="border border-gray-200 rounded-lg p-4">
              <div 
                className="flex items-center justify-between cursor-pointer"
                onClick={() => setShowTargetDetails(!showTargetDetails)}
              >
                <h3 className="font-semibold text-[#ec6225]">Target Database Details</h3>
                {showTargetDetails ? 
                  <ChevronDown size={20} className="text-gray-500" /> : 
                  <ChevronRight size={20} className="text-gray-500" />
                }
              </div>
              
              {loadingTargetDetails ? (
                <div className="mt-3 text-sm text-gray-500 flex items-center gap-2">
                  <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-[#ec6225]"></div>
                  Loading target database details...
                </div>
              ) : targetDetailsError ? (
                <div className="mt-3 p-3 bg-red-50 border border-red-200 rounded-lg">
                  <p className="text-sm text-red-700">
                    <strong>Error:</strong> {targetDetailsError}
                  </p>
                </div>
              ) : targetDetails && showTargetDetails ? (
                <div className="mt-3 space-y-3">
                  {/* Primary Info Cards - Large and Prominent */}
                  <div className="grid grid-cols-2 gap-3">
                    <div className="min-w-0 rounded-lg bg-gradient-to-br from-orange-100 via-orange-50 to-white border border-orange-200 px-3 py-2 shadow-sm hover:shadow-md transition-all">
                      <div className="text-[10px] font-semibold uppercase tracking-wider text-orange-700 mb-1">DATABASE TYPE</div>
                      <div className="text-sm font-semibold text-[#ec6225] leading-snug truncate" title={normalizeDisplay(targetDetails.database_info?.type)}>
                        {normalizeDisplay(targetDetails.database_info?.type)}
                      </div>
                    </div>
                    <div className="min-w-0 rounded-lg bg-gradient-to-br from-orange-100 via-orange-50 to-white border border-orange-200 px-3 py-2 shadow-sm hover:shadow-md transition-all">
                      <div className="text-[10px] font-semibold uppercase tracking-wider text-orange-700 mb-1">CONNECTION NAME</div>
                      <div className="text-sm font-semibold text-[#ec6225] leading-snug truncate" title={normalizeDisplay(targetDetails.connection?.name)}>
                        {normalizeDisplay(targetDetails.connection?.name || undefined)}
                      </div>
                    </div>
                  </div>

                  {/* Secondary Info - Medium Size */}
                  {targetDetails.database_info?.type === 'Databricks' ? (
                    // Inline layout for Databricks - Encoding label and value on same line
                    <div className="rounded-lg bg-orange-50 border border-orange-200 px-3 py-2">
                      <div className="flex items-center gap-2">
                        <div className="text-[10px] font-semibold uppercase tracking-wide text-gray-600 mb-0">ENCODING</div>
                        <div className="text-[11px] font-medium text-gray-700">{normalizeDisplay(targetDetails.database_info?.encoding)}</div>
                      </div>
                    </div>
                  ) : (
                    // Two column layout for other databases
                    <div className="grid grid-cols-2 gap-3">
                      <div className="rounded-lg bg-orange-50 border border-orange-200 px-3 py-2">
                        <div className="text-[10px] font-semibold uppercase tracking-wide text-gray-600 mb-0.5">VERSION</div>
                        <div className="text-[11px] font-medium text-gray-700">{normalizeDisplay(targetDetails.database_info?.version)}</div>
                      </div>
                      <div className="rounded-lg bg-orange-50 border border-orange-200 px-3 py-2">
                        <div className="text-[10px] font-semibold uppercase tracking-wide text-gray-600 mb-0.5">ENCODING</div>
                        <div className="text-[11px] font-medium text-gray-700">{normalizeDisplay(targetDetails.database_info?.encoding)}</div>
                      </div>
                    </div>
                  )}

                  {/* Connection Location - Enhanced Hierarchy */}
                  {targetDetails.location && (
                    <div className="rounded-xl border border-orange-100 bg-white/90 px-4 py-4 shadow-md space-y-3">
                      <div className="flex items-center gap-2 text-sm font-semibold text-gray-900">
                        <Database size={16} className="text-orange-600" />
                        <span>Connection Location</span>
                      </div>
                      
                      {(targetDetails.location.database || targetDetails.location.schema) && (
                        <div className="rounded-lg border border-orange-100 bg-orange-50/60 px-4 py-3 space-y-1">
                          <div className="text-[11px] font-semibold uppercase tracking-tight text-orange-900">Path</div>
                          <div
                            className="text-[12px] font-semibold font-mono text-orange-700 tracking-wide"
                            title={formatLocationPath(targetDetails.location.database, targetDetails.location.schema)}
                          >
                            {formatLocationPath(targetDetails.location.database, targetDetails.location.schema)}
                          </div>
                        </div>
                      )}
                      
                      <div className="space-y-2">
                        <DetailField label="Database" value={targetDetails.location.database} mono />
                        <DetailField label="Schema" value={targetDetails.location.schema} mono />
                        <DetailField label="Host" value={targetDetails.location.host} mono wrap />
                        {targetDetails.location.warehouse && <DetailField label="Warehouse" value={targetDetails.location.warehouse} mono />}
                        {targetDetails.location.account && <DetailField label="Account" value={targetDetails.location.account} mono wrap />}
                      </div>
                    </div>
                  )}
                  
                  {/* Storage Dashboard for Target */}
                  {targetDetails.storage_info && targetStorageTotals && (
                    <div className="mt-3 p-3 bg-gradient-to-br from-orange-50 to-orange-100 rounded-lg border border-orange-300 shadow-sm">
                      <h4 className="font-bold text-orange-900 mb-2 text-xs flex items-center gap-2">
                        <Box size={16} className="text-orange-600" />
                        Storage Information
                      </h4>
                      <div className="grid grid-cols-3 gap-2">
                        <div className="bg-white p-2 rounded border border-blue-200 shadow-sm hover:shadow-md transition-all">
                          <div className="text-[10px] font-semibold text-gray-600 mb-0.5">Total Size</div>
                          <div className="font-bold text-blue-600 text-sm">
                            {formatBytes(targetStorageTotals.total)}
                          </div>
                        </div>
                        <div className="bg-white p-2 rounded border border-green-200 shadow-sm hover:shadow-md transition-all">
                          <div className="text-[10px] font-semibold text-gray-600 mb-0.5">Data Size</div>
                          <div className="font-bold text-green-600 text-sm">
                            {formatBytes(targetStorageTotals.data)}
                          </div>
                        </div>
                        <div className="bg-white p-2 rounded border border-purple-200 shadow-sm hover:shadow-md transition-all">
                          <div className="text-[10px] font-semibold text-gray-600 mb-0.5">Index Size</div>
                          <div className="font-bold text-purple-600 text-sm">
                            {formatBytes(targetStorageTotals.index)}
                          </div>
                        </div>
                      </div>
                    </div>
                  )}
                  
                  {targetDetails.tables && targetDetails.tables.length > 0 && (
                    <div className="mt-3">
                      <div className="font-bold text-sm mb-2 flex items-center justify-between text-gray-800">
                        <div className="flex items-center gap-2">
                          <Table size={16} className="text-orange-600" />
                          Schemas & Tables
                        </div>
                        <button
                          type="button"
                          title="Refresh metadata"
                          onClick={() => refreshSectionMetadata(false)}
                          disabled={refreshingTargetMetadata || loadingTargetDetails}
                          className="h-6 w-6 rounded-full flex items-center justify-center text-orange-500/70 hover:text-[#ec6225] hover:bg-orange-50 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                          aria-label="Refresh metadata"
                        >
                          <RefreshCw size={16} className={refreshingTargetMetadata ? 'animate-spin' : ''} />
                        </button>
                      </div>
                      {(targetDetails.location?.database || targetDetails.location?.schema) && (
                        <div className="mb-2 rounded-lg border border-orange-100 bg-orange-50/70 px-4 py-3 space-y-1">
                          <div className="text-[11px] font-semibold uppercase tracking-tight text-orange-900">Path</div>
                          <div
                            className="text-[12px] font-semibold font-mono text-orange-700 tracking-wide"
                            title={formatLocationPath(targetDetails.location?.database, targetDetails.location?.schema)}
                          >
                            {formatLocationPath(targetDetails.location?.database, targetDetails.location?.schema)}
                          </div>
                        </div>
                      )}

                      {renderTargetTables()}
                    </div>
                  )}
                </div>
              ) : null}
            </div>
            )}
          </div>
        )}
        
        <div className="space-y-4">
          <div className="flex gap-3 items-center flex-wrap">
            <button
              onClick={() => startAnalysis()}
              disabled={!sourceId || !targetId || !sourceDetails || !targetDetails || analyzing || invalidSelectedTables.length > 0}
              className="btn-primary shadow-lg disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {analyzing ? 'Analyzing...' : analysisResults ? 'Re-run Analysis' : 'Start Analysis'}
            </button>
            
            {analysisResults && (
              <>
                <button
                  onClick={refreshAnalysis}
                  disabled={analyzing}
                  className="btn-primary flex items-center gap-2 shadow-lg disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  <RefreshCw size={18} className={analyzing ? 'animate-spin' : ''} />
                  Refresh Data
                </button>
                
                <div className="flex items-center gap-2 px-4 py-2 bg-gray-100 rounded-lg">
                  <Clock size={16} className="text-[#085690]" />
                  <span className="text-sm text-gray-700">
                    Last updated: {lastUpdated ? lastUpdated.toLocaleTimeString() : 'Never'}
                  </span>
                </div>
                
                <button
                  onClick={() => exportReport('pdf')}
                  disabled={analyzing}
                  className="btn-export flex items-center gap-2 border-2 border-[#085690] text-[#085690] hover:bg-[#085690] hover:text-white disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:bg-transparent disabled:hover:text-[#085690]"
                >
                  <FileText size={18} />
                  Export PDF
                </button>
                <button
                  onClick={() => exportReport('excel')}
                  disabled={analyzing}
                  className="btn-export flex items-center gap-2 border-2 border-[#085690] text-[#085690] hover:bg-[#085690] hover:text-white disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:bg-transparent disabled:hover:text-[#085690]"
                >
                  <FileSpreadsheet size={18} />
                  Export Excel
                </button>
                <button
                  onClick={() => exportReport('json')}
                  disabled={analyzing}
                  className="btn-export flex items-center gap-2 border-2 border-[#085690] text-[#085690] hover:bg-[#085690] hover:text-white disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:bg-transparent disabled:hover:text-[#085690]"
                >
                  <FileJson size={18} />
                  Export JSON
                </button>
              </>
            )}
          </div>
          
          {analysisResults && (
            <div className="flex items-center gap-4 p-4 bg-gray-50 rounded-lg border border-gray-200">
              <div className="flex items-center gap-2">
                <input
                  type="checkbox"
                  id="autoRefresh"
                  checked={autoRefresh}
                  onChange={(e) => setAutoRefresh(e.target.checked)}
                  className="w-4 h-4 text-[#085690] border-gray-300 rounded focus:ring-[#085690]"
                />
                <label htmlFor="autoRefresh" className="text-sm font-medium text-gray-700">
                  Auto-refresh
                </label>
              </div>
              
              {autoRefresh && (
                <div className="flex items-center gap-2">
                  <label className="text-sm text-gray-600">Every:</label>
                  <select
                    value={refreshInterval}
                    onChange={(e) => setRefreshInterval(Number(e.target.value))}
                    className="border border-gray-300 rounded px-2 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-[#085690]"
                  >
                    <option value={10}>10 seconds</option>
                    <option value={30}>30 seconds</option>
                    <option value={60}>1 minute</option>
                    <option value={120}>2 minutes</option>
                    <option value={300}>5 minutes</option>
                  </select>
                </div>
              )}
              
              <div className="text-sm text-gray-500 ml-auto">
                {autoRefresh ? (
                  <span className="flex items-center gap-2">
                    <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
                    Auto-refresh enabled
                  </span>
                ) : (
                  'Auto-refresh disabled'
                )}
              </div>
            </div>
          )}
        </div>
      </div>
      
      {(analyzing || analysisStatus?.done) && (
        <div className="bg-white rounded-lg shadow p-6 mb-6">
          <div className="flex flex-col items-center gap-6 text-center">
            <div className="max-w-xl">
              <h3 className="font-semibold mb-2 text-[#085690]">
                {analysisStatus?.done ? 'Analysis Complete' : 'Analysis in Progress'}
              </h3>
              <p className="text-sm text-gray-600">
                {analysisStatus?.done
                  ? 'All analysis steps completed.'
                  : 'Running analysis. Steps update automatically.'}
              </p>
            </div>

            <div className="w-full max-w-5xl relative overflow-hidden rounded-2xl border border-white/60 bg-gradient-to-br from-white/80 to-white/60 shadow-glass-lg px-6 py-5">
              <div className="absolute inset-0 bg-gradient-to-br from-primary-500/5 via-transparent to-accent-500/10" />
              <div className="relative mb-3 text-center">
                <div className="text-sm font-semibold text-[#085690]">Analysis Steps</div>
              </div>
              <div className="space-y-2">
                {analysisSteps.map((step) => {
                  const isComplete = analysisPercent >= step.end
                  const isActive = analysisPercent >= step.start && analysisPercent < step.end
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
                  const raw = stepSpan > 0 ? ((analysisPercent - step.start) / stepSpan) * 100 : 0
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
                    style={{ width: `${analysisPercent}%` }}
                  />
                </div>
                <div className="mt-2 text-[11px] font-semibold text-gray-500 text-right">
                  Overall {analysisPercent}%
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
      
      {analysisResults && Object.keys(analysisResults).length > 0 && (
        <>
          <div className="bg-white rounded-lg shadow p-6 mb-6 border-t-4 border-[#085690]">
            <h2 className="text-xl font-bold mb-4 text-[#085690]">Database Information</h2>
            <div className="grid grid-cols-4 gap-4 mb-4">
              <div>
                <div className="text-sm text-gray-600">Type</div>
                <div className="font-medium text-gray-800">{results.database_info?.type || 'N/A'}</div>
              </div>
              <div>
                <div className="text-sm text-gray-600">Version</div>
                <div className="font-medium text-gray-800">{results.database_info?.version || 'N/A'}</div>
              </div>
              <div>
                <div className="text-sm text-gray-600">Encoding</div>
                <div className="font-medium text-gray-800">{results.database_info?.encoding || 'N/A'}</div>
              </div>
              <div>
                <div className="text-sm text-gray-600">Collation</div>
                <div className="font-medium text-gray-800">{results.database_info?.collation || 'N/A'}</div>
              </div>
            </div>
            
            {/* Storage Information on Selected Objects */}
            {results.storage_info && (
              <div className="mb-4 p-4 bg-gradient-to-br from-indigo-50 to-indigo-100 rounded-lg border border-indigo-300 shadow-sm">
                <div className="flex items-center justify-between gap-3 mb-2">
                  <h3 className="font-medium text-indigo-800 flex items-center gap-2">
                    <Box size={16} className="text-indigo-600" />
                    Storage Information on Selected Objects
                  </h3>
                  {(!Object.values(selectedTables).some(Boolean) && !Object.values(selectedColumns).some(cols => Object.values(cols).some(Boolean))) && (
                    <span className="text-xs text-indigo-800 bg-white/70 border border-indigo-200 px-2 py-1 rounded">
                      Select tables to see scoped storage details
                    </span>
                  )}
                </div>
                {selectedStorageTablesFromAnalysis.length === 0 ? (
                  <div className="text-sm text-indigo-900 bg-white/60 border border-indigo-200 rounded px-3 py-2">
                    No storage information available for the current selection.
                  </div>
                ) : (
                  <>
                    <div className="grid grid-cols-3 gap-3">
                      <div className="bg-white p-3 rounded border border-indigo-200 shadow-sm hover:shadow-md transition-all">
                        <div className="text-gray-500 text-sm">Total Size</div>
                        <div className="font-bold text-indigo-600 text-lg">
                          {calculateSelectedObjectsSize('total', true)}
                        </div>
                      </div>
                      <div className="bg-white p-3 rounded border border-green-200 shadow-sm hover:shadow-md transition-all">
                        <div className="text-gray-500 text-sm">Data Size</div>
                        <div className="font-bold text-green-600 text-lg">
                          {calculateSelectedObjectsSize('data', true)}
                        </div>
                      </div>
                      <div className="bg-white p-3 rounded border border-purple-200 shadow-sm hover:shadow-md transition-all">
                        <div className="text-gray-500 text-sm">Index Size</div>
                        <div className="font-bold text-purple-600 text-lg">
                          {calculateSelectedObjectsSize('index', true)}
                        </div>
                      </div>
                    </div>

                    <div className="mt-3 border border-indigo-200 bg-white/80 rounded-lg overflow-hidden">
                      <div className="px-3 py-2 text-[11px] font-semibold uppercase tracking-wide text-indigo-900 bg-indigo-50 border-b border-indigo-200 flex items-center justify-between">
                        <span>Selected Object Storage Details</span>
                        <span className="text-xs font-mono text-indigo-700">
                          {selectedStorageTablesFromAnalysis.length} item{selectedStorageTablesFromAnalysis.length === 1 ? '' : 's'}
                        </span>
                      </div>
                      <div className="max-h-64 overflow-auto">
                        <table className="w-full text-xs">
                          <thead className="bg-indigo-50 sticky top-0 z-10">
                            <tr>
                              <th className="p-2 text-left font-semibold text-indigo-900">Schema</th>
                              <th className="p-2 text-left font-semibold text-indigo-900">Object</th>
                              <th className="p-2 text-right font-semibold text-indigo-900">Data</th>
                              <th className="p-2 text-right font-semibold text-indigo-900">Index</th>
                              <th className="p-2 text-right font-semibold text-indigo-900">Total</th>
                            </tr>
                          </thead>
                          <tbody>
                            {selectedStorageTablesFromAnalysis.map((table, idx) => {
                              const schema = (table.schema || table.table_schema || 'default').toLowerCase()
                              const name = (table.name || table.table || table.table_name || '').toLowerCase()
                              const dataSizeValue = toStorageNumber(table.data_size ?? table.data_length ?? 0)
                              const indexSizeValue = toStorageNumber(table.index_size ?? table.index_length ?? 0)
                              const totalSizeValue = toStorageNumber(
                                table.total_size ?? dataSizeValue + indexSizeValue
                              )
                              const dataSize = formatBytes(dataSizeValue)
                              const indexSize = formatBytes(indexSizeValue)
                              const totalSize = formatBytes(totalSizeValue)
                              return (
                                <tr key={`${schema}-${name}-${idx}`} className="border-b border-indigo-100 last:border-b-0">
                                  <td className="p-2 font-mono text-indigo-900">{schema}</td>
                                  <td className="p-2 font-mono font-semibold text-indigo-900">{name}</td>
                                  <td className="p-2 text-right text-indigo-800">{dataSize}</td>
                                  <td className="p-2 text-right text-indigo-800">{indexSize}</td>
                                  <td className="p-2 text-right font-bold text-indigo-900">{totalSize}</td>
                                </tr>
                              )
                            })}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  </>
                )}
              </div>
            )}
            
            <div>
              <div className="text-sm text-gray-600 mb-1">Schemas</div>
              <div className="flex flex-wrap gap-2">
                {results.database_info?.schemas?.map((schema: string, i: number) => (
                  <span key={i} className="px-3 py-1 bg-gray-100 text-gray-700 rounded-full text-sm font-medium">
                    {schema}
                  </span>
                ))}
              </div>
            </div>
          </div>

          <div className="grid grid-cols-4 gap-4 mb-6">
            <MetricCard icon={Table} label="Tables" value={getFilteredAnalysisResults?.tables?.length || 0} color="#085690" />
            <MetricCard icon={Eye} label="Views" value={(getFilteredAnalysisResults?.views?.length || 0) + (getFilteredAnalysisResults?.materialized_views?.length || 0)} color="#ec6225" />
            <MetricCard icon={Zap} label="Triggers" value={getFilteredAnalysisResults?.triggers?.length || 0} color="#085690" />
            <MetricCard icon={Hash} label="Sequences" value={getFilteredAnalysisResults?.sequences?.length || 0} color="#ec6225" />
          </div>

          <div className="grid grid-cols-4 gap-4 mb-6">
            <MetricCard icon={Database} label="Indexes" value={getFilteredAnalysisResults?.indexes?.length || 0} color="#085690" />
            <MetricCard icon={Lock} label="Constraints" value={getFilteredAnalysisResults?.constraints?.length || 0} color="#ec6225" />
            <MetricCard icon={Box} label="User Types" value={getFilteredAnalysisResults?.user_types?.length || 0} color="#085690" />
            <MetricCard icon={Code} label="Procedures" value={getFilteredAnalysisResults?.procedures?.length || 0} color="#ec6225" />
          </div>

          <DataLineageLive
            enabled={Boolean(sourceId && targetId)}
            sourceDetails={sourceDetails}
            targetDetails={targetDetails}
            selectedTables={selectedTables}
            lastUpdated={lastUpdated}
          />

          <div className="bg-white rounded-lg shadow">
            <div className="border-b border-gray-200">
              <nav className="flex">
                {['overview', 'tables', 'views', 'triggers', 'sequences', 'indexes', 'constraints', 'types', 'permissions'].map((tab) => (
                  <button
                    key={tab}
                    onClick={() => setActiveTab(tab)}
                    className={`px-6 py-3 font-medium text-sm transition-all ${
                      activeTab === tab
                        ? 'border-b-2 border-[#085690] text-[#085690]'
                        : 'text-gray-500 hover:text-gray-700'
                    }`}
                  >
                    {tab.charAt(0).toUpperCase() + tab.slice(1)}
                  </button>
                ))}
              </nav>
            </div>

            <div className="p-6">
              {activeTab === 'overview' && (
                <div className="space-y-4">
                  <h3 className="text-lg font-semibold text-[#085690] mb-3">Analysis Summary</h3>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="p-4 bg-gray-50 rounded-lg">
                      <div className="text-sm text-gray-600 mb-2">Total Objects</div>
                      <div className="text-2xl font-bold text-[#085690]">
                        {(getFilteredAnalysisResults?.tables?.length || 0) + 
                         (getFilteredAnalysisResults?.views?.length || 0) + 
                         (getFilteredAnalysisResults?.materialized_views?.length || 0) + 
                         (getFilteredAnalysisResults?.triggers?.length || 0) + 
                         (getFilteredAnalysisResults?.sequences?.length || 0) + 
                         (getFilteredAnalysisResults?.indexes?.length || 0) + 
                         (getFilteredAnalysisResults?.procedures?.length || 0)}
                      </div>
                    </div>
                    <div className="p-4 bg-gray-50 rounded-lg">
                      <div className="text-sm text-gray-600 mb-2">Total Columns</div>
                      <div className="text-2xl font-bold text-[#ec6225]">{getFilteredAnalysisResults?.columns?.length || 0}</div>
                    </div>
                  </div>
                  {results.driver_unavailable && (
                    <div className="p-4 bg-gray-100 border-l-4 border-[#ec6225] text-gray-700">
                      <strong>Note:</strong> This is simulated data. Connect to a real database for actual analysis.
                    </div>
                  )}
                </div>
              )}

              {activeTab === 'tables' && (
                <div>
                  <h3 className="text-lg font-semibold text-[#085690] mb-3">Tables ({getFilteredAnalysisResults?.tables?.length || 0})</h3>
                  <div className="overflow-auto max-h-96">
                    <table className="w-full">
                      <thead className="bg-gray-100 sticky top-0">
                        <tr>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Schema</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Table Name</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Type</th>
                          <th className="text-right p-3 text-sm font-semibold text-gray-700">Row Count</th>
                        </tr>
                      </thead>
                      <tbody>
                        {getFilteredAnalysisResults?.tables?.map((table: any, i: number) => {
                          const profile = getFilteredAnalysisResults.data_profiles?.find((p: any) => p.table === table.name)
                          return (
                            <tr key={i} className="border-b border-gray-200 hover:bg-gray-50">
                              <td className="p-3 text-sm">{table.schema}</td>
                              <td className="p-3 text-sm font-medium text-[#085690]">{table.name}</td>
                              <td className="p-3 text-sm">{table.type}</td>
                              <td className="p-3 text-sm text-right">{profile?.row_count?.toLocaleString() || '-'}</td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {activeTab === 'views' && (
                <div>
                  <h3 className="text-lg font-semibold text-[#085690] mb-3">
                    Views ({(getFilteredAnalysisResults?.views?.length || 0) + (getFilteredAnalysisResults?.materialized_views?.length || 0)})
                  </h3>
                  <div className="space-y-4">
                    {getFilteredAnalysisResults?.views?.length > 0 && (
                      <div>
                        <h4 className="font-medium text-gray-700 mb-2">Regular Views</h4>
                        <div className="overflow-auto max-h-64">
                          <table className="w-full">
                            <thead className="bg-gray-100">
                              <tr>
                                <th className="text-left p-3 text-sm font-semibold text-gray-700">Schema</th>
                                <th className="text-left p-3 text-sm font-semibold text-gray-700">View Name</th>
                              </tr>
                            </thead>
                            <tbody>
                              {getFilteredAnalysisResults.views.map((view: any, i: number) => (
                                <tr key={i} className="border-b border-gray-200 hover:bg-gray-50">
                                  <td className="p-3 text-sm">{view.schema}</td>
                                  <td className="p-3 text-sm font-medium text-[#085690]">{view.name}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                    {getFilteredAnalysisResults?.materialized_views?.length > 0 && (
                      <div>
                        <h4 className="font-medium text-gray-700 mb-2">Materialized Views</h4>
                        <div className="overflow-auto max-h-64">
                          <table className="w-full">
                            <thead className="bg-gray-100">
                              <tr>
                                <th className="text-left p-3 text-sm font-semibold text-gray-700">Schema</th>
                                <th className="text-left p-3 text-sm font-semibold text-gray-700">View Name</th>
                              </tr>
                            </thead>
                            <tbody>
                              {getFilteredAnalysisResults.materialized_views.map((view: any, i: number) => (
                                <tr key={i} className="border-b border-gray-200 hover:bg-gray-50">
                                  <td className="p-3 text-sm">{view.schema}</td>
                                  <td className="p-3 text-sm font-medium text-[#ec6225]">{view.name}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              )}

              {activeTab === 'triggers' && (
                <div>
                  <h3 className="text-lg font-semibold text-[#085690] mb-3">Triggers ({getFilteredAnalysisResults?.triggers?.length || 0})</h3>
                  <div className="overflow-auto max-h-96">
                    <table className="w-full">
                      <thead className="bg-gray-100 sticky top-0">
                        <tr>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Schema</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Trigger Name</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Table</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Timing</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Event</th>
                        </tr>
                      </thead>
                      <tbody>
                        {getFilteredAnalysisResults?.triggers?.map((trigger: any, i: number) => (
                          <tr key={i} className="border-b border-gray-200 hover:bg-gray-50">
                            <td className="p-3 text-sm">{trigger.schema}</td>
                            <td className="p-3 text-sm font-medium text-[#085690]">{trigger.name}</td>
                            <td className="p-3 text-sm">{trigger.table}</td>
                            <td className="p-3 text-sm">
                              <span className="px-2 py-1 bg-gray-100 rounded text-xs">{trigger.timing}</span>
                            </td>
                            <td className="p-3 text-sm">
                              <span className="px-2 py-1 bg-[#ec6225] bg-opacity-10 text-[#ec6225] rounded text-xs">{trigger.event}</span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {activeTab === 'sequences' && (
                <div>
                  <h3 className="text-lg font-semibold text-[#085690] mb-3">Sequences ({getFilteredAnalysisResults?.sequences?.length || 0})</h3>
                  <div className="overflow-auto max-h-96">
                    <table className="w-full">
                      <thead className="bg-gray-100 sticky top-0">
                        <tr>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Schema</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Sequence Name</th>
                          <th className="text-right p-3 text-sm font-semibold text-gray-700">Current Value</th>
                          <th className="text-right p-3 text-sm font-semibold text-gray-700">Increment</th>
                        </tr>
                      </thead>
                      <tbody>
                        {getFilteredAnalysisResults?.sequences?.map((seq: any, i: number) => (
                          <tr key={i} className="border-b border-gray-200 hover:bg-gray-50">
                            <td className="p-3 text-sm">{seq.schema}</td>
                            <td className="p-3 text-sm font-medium text-[#085690]">{seq.name}</td>
                            <td className="p-3 text-sm text-right font-mono">{seq.current_value?.toLocaleString()}</td>
                            <td className="p-3 text-sm text-right font-mono">{seq.increment}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {activeTab === 'indexes' && (
                <div>
                  <h3 className="text-lg font-semibold text-[#085690] mb-3">Indexes ({getFilteredAnalysisResults?.indexes?.length || 0})</h3>
                  <div className="overflow-auto max-h-96">
                    <table className="w-full">
                      <thead className="bg-gray-100 sticky top-0">
                        <tr>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Schema</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Table</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Index Name</th>
                        </tr>
                      </thead>
                      <tbody>
                        {getFilteredAnalysisResults?.indexes?.map((index: any, i: number) => (
                          <tr key={i} className="border-b border-gray-200 hover:bg-gray-50">
                            <td className="p-3 text-sm">{index.schema}</td>
                            <td className="p-3 text-sm">{index.table}</td>
                            <td className="p-3 text-sm font-medium text-[#085690]">{index.name}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {activeTab === 'constraints' && (
                <div>
                  <h3 className="text-lg font-semibold text-[#085690] mb-3">Constraints ({getFilteredAnalysisResults?.constraints?.length || 0})</h3>
                  <div className="overflow-auto max-h-96">
                    <table className="w-full">
                      <thead className="bg-gray-100 sticky top-0">
                        <tr>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Schema</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Table</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Constraint Name</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Type</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">On Delete</th>
                        </tr>
                      </thead>
                      <tbody>
                        {getFilteredAnalysisResults?.constraints?.map((constraint: any, i: number) => (
                          <tr key={i} className="border-b border-gray-200 hover:bg-gray-50">
                            <td className="p-3 text-sm">{constraint.schema}</td>
                            <td className="p-3 text-sm">{constraint.table}</td>
                            <td className="p-3 text-sm font-medium text-[#085690]">{constraint.name}</td>
                            <td className="p-3 text-sm">
                              <span className="px-2 py-1 bg-gray-100 rounded text-xs">{constraint.type}</span>
                            </td>
                            <td className="p-3 text-sm">{constraint.on_delete || '-'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {activeTab === 'types' && (
                <div>
                  <h3 className="text-lg font-semibold text-[#085690] mb-3">User-Defined Types ({getFilteredAnalysisResults?.user_types?.length || 0})</h3>
                  <div className="overflow-auto max-h-96">
                    <table className="w-full">
                      <thead className="bg-gray-100 sticky top-0">
                        <tr>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Schema</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Type Name</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Category</th>
                        </tr>
                      </thead>
                      <tbody>
                        {getFilteredAnalysisResults?.user_types?.map((type: any, i: number) => (
                          <tr key={i} className="border-b border-gray-200 hover:bg-gray-50">
                            <td className="p-3 text-sm">{type.schema}</td>
                            <td className="p-3 text-sm font-medium text-[#085690]">{type.name}</td>
                            <td className="p-3 text-sm">
                              <span className="px-2 py-1 bg-[#ec6225] bg-opacity-10 text-[#ec6225] rounded text-xs capitalize">
                                {type.category}
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {activeTab === 'permissions' && (
                <div>
                  <h3 className="text-lg font-semibold text-[#085690] mb-3">Permissions ({getFilteredAnalysisResults?.permissions?.length || 0})</h3>
                  <div className="overflow-auto max-h-96">
                    <table className="w-full">
                      <thead className="bg-gray-100 sticky top-0">
                        <tr>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Grantee</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Schema</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Object</th>
                          <th className="text-left p-3 text-sm font-semibold text-gray-700">Privilege</th>
                        </tr>
                      </thead>
                      <tbody>
                        {getFilteredAnalysisResults?.permissions?.map((perm: any, i: number) => (
                          <tr key={i} className="border-b border-gray-200 hover:bg-gray-50">
                            <td className="p-3 text-sm font-medium text-[#085690]">{perm.grantee}</td>
                            <td className="p-3 text-sm">{perm.schema}</td>
                            <td className="p-3 text-sm">{perm.object}</td>
                            <td className="p-3 text-sm">
                              <span className="px-2 py-1 bg-gray-100 rounded text-xs">{perm.privilege}</span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          </div>

          <div className="mt-6 flex justify-end">
            <button
              onClick={() => navigate('/extract')}
              disabled={invalidSelectedTables.length > 0}
              className="flex items-center gap-2 px-8 py-3 bg-gradient-to-r from-[#ec6225] to-[#ff7a3d] text-white rounded-lg hover:shadow-lg transition-all disabled:opacity-50 disabled:cursor-not-allowed font-medium text-lg"
            >
              Proceed to Extract
              <ArrowRight size={20} />
            </button>
          </div>
        </>
      )}
      
      {/* Rename Column Dialog */}
      {renameDialog.open && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 w-full max-w-md mx-4">
            <h3 className="text-lg font-semibold text-[#085690] mb-4">Rename Column</h3>
            <p className="text-sm text-gray-600 mb-2">
              Rename <strong>{renameDialog.columnName}</strong> in <strong>{renameDialog.tableName}</strong>:
            </p>
            <input
              type="text"
              value={renameDialog.newColumnName}
              onChange={(e) => {
                const value = e.target.value;
                setRenameDialog(prev => ({
                  ...prev,
                  newColumnName: value,
                  error: ''
                }));
              }}
              placeholder="Enter new column name"
              className="w-full px-3 py-2 border border-gray-300 rounded-lg mb-2"
              autoFocus
            />
            {renameDialog.error && (
              <p className="text-red-500 text-sm mb-3">{renameDialog.error}</p>
            )}
            <div className="flex justify-end gap-3">
              <button
                onClick={() => setRenameDialog({
                  open: false,
                  tableName: '',
                  columnName: '',
                  newColumnName: '',
                  error: ''
                })}
                className="px-4 py-2 text-gray-600 hover:bg-gray-100 rounded-lg"
              >
                Cancel
              </button>
              <button
                onClick={handleRenameColumn}
                className="px-4 py-2 bg-[#085690] text-white rounded-lg hover:bg-[#064475]"
              >
                Rename
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

