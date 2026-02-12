import React, { useState, useEffect, useCallback } from 'react'
import { BrowserRouter, useLocation, useNavigate } from 'react-router-dom'
import Layout from './components/Layout'
import Login from './pages/Login'
import Analyze from './pages/Analyze'
import Extract from './pages/Extract'
import ViewLogs from './pages/ViewLogs'
import DataTypes from './pages/DataTypes'
import { LogsModal } from './components/LogsModal'
import { ConnectionModal } from './components/ConnectionModal'
import { Toaster } from './components/ui/toast'
import LockedStage from './components/LockedStage'
import { StageKey, StageProgress } from './types/workflow'
import { WizardProvider, WizardStepPath, AnalyzeMetrics } from './components/WizardContext'
import { ensureSessionId as ensureLogSessionId } from './utils/session'

export type Connection = {
  id: number
  name: string
  db_type: string
  created_at: string
}

const STAGE_STORAGE_KEYS: Record<StageKey, string> = {
  analysis: 'strata-stage-analysis',
  extraction: 'strata-stage-extraction',
  migration: 'strata-stage-migration'
}

const getStoredStageProgress = (): StageProgress => {
  if (typeof window === 'undefined') {
    return { analysis: false, extraction: false, migration: false }
  }

  return {
    analysis: localStorage.getItem(STAGE_STORAGE_KEYS.analysis) === 'true',
    extraction: localStorage.getItem(STAGE_STORAGE_KEYS.extraction) === 'true',
    migration: localStorage.getItem(STAGE_STORAGE_KEYS.migration) === 'true'
  }
}

const WIZARD_PATHS: WizardStepPath[] = ['/', '/extract']

const isWizardPath = (path: string): path is WizardStepPath => {
  return (WIZARD_PATHS as string[]).includes(path)
}

const isKnownPath = (path: string) => {
  return isWizardPath(path) || path === '/logs' || path === '/datatypes'
}

// Error Boundary Component
class ErrorBoundary extends React.Component<{
  children: React.ReactNode;
  onError: (error: Error) => void;
}, { hasError: boolean; message?: string; stack?: string }> {
  constructor(props: { children: React.ReactNode; onError: (error: Error) => void }) {
    super(props);
    this.state = { hasError: false, message: undefined, stack: undefined };
  }

  static getDerivedStateFromError(_error: Error) {
    return { hasError: true };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error('Error caught by boundary:', error, errorInfo);
    this.setState({ message: error.message, stack: errorInfo.componentStack || error.stack });
    this.props.onError(error);
  }

  render() {
    if (this.state.hasError) {
      const { message, stack } = this.state;
      return (
        <div className="min-h-screen flex items-center justify-center bg-red-50 p-4">
          <div className="bg-white rounded-lg shadow-lg p-6 max-w-md w-full text-center">
            <h2 className="text-xl font-bold text-red-600 mb-2">Something went wrong</h2>
            <p className="text-gray-600 mb-4">
              {message ? `Error: ${message}` : 'An error occurred while loading the application.'}
            </p>
            {stack && (
              <details className="text-left bg-red-50 border border-red-100 rounded-md p-3 text-xs text-red-800 mb-3 max-h-40 overflow-auto">
                <summary className="cursor-pointer font-semibold mb-1">Details</summary>
                <pre className="whitespace-pre-wrap break-words text-xs">{stack}</pre>
              </details>
            )}
            <button 
              onClick={() => window.location.reload()} 
              className="bg-red-500 text-white px-4 py-2 rounded hover:bg-red-600 transition-colors"
            >
              Reload Page
            </button>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

function AuthenticatedContent({
  connections,
  stageProgress,
  resetWorkflowStages,
  markStageComplete,
  setLastWizardPath
}: {
  connections: Connection[]
  stageProgress: StageProgress
  resetWorkflowStages: () => void
  markStageComplete: (stage: StageKey) => void
  setLastWizardPath: (path: WizardStepPath) => void
}) {
  const location = useLocation()
  const navigate = useNavigate()
  const pathname = location.pathname

  useEffect(() => {
    if (isWizardPath(pathname)) {
      setLastWizardPath(pathname)
    }
  }, [pathname, setLastWizardPath])

  useEffect(() => {
    if (!isKnownPath(pathname)) {
      navigate('/', { replace: true })
    }
  }, [pathname, navigate])

  return (
    <>
      <div hidden={pathname !== '/'}>
        <Analyze
          connections={connections}
          onAnalysisRestart={resetWorkflowStages}
          onAnalysisComplete={() => markStageComplete('analysis')}
        />
      </div>

      <div hidden={pathname !== '/extract'}>
        {stageProgress.analysis ? (
          <Extract onExtractionComplete={() => markStageComplete('extraction')} />
        ) : (
          <LockedStage
            title="Extraction Locked"
            message="Complete the analysis step before running extraction."
            actionLabel="Go to Analysis"
            actionPath="/"
          />
        )}
      </div>
      {pathname === '/logs' && <ViewLogs />}
      <div hidden={pathname !== '/datatypes'}>
        <DataTypes />
      </div>
    </>
  )
}

function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false)
  const [showConnectionModal, setShowConnectionModal] = useState(false)
  const [showLogsModal, setShowLogsModal] = useState(false)
  const [connections, setConnections] = useState<Connection[]>([])
  const [stageProgress, setStageProgress] = useState<StageProgress>(() => getStoredStageProgress())
  const [notification, setNotification] = useState<string | null>(null)
  const [lastWizardPath, setLastWizardPath] = useState<WizardStepPath>('/')
  const [wizardResetId, setWizardResetId] = useState(0)
  const [analyzeMetrics, setAnalyzeMetrics] = useState<AnalyzeMetrics>(null)
  const [runtimeError, setRuntimeError] = useState<string | null>(null)

  useEffect(() => {
    const handleError = (event: ErrorEvent) => {
      setRuntimeError(event.error?.message || event.message || 'Unknown runtime error')
    }
    const handleRejection = (event: PromiseRejectionEvent) => {
      const reason = (event.reason && event.reason.message) || String(event.reason)
      setRuntimeError(reason || 'Unhandled promise rejection')
    }

    window.addEventListener('error', handleError)
    window.addEventListener('unhandledrejection', handleRejection)
    return () => {
      window.removeEventListener('error', handleError)
      window.removeEventListener('unhandledrejection', handleRejection)
    }
  }, [])

  const persistStageValue = useCallback((stage: StageKey, value: boolean) => {
    if (typeof window === 'undefined') return
    if (value) {
      localStorage.setItem(STAGE_STORAGE_KEYS[stage], 'true')
    } else {
      localStorage.removeItem(STAGE_STORAGE_KEYS[stage])
    }
  }, [])

  const setStageValue = useCallback((stage: StageKey, value: boolean) => {
    setStageProgress(prev => {
      if (prev[stage] === value) {
        return prev
      }
      const updated = { ...prev, [stage]: value }
      persistStageValue(stage, value)
      return updated
    })
  }, [persistStageValue])

  const resetWorkflowStages = useCallback(() => {
    const reset: StageProgress = { analysis: false, extraction: false, migration: false }
    setStageProgress(reset)
    if (typeof window !== 'undefined') {
      Object.values(STAGE_STORAGE_KEYS).forEach(key => localStorage.removeItem(key))
    }
  }, [])

  const loadConnections = async () => {
    try {
      console.log('Loading connections...');
      const res = await fetch('/api/connections');
      console.log('Response status:', res.status);
      
      if (!res.ok) {
        throw new Error(`HTTP error! status: ${res.status}`);
      }
      
      const data = await res.json();
      console.log('Connections loaded:', data);
      
      if (data.ok) {
        setConnections(data.data);
        console.log('Connections state updated:', data.data);
      } else {
        console.error('API returned error:', data.message);
      }
    } catch (err) {
      console.error('Failed to load connections:', err);
      // Set empty connections array to prevent app crash
      setConnections([]);
    }
  }

  const handleLogin = () => {
    resetWorkflowStages()
    setIsAuthenticated(true)
    window.history.pushState({}, '', '/')
  }

  const markStageComplete = useCallback((stage: StageKey) => {
    setStageValue(stage, true)
  }, [setStageValue])

  const handleBlockedNavigation = useCallback((message: string) => {
    setNotification(message)
  }, [])

  const notify = useCallback((message: string) => {
    setNotification(message)
  }, [])

  const resetWizardState = useCallback((message?: string) => {
    resetWorkflowStages()
    setLastWizardPath('/')
    setWizardResetId(prev => prev + 1)
    setAnalyzeMetrics(null)
    if (message) {
      setNotification(message)
    }
  }, [resetWorkflowStages])

  useEffect(() => {
    if (isAuthenticated) {
      loadConnections()
    }
  }, [isAuthenticated])

  useEffect(() => {
    if (!isAuthenticated) return
    ensureLogSessionId().catch(err => {
      console.error('Failed to initialize log session', err)
    })
  }, [isAuthenticated])

  if (!isAuthenticated) {
    return <Login onLogin={handleLogin} />
  }

  return (
    <ErrorBoundary onError={(err) => setRuntimeError(err.message)}>
      <BrowserRouter>
        <WizardProvider value={{ lastWizardPath, setLastWizardPath, wizardResetId, resetWizardState, notify, analyzeMetrics, setAnalyzeMetrics }}>
          <Layout
            onOpenSettings={() => setShowConnectionModal(true)}
            onLogout={() => {
              resetWorkflowStages()
              setShowLogsModal(false)
              setIsAuthenticated(false)
              window.history.pushState({}, '', '/')
            }}
            stageProgress={stageProgress}
            onBlockedNavigation={handleBlockedNavigation}
            notification={runtimeError || notification}
            onDismissNotification={() => {
              setNotification(null)
              setRuntimeError(null)
            }}
            modal={<LogsModal isOpen={showLogsModal} onClose={() => setShowLogsModal(false)} />}
          >
            <AuthenticatedContent
              connections={connections}
              stageProgress={stageProgress}
              resetWorkflowStages={resetWorkflowStages}
              markStageComplete={markStageComplete}
              setLastWizardPath={setLastWizardPath}
            />
          </Layout>
        </WizardProvider>
        
        {showConnectionModal && (
          <ConnectionModal
            onClose={() => setShowConnectionModal(false)}
            onSaved={() => {
              loadConnections()
            }}
          />
        )}
        
        <Toaster />
      </BrowserRouter>
    </ErrorBoundary>
  )
}

export default App
