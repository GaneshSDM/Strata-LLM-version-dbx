import { createContext, useContext, type ReactNode } from 'react'

export type WizardStepPath = '/' | '/extract'

export type AnalyzeMetrics = {
  tables: number
  views: number
  materialized_views: number
  triggers: number
  sequences: number
  indexes: number
  constraints: number
  user_types: number
  procedures: number
} | null

export type WizardContextValue = {
  lastWizardPath: WizardStepPath
  setLastWizardPath: (path: WizardStepPath) => void
  wizardResetId: number
  resetWizardState: (message?: string) => void
  notify: (message: string) => void
  analyzeMetrics: AnalyzeMetrics
  setAnalyzeMetrics: (metrics: AnalyzeMetrics) => void
}

const WizardContext = createContext<WizardContextValue | null>(null)

export function WizardProvider({
  value,
  children
}: {
  value: WizardContextValue
  children: ReactNode
}) {
  return <WizardContext.Provider value={value}>{children}</WizardContext.Provider>
}

export function useWizard() {
  const ctx = useContext(WizardContext)
  if (!ctx) {
    throw new Error('useWizard must be used within WizardProvider')
  }
  return ctx
}
