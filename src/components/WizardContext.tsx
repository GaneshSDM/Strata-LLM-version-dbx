import { createContext, useContext, type ReactNode } from 'react'

export type WizardStepPath = '/' | '/extract' | '/migrate' | '/reconcile'

export type WizardContextValue = {
  lastWizardPath: WizardStepPath
  setLastWizardPath: (path: WizardStepPath) => void
  wizardResetId: number
  resetWizardState: (message?: string) => void
  notify: (message: string) => void
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
