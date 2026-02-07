import { createContext, useContext } from 'react'

export type LineageHoverContextValue = {
  hoveredNodeId: string | null
  adjacentNodeIdsByNodeId: Map<string, Set<string>>
}

const LineageHoverContext = createContext<LineageHoverContextValue | null>(null)

export const LineageHoverProvider = LineageHoverContext.Provider

export const useLineageHover = () => {
  const value = useContext(LineageHoverContext)
  if (!value) {
    return { hoveredNodeId: null, adjacentNodeIdsByNodeId: new Map<string, Set<string>>() }
  }
  return value
}

