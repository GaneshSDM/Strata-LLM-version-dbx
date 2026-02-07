export type StageProgress = {
  analysis: boolean
  extraction: boolean
  migration: boolean
}

export type StageKey = keyof StageProgress
