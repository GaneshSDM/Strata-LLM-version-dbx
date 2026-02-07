const SESSION_KEY = 'strata-session-id'

export const getSessionId = () => {
  if (typeof window === 'undefined') {
    return null
  }
  return sessionStorage.getItem(SESSION_KEY)
}

export const setSessionId = (sessionId: string) => {
  if (typeof window === 'undefined') {
    return
  }
  sessionStorage.setItem(SESSION_KEY, sessionId)
}

export const getSessionHeaders = (): Record<string, string> => {
  const sessionId = getSessionId()
  return sessionId ? { 'X-Session-Id': sessionId } : {'X-Session-Id': ''}
}

export const ensureSessionId = async () => {
  if (typeof window === 'undefined') {
    return null
  }
  const existing = getSessionId()
  if (existing) return existing
  try {
    const res = await fetch('/api/session/start', { method: 'POST' })
    const data = await res.json()
    if (res.ok && data?.session_id) {
      setSessionId(data.session_id)
      return data.session_id as string
    }
  } catch (err) {
    console.error('Failed to initialize session id', err)
  }
  return null
}
