import type { DatabaseType, LineageNodeData, LoadType, LineageStatus } from './types'

export const normalizeDbType = (value?: string): DatabaseType => {
  const v = String(value || '')
    .trim()
    .toLowerCase()
  if (v.includes('postgres')) return 'postgresql'
  if (v.includes('snowflake')) return 'snowflake'
  if (v.includes('mysql')) return 'mysql'
  if (v.includes('oracle')) return 'oracle'
  if (v.replace(/[\s_-]/g, '').includes('sqlserver')) return 'sqlserver'
  if (v.replace(/[\s_-]/g, '').includes('mssql')) return 'sqlserver'
  return 'unknown'
}

export const normalizeIdent = (value: string) =>
  String(value || '')
    .trim()
    .replace(/^\"|\"$/g, '')
    .toLowerCase()

export const splitTableFullName = (fullName: string) => {
  const value = String(fullName || '').trim()
  const dotIndex = value.indexOf('.')
  if (dotIndex === -1) return { schema: 'public', name: value }
  return { schema: value.slice(0, dotIndex), name: value.slice(dotIndex + 1) }
}

export const makeTableKey = (schema: string, name: string) => `${normalizeIdent(schema)}.${normalizeIdent(name)}`

export const formatCompactNumber = (value: number) => {
  if (!Number.isFinite(value)) return '0'
  return Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 }).format(value)
}

export const toIsoTimestamp = (value?: string | number | Date | null) => {
  if (!value) return new Date().toISOString()
  const d = value instanceof Date ? value : new Date(value)
  return Number.isNaN(d.getTime()) ? new Date().toISOString() : d.toISOString()
}

export const toStatusLabel = (status: LineageStatus) => {
  switch (status) {
    case 'SUCCESS':
      return 'Success'
    case 'FAILED':
      return 'Failed'
    case 'PENDING':
      return 'Pending'
    default:
      return status
  }
}

export const statusPillClass = (status: LineageNodeData['status']) => {
  switch (status) {
    case 'SUCCESS':
      return 'bg-emerald-50 text-emerald-700 border-emerald-200'
    case 'FAILED':
      return 'bg-rose-50 text-rose-700 border-rose-200'
    case 'PENDING':
      return 'bg-amber-50 text-amber-800 border-amber-200'
    default:
      return 'bg-gray-50 text-gray-700 border-gray-200'
  }
}

export const loadTypePillClass = (loadType: LoadType) => {
  switch (loadType) {
    case 'FULL':
      return 'bg-slate-50 text-slate-700 border-slate-200'
    case 'INCREMENTAL':
      return 'bg-indigo-50 text-indigo-700 border-indigo-200'
    default:
      return 'bg-slate-50 text-slate-700 border-slate-200'
  }
}

