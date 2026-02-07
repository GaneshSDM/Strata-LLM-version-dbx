export type DatabaseType = 'postgresql' | 'snowflake' | 'mysql' | 'oracle' | 'sqlserver' | 'databricks' | 'unknown'

export type LoadType = 'FULL' | 'INCREMENTAL'

export type LineageStatus = 'SUCCESS' | 'FAILED' | 'PENDING'

export type LineageNodeData = {
  databaseType: DatabaseType
  database: string
  schema: string
  table: string
  rowCount: number
  columnCount: number
  lastUpdated: string
  loadType: LoadType
  status: LineageStatus
}

export type LineageEdgeData = {
  mappingType: 'Table → Table'
  loadMode: LoadType
  lastExecutionTime: string
}

