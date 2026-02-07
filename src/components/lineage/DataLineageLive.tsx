import { Fragment, useCallback, useEffect, useMemo, useState, useTransition } from 'react'
import { MarkerType, type Edge, type Node } from 'reactflow'
import LineageFlow from '../LineageFlow'
import type { LineageEdgeData, LineageNodeData } from './types'
import { layoutLineageGraph } from './layout'
import { makeTableKey, normalizeDbType, normalizeIdent, splitTableFullName, toIsoTimestamp } from './utils'
import TableDetailsDrawer from './TableDetailsDrawer'

// Define the type for column renames
interface ColumnRenameMap {
  [tableName: string]: {
    [oldColumnName: string]: string;
  };
}

type DetailsTable = { schema?: string; name?: string; row_count?: number }
type DetailsColumn = { schema?: string; table?: string }
type DbDetails = {
  connection?: { name?: string; db_type?: string }
  location?: { database?: string; schema?: string }
  tables?: DetailsTable[]
  columns?: DetailsColumn[]
}

type Summary = { sources: number; targets: number; links: number }

type Props = {
  sourceDetails: DbDetails | null
  targetDetails: DbDetails | null
  selectedTables: Record<string, boolean>
  enabled?: boolean
  lastUpdated?: string | number | Date | null
}

type ExtendedLineageNodeData = LineageNodeData & {
  renameCount?: number;
};

type GraphState = {
  status: 'idle' | 'loading' | 'ready' | 'empty' | 'error'
  nodes: Array<Node<ExtendedLineageNodeData>>
  edges: Array<Edge<LineageEdgeData>>
  summary: Summary
  errorMessage?: string
}

const EmptyState = ({ title, message }: { title: string; message: string }) => (
  <div className="h-full w-full flex items-center justify-center p-8">
    <div className="max-w-md text-center">
      <div className="text-sm font-semibold text-gray-800">{title}</div>
      <div className="text-xs text-gray-500 mt-2">{message}</div>
    </div>
  </div>
)

const Skeleton = () => (
  <div className="h-full w-full p-6">
    <div className="grid grid-cols-2 gap-10 h-full items-center">
      {[0, 1].map((col) => (
        <div key={col} className="space-y-4">
          {[0, 1, 2].map((row) => (
            <div key={row} className="rounded-xl border border-gray-200 bg-white shadow-sm p-4 animate-pulse">
              <div className="h-3 w-24 bg-gray-200 rounded mb-3" />
              <div className="h-4 w-56 bg-gray-200 rounded" />
            </div>
          ))}
        </div>
      ))}
    </div>
  </div>
)

const buildGraph = ({
  sourceDetails,
  targetDetails,
  sourceTables,
  lastUpdated
}: {
  sourceDetails: DbDetails
  targetDetails: DbDetails
  sourceTables: string[]
  lastUpdated: string
}): { nodes: Array<Node<LineageNodeData>>; edges: Array<Edge<LineageEdgeData>>; summary: Summary } => {
  const sourceDbType = normalizeDbType(sourceDetails.connection?.db_type)
  const targetDbType = normalizeDbType(targetDetails.connection?.db_type)

  const sourceDatabase =
    sourceDetails.location?.database || sourceDetails.connection?.name || sourceDetails.location?.schema || 'SOURCE_DB'
  const targetDatabase =
    targetDetails.location?.database || targetDetails.connection?.name || targetDetails.location?.schema || 'TARGET_DB'

  const targetTables = targetDetails.tables || []
  const targetDefaultSchema = targetDetails.location?.schema

  const targetByExact = new Map<string, Array<{ schema: string; name: string }>>()
  const targetByName = new Map<string, Array<{ schema: string; name: string }>>()

  for (const t of targetTables) {
    const schema = String(t.schema || 'public')
    const name = String(t.name || '').trim()
    if (!name) continue
    const exactKey = makeTableKey(schema, name)
    const nameKey = normalizeIdent(name)

    if (!targetByExact.has(exactKey)) targetByExact.set(exactKey, [])
    targetByExact.get(exactKey)!.push({ schema, name })

    if (!targetByName.has(nameKey)) targetByName.set(nameKey, [])
    targetByName.get(nameKey)!.push({ schema, name })
  }

  const getRowCount = (details: DbDetails, schema: string, table: string) => {
    const match = (details.tables || []).find(
      (t) => normalizeIdent(String(t.schema || '')) === normalizeIdent(schema) && normalizeIdent(String(t.name || '')) === normalizeIdent(table)
    )
    return Number(match?.row_count || 0) || 0
  }

  const getColumnCount = (details: DbDetails, schema: string, table: string) => {
    return (details.columns || []).filter(
      (c) => normalizeIdent(String(c.schema || '')) === normalizeIdent(schema) && normalizeIdent(String(c.table || '')) === normalizeIdent(table)
    ).length
  }

  const sourceNodes = new Map<string, Node<LineageNodeData>>()
  const targetNodes = new Map<string, Node<LineageNodeData>>()
  const edges: Array<Edge<LineageEdgeData>> = []
  const seenEdges = new Set<string>()

  for (const fullName of sourceTables) {
    const { schema, name } = splitTableFullName(fullName)
    const exactKey = makeTableKey(schema, name)
    let targets =
      (targetByExact.get(exactKey) && targetByExact.get(exactKey)!.length > 0 ? targetByExact.get(exactKey) : targetByName.get(normalizeIdent(name))) ||
      []

    if (!targets.length) {
      const virtualSchema = String(targetDefaultSchema || schema || 'public')
      targets = [{ schema: virtualSchema, name }]
    }

    const srcId = `src:${exactKey}`
    if (!sourceNodes.has(srcId)) {
      sourceNodes.set(srcId, {
        id: srcId,
        type: 'sourceNode',
        position: { x: 0, y: 0 },
        data: {
          databaseType: sourceDbType,
          database: sourceDatabase,
          schema,
          table: name,
          rowCount: getRowCount(sourceDetails, schema, name),
          columnCount: getColumnCount(sourceDetails, schema, name),
          lastUpdated,
          loadType: 'FULL',
          status: 'SUCCESS'
        },
        draggable: false,
        selectable: false
      })
    }

    for (const target of targets) {
      const targetKey = makeTableKey(target.schema, target.name)
      const tgtId = `tgt:${targetKey}`
      const targetExists = targetByExact.has(targetKey)

      if (!targetNodes.has(tgtId)) {
        targetNodes.set(tgtId, {
          id: tgtId,
          type: 'targetNode',
          position: { x: 0, y: 0 },
          data: {
            databaseType: targetDbType,
            database: targetDatabase,
            schema: target.schema,
            table: target.name,
            rowCount: getRowCount(targetDetails, target.schema, target.name),
            columnCount: getColumnCount(targetDetails, target.schema, target.name),
            lastUpdated,
            loadType: 'FULL',
            status: targetExists ? 'SUCCESS' : 'PENDING'
          },
          draggable: false,
          selectable: false
        })
      }

      const edgeId = `e:${srcId}->${tgtId}`
      if (seenEdges.has(edgeId)) continue
      seenEdges.add(edgeId)
      edges.push({
        id: edgeId,
        source: srcId,
        target: tgtId,
        type: 'lineageEdge',
        animated: true,
        markerEnd: { type: MarkerType.ArrowClosed },
        style: { stroke: '#085690', strokeWidth: 2, opacity: 0.55 },
        data: { mappingType: 'Table → Table', loadMode: 'FULL', lastExecutionTime: lastUpdated }
      })
    }
  }

  const nodes = [...sourceNodes.values(), ...targetNodes.values()]
  const laidOut = layoutLineageGraph({ nodes, edges })

  return {
    nodes: laidOut.nodes,
    edges: laidOut.edges,
    summary: { sources: sourceNodes.size, targets: targetNodes.size, links: edges.length }
  }
}

export default function DataLineageLive({
  sourceDetails,
  targetDetails,
  selectedTables,
  enabled = true,
  lastUpdated
}: Props) {
  const [graph, setGraph] = useState<GraphState>({
    status: 'idle',
    nodes: [],
    edges: [],
    summary: { sources: 0, targets: 0, links: 0 }
  })
  const [isPending, startTransition] = useTransition()

  const [search, setSearch] = useState('')
  const [schemaFilter, setSchemaFilter] = useState<string>('__all__')
  const [dbTypeFilter, setDbTypeFilter] = useState<Record<string, boolean>>({})

  const [drawerOpen, setDrawerOpen] = useState(false)
  const [drawerRole, setDrawerRole] = useState<'SOURCE' | 'TARGET'>('SOURCE')
  const [drawerNode, setDrawerNode] = useState<LineageNodeData | null>(null)
  const [columnRenames, setColumnRenames] = useState<ColumnRenameMap>({})

  const sourceTableList = useMemo(() => {
    return Object.entries(selectedTables)
      .filter(([, isSelected]) => isSelected)
      .map(([tableName]) => tableName)
  }, [selectedTables])

  useEffect(() => {
    if (!enabled) {
      setGraph({ status: 'idle', nodes: [], edges: [], summary: { sources: 0, targets: 0, links: 0 } })
      return
    }

    if (!sourceDetails || !targetDetails || sourceTableList.length === 0) {
      setGraph({ status: 'idle', nodes: [], edges: [], summary: { sources: 0, targets: 0, links: 0 } })
      return
    }

    setGraph((prev) => ({
      ...prev,
      status: prev.nodes.length ? 'loading' : 'loading',
      errorMessage: undefined
    }))

    const timestamp = toIsoTimestamp(lastUpdated)

    startTransition(() => {
      try {
        const built = buildGraph({ sourceDetails, targetDetails, sourceTables: sourceTableList, lastUpdated: timestamp })
        if (built.edges.length === 0) {
          setGraph({ status: 'empty', nodes: [], edges: [], summary: built.summary })
          return
        }

        // Calculate and add rename counts to target nodes
        const nodesWithRenames = built.nodes.map(node => {
          if (node.type === 'targetNode') {
            // Extract table name from node data
            const tableName = `${node.data.schema}.${node.data.table}`;
            
            // Count renames for this table
            const tableRenames = columnRenames[tableName];
            const renameCount = tableRenames ? Object.keys(tableRenames).length : 0;
            
            return {
              ...node,
              data: {
                ...node.data,
                renameCount: renameCount > 0 ? renameCount : undefined
              }
            };
          }
          return node;
        });

        setGraph({ status: 'ready', nodes: nodesWithRenames, edges: built.edges, summary: built.summary })
      } catch (e) {
        const message = e instanceof Error ? e.message : 'Unknown error'
        console.error('[Lineage] build failed:', e)
        setGraph((prev) => ({ ...prev, status: 'error', errorMessage: message }))
      }
    })
  }, [enabled, sourceDetails, targetDetails, sourceTableList, lastUpdated, startTransition])

  // Fetch column renames when the component mounts or when selected tables change
  useEffect(() => {
    const fetchColumnRenames = async () => {
      try {
        const response = await fetch('/api/session/get-column-renames');
        if (response.ok) {
          const data = await response.json();
          setColumnRenames(data);
        }
      } catch (error) {
        console.error('Error fetching column renames:', error);
      }
    };

    fetchColumnRenames();
  }, [sourceTableList]);

  const availableSchemas = useMemo(() => {
    const set = new Set<string>()
    for (const n of graph.nodes) set.add(n.data.schema)
    return Array.from(set).sort((a, b) => a.localeCompare(b))
  }, [graph.nodes])

  const availableDbTypes = useMemo(() => {
    const set = new Set<string>()
    for (const n of graph.nodes) set.add(n.data.databaseType)
    return Array.from(set).filter((v) => v && v !== 'unknown').sort((a, b) => a.localeCompare(b))
  }, [graph.nodes])

  useEffect(() => {
    if (!availableDbTypes.length) return
    setDbTypeFilter((prev) => {
      const next: Record<string, boolean> = { ...prev }
      let changed = false
      for (const t of availableDbTypes) {
        if (next[t] === undefined) {
          next[t] = true
          changed = true
        }
      }
      return changed ? next : prev
    })
  }, [availableDbTypes])

  const filtered = useMemo(() => {
    if (graph.status !== 'ready' && graph.status !== 'loading') return { nodes: graph.nodes, edges: graph.edges }

    const query = search.trim().toLowerCase()
    const allowSchemas = schemaFilter === '__all__' ? null : new Set([schemaFilter])
    const allowDbTypes = new Set(Object.entries(dbTypeFilter).filter(([, v]) => v).map(([k]) => k))
    const hasActiveFilters =
      query.length > 0 ||
      allowSchemas !== null ||
      (availableDbTypes.length > 0 && allowDbTypes.size !== availableDbTypes.length)

    if (!hasActiveFilters) return { nodes: graph.nodes, edges: graph.edges }

    const allowedNodeIds = new Set<string>()
    const nodes = graph.nodes.filter((n) => {
      if (allowSchemas && !allowSchemas.has(n.data.schema)) return false
      if (allowDbTypes.size && !allowDbTypes.has(n.data.databaseType)) return false
      if (query) {
        const hay = `${n.data.database} ${n.data.schema}.${n.data.table}`.toLowerCase()
        if (!hay.includes(query)) return false
      }
      allowedNodeIds.add(n.id)
      return true
    })

    const edges = graph.edges.filter((e) => allowedNodeIds.has(e.source) && allowedNodeIds.has(e.target))
    if (!nodes.length) return { nodes: [], edges: [] }
    const laidOut = layoutLineageGraph({ nodes, edges })
    return { nodes: laidOut.nodes, edges: laidOut.edges }
  }, [graph, search, schemaFilter, dbTypeFilter, availableDbTypes.length])

  const onNodeClick = useCallback((node: Node<LineageNodeData>) => {
    const role = node.type === 'targetNode' ? 'TARGET' : 'SOURCE'
    setDrawerRole(role)
    setDrawerNode(node.data)
    setDrawerOpen(true)
  }, [])

  const onCloseDrawer = useCallback(() => setDrawerOpen(false), [])

  return (
    <Fragment>
      <div className="bg-white rounded-lg shadow mb-6 border border-gray-100">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
          <div>
            <h3 className="text-lg font-semibold text-[#085690]">Data Lineage (Live)</h3>
            <p className="text-sm text-gray-600">Object-level Source → Target mappings. Updates when you change table selections.</p>
          </div>
          <div className="text-xs text-gray-500">
            {graph.summary.sources} sources · {graph.summary.targets} targets · {graph.summary.links} links
          </div>
        </div>

        <div className="h-[460px] relative">
          {!enabled ? (
            <EmptyState title="Lineage disabled" message="Enable lineage to view object-level Source → Target mappings." />
          ) : !sourceDetails || !targetDetails ? (
            <EmptyState title="Select source and target tables to view lineage" message="Choose connections and pick tables to render live lineage." />
          ) : sourceTableList.length === 0 ? (
            <EmptyState title="Select source and target tables to view lineage" message="Select one or more source tables to render live lineage." />
          ) : graph.status === 'error' ? (
            <EmptyState title="Lineage failed to load" message={graph.errorMessage || 'Unexpected error while building lineage.'} />
          ) : graph.status === 'empty' ? (
            <EmptyState title="No lineage available" message="No direct Source → Target table mappings were found for the current selection." />
          ) : graph.status === 'idle' ? (
            <EmptyState title="Lineage will appear here" message="Select tables to view object-level Source → Target mappings." />
          ) : graph.status === 'loading' && graph.nodes.length === 0 ? (
            <Skeleton />
          ) : filtered.nodes.length === 0 ? (
            <EmptyState title="No objects match filters" message="Clear search/filters to display the current lineage selection." />
          ) : (
            <div className="absolute inset-0 flex">
              <div className="w-[260px] border-r border-gray-100 bg-gradient-to-b from-white to-gray-50/30 p-4 h-full overflow-auto">
                <div className="text-[11px] font-semibold uppercase tracking-[0.12em] text-gray-500">Filters</div>
                <div className="mt-3 space-y-3">
                  <div>
                    <label className="text-[11px] font-semibold text-gray-700">Search</label>
                    <input
                      value={search}
                      onChange={(e) => setSearch(e.target.value)}
                      placeholder="schema.table"
                      className="mt-1 w-full input-modern text-sm"
                    />
                  </div>
                  <div>
                    <label className="text-[11px] font-semibold text-gray-700">Schema</label>
                    <select
                      value={schemaFilter}
                      onChange={(e) => setSchemaFilter(e.target.value)}
                      className="mt-1 w-full input-modern text-sm"
                    >
                      <option value="__all__">All schemas</option>
                      {availableSchemas.map((s) => (
                        <option key={s} value={s}>
                          {s}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <div className="text-[11px] font-semibold text-gray-700">Database Type</div>
                    <div className="mt-2 space-y-2">
                      {availableDbTypes.length ? (
                        availableDbTypes.map((t) => (
                          <label key={t} className="flex items-center gap-2 text-sm text-gray-700">
                            <input
                              type="checkbox"
                              checked={dbTypeFilter[t] !== false}
                              onChange={(e) => setDbTypeFilter((prev) => ({ ...prev, [t]: e.target.checked }))}
                            />
                            <span className="text-[12px] font-semibold capitalize">{t}</span>
                          </label>
                        ))
                      ) : (
                        <div className="text-[12px] text-gray-500">—</div>
                      )}
                    </div>
                  </div>
                  <div className="pt-2">
                    <div className="rounded-xl border border-gray-200 bg-white p-3">
                      <div className="text-[11px] font-semibold text-gray-900">Graph</div>
                      <div className="mt-1 text-[12px] text-gray-600">
                        {filtered.nodes.length} nodes · {filtered.edges.length} edges
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              <div className="flex-1 h-full">
                <LineageFlow nodes={filtered.nodes} edges={filtered.edges} onNodeClick={onNodeClick} />
              </div>
            </div>
          )}

          {(graph.status === 'loading' && graph.nodes.length > 0) || isPending ? (
            <div className="absolute inset-0 bg-white/50 backdrop-blur-[1px] flex items-center justify-center pointer-events-none">
              <div className="flex items-center gap-3 px-4 py-2 rounded-lg border border-gray-200 bg-white shadow-sm">
                <div className="w-3 h-3 rounded-full bg-[#085690] animate-pulse" />
                <span className="text-sm font-medium text-gray-700">Updating lineage…</span>
              </div>
            </div>
          ) : null}
        </div>
      </div>

      <TableDetailsDrawer open={drawerOpen} role={drawerRole} node={drawerNode} columnRenames={columnRenames} onClose={onCloseDrawer} />
    </Fragment>
  )
}
