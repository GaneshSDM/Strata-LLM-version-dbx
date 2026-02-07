import { memo, useMemo, useState } from 'react'
import ReactFlow, { Background, Controls, type Edge, type Node } from 'reactflow'
import 'reactflow/dist/style.css'

import LineageEdge from './lineage/edges/LineageEdge'
import SourceNode from './lineage/nodes/SourceNode'
import TargetNode from './lineage/nodes/TargetNode'
import { LineageHoverProvider } from './lineage/interaction'
import type { LineageEdgeData, LineageNodeData } from './lineage/types'

type Props = {
  nodes: Array<Node<LineageNodeData>>
  edges: Array<Edge<LineageEdgeData>>
  onNodeClick?: (node: Node<LineageNodeData>) => void
}

const LineageFlow = memo(function LineageFlow({ nodes, edges, onNodeClick }: Props) {
  const nodeTypes = useMemo(() => ({ sourceNode: SourceNode, targetNode: TargetNode }), [])
  const edgeTypes = useMemo(() => ({ lineageEdge: LineageEdge }), [])
  const [hoverNodeId, setHoverNodeId] = useState<string | null>(null)

  const adjacentNodeIdsByNodeId = useMemo(() => {
    const map = new Map<string, Set<string>>()
    const add = (a: string, b: string) => {
      const set = map.get(a) || new Set<string>()
      set.add(b)
      map.set(a, set)
    }
    for (const e of edges) {
      add(e.source, e.target)
      add(e.target, e.source)
    }
    return map
  }, [edges])

  return (
    <LineageHoverProvider value={{ hoveredNodeId: hoverNodeId, adjacentNodeIdsByNodeId }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        fitView
        nodesDraggable={false}
        nodesConnectable={false}
        zoomOnScroll
        panOnScroll
        minZoom={0.6}
        maxZoom={1.5}
        proOptions={{ hideAttribution: true }}
        onNodeClick={(_, node) => {
          setHoverNodeId(null)
          onNodeClick?.(node)
        }}
        onNodeMouseEnter={(_, node) => setHoverNodeId(node.id)}
        onNodeMouseLeave={() => setHoverNodeId(null)}
      >
        <Background gap={16} />
        <Controls />
      </ReactFlow>
    </LineageHoverProvider>
  )
})

LineageFlow.displayName = 'LineageFlow'
export default LineageFlow
