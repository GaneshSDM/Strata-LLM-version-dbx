import dagre from 'dagre'
import type { Edge, Node } from 'reactflow'

const DEFAULT_NODE_WIDTH = 304
const DEFAULT_NODE_HEIGHT = 124

export const layoutLineageGraph = <TData, TEdgeData = unknown>({
  nodes,
  edges,
  nodeWidth = DEFAULT_NODE_WIDTH,
  nodeHeight = DEFAULT_NODE_HEIGHT
}: {
  nodes: Array<Node<TData>>
  edges: Array<Edge<TEdgeData>>
  nodeWidth?: number
  nodeHeight?: number
}) => {
  const g = new dagre.graphlib.Graph()
  g.setDefaultEdgeLabel(() => ({}))
  g.setGraph({
    rankdir: 'LR',
    nodesep: 64,
    ranksep: 160,
    marginx: 24,
    marginy: 24
  })

  const orderedNodes = [...nodes].sort((a, b) => a.id.localeCompare(b.id))
  for (const node of orderedNodes) {
    g.setNode(node.id, { width: nodeWidth, height: nodeHeight })
  }

  const orderedEdges = [...edges].sort((a, b) => a.id.localeCompare(b.id))
  for (const edge of orderedEdges) {
    g.setEdge(edge.source, edge.target)
  }

  dagre.layout(g)

  const positioned = orderedNodes.map((node) => {
    const pos = g.node(node.id) as { x: number; y: number } | undefined
    if (!pos) return node
    return {
      ...node,
      position: { x: pos.x - nodeWidth / 2, y: pos.y - nodeHeight / 2 }
    }
  })

  return { nodes: positioned, edges: orderedEdges }
}

