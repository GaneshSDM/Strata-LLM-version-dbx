import type { NodeProps } from 'reactflow'
import type { LineageNodeData } from '../types'
import TableNodeBase from './TableNodeBase'

export default function SourceNode(props: NodeProps<LineageNodeData>) {
  return <TableNodeBase {...props} role="SOURCE" />
}

