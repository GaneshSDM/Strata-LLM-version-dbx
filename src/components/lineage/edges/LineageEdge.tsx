import { memo, useMemo, useState } from 'react'
import { BaseEdge, EdgeLabelRenderer, getSmoothStepPath, type EdgeProps } from 'reactflow'
import { useLineageHover } from '../interaction'
import type { LineageEdgeData } from '../types'

const Tooltip = ({ data }: { data?: LineageEdgeData }) => {
  if (!data) return null
  return (
    <div className="pointer-events-none rounded-lg border border-gray-200 bg-white/95 px-3 py-2 shadow-sm backdrop-blur">
      <div className="text-[11px] font-semibold text-gray-900">{data.mappingType}</div>
      <div className="mt-1 grid grid-cols-2 gap-x-3 gap-y-0.5 text-[10px] text-gray-600">
        <div className="font-semibold text-gray-500 uppercase tracking-[0.12em]">Load</div>
        <div className="text-right font-semibold text-gray-800">{data.loadMode}</div>
        <div className="font-semibold text-gray-500 uppercase tracking-[0.12em]">Last Run</div>
        <div className="text-right font-semibold text-gray-800">{new Date(data.lastExecutionTime).toLocaleString()}</div>
      </div>
    </div>
  )
}

function LineageEdgeInner(props: EdgeProps<LineageEdgeData>) {
  const { id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, style, data, markerEnd, markerStart } =
    props
  const { hoveredNodeId } = useLineageHover()
  const [hovered, setHovered] = useState(false)

  const [edgePath, labelX, labelY] = useMemo(
    () => getSmoothStepPath({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, borderRadius: 16 }),
    [sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition]
  )

  const isConnectedToHovered =
    hoveredNodeId != null && (props.source === hoveredNodeId || props.target === hoveredNodeId)
  const dim = hoveredNodeId != null && !isConnectedToHovered

  const effectiveStyle = useMemo(() => {
    const base = (style || {}) as Record<string, unknown>
    const opacity = typeof (base as any).opacity === 'number' ? ((base as any).opacity as number) : 0.55
    const strokeWidth = typeof (base as any).strokeWidth === 'number' ? ((base as any).strokeWidth as number) : 2
    return {
      ...base,
      opacity: dim ? Math.min(0.18, opacity) : Math.max(0.8, opacity),
      strokeWidth: dim ? strokeWidth : Math.max(2.5, strokeWidth)
    } as any
  }, [style, dim])

  return (
    <>
      <BaseEdge id={id} path={edgePath} style={effectiveStyle} markerEnd={markerEnd} markerStart={markerStart} />
      <path
        d={edgePath}
        fill="none"
        stroke="transparent"
        strokeWidth={18}
        style={{ pointerEvents: 'stroke' }}
        onMouseEnter={() => setHovered(true)}
        onMouseLeave={() => setHovered(false)}
      />
      <EdgeLabelRenderer>
        <div
          style={{
            transform: `translate(-50%, -100%) translate(${labelX}px,${labelY}px)`,
            position: 'absolute'
          }}
          className="nodrag nopan"
        >
          {hovered ? <Tooltip data={data} /> : null}
        </div>
      </EdgeLabelRenderer>
    </>
  )
}

const LineageEdge = memo(LineageEdgeInner)
export default LineageEdge
