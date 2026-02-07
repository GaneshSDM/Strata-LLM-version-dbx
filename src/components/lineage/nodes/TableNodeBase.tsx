import { memo } from 'react'
import { Handle, Position, type NodeProps } from 'reactflow'
import DbLogo from '../DbLogo'
import { useLineageHover } from '../interaction'
import type { LineageNodeData } from '../types'
import { formatCompactNumber, loadTypePillClass, statusPillClass, toStatusLabel } from '../utils'

type Role = 'SOURCE' | 'TARGET'

type ExtendedLineageNodeData = LineageNodeData & {
  renameCount?: number;
};

type Props = NodeProps<ExtendedLineageNodeData> & { role: Role }

const TableNodeBase = memo(function TableNodeBase({ data, id, role }: Props) {
  const { hoveredNodeId, adjacentNodeIdsByNodeId } = useLineageHover()
  const isSource = role === 'SOURCE'
  const badgeClass = isSource ? 'badge-primary' : 'badge-accent'
  const accent = isSource ? 'border-primary-200 bg-primary-50/20' : 'border-accent-200 bg-accent-50/20'
  const handleColor = isSource ? '!bg-primary-500' : '!bg-accent-500'
  const handleType = isSource ? 'source' : 'target'
  const handlePos = isSource ? Position.Right : Position.Left
  const ringClass = isSource ? 'ring-primary-200' : 'ring-accent-200'

  const isHovered = hoveredNodeId === id
  const connectedIds = hoveredNodeId ? adjacentNodeIdsByNodeId.get(hoveredNodeId) : null
  const isConnected = hoveredNodeId ? connectedIds?.has(id) : false
  const dimOthers = hoveredNodeId && !isHovered && !isConnected

  return (
    <div
      className={[
        'w-[304px] rounded-xl border bg-white shadow-sm transition-shadow transition-opacity',
        accent,
        dimOthers ? 'opacity-50' : 'opacity-100',
        hoveredNodeId && (isHovered || isConnected) ? `ring-2 ${ringClass} ring-offset-2 ring-offset-white` : ''
      ].join(' ')}
    >
      <div className="flex items-start justify-between gap-3 px-4 py-3 border-b border-gray-100">
        <div className="min-w-0 flex items-start gap-2">
          <DbLogo databaseType={data.databaseType} className="w-5 h-5 object-contain filter grayscale brightness-0 mt-0.5" />
          <div className="min-w-0">
            <div className="text-[11px] font-semibold text-gray-500 uppercase tracking-[0.12em] truncate">
              {data.database}
            </div>
            <div className="text-[13px] font-semibold text-gray-900 truncate">
              <span className="font-bold">{data.schema}</span>
              <span className="text-gray-400 mx-1">·</span>
              <span className="font-bold">{data.table}</span>
            </div>
          </div>
        </div>
        <div className="flex flex-col items-end">
          {data.renameCount !== undefined && data.renameCount > 0 && (
            <span className="inline-flex items-center justify-center px-2 py-1 rounded-full text-[10px] font-bold bg-yellow-100 text-yellow-800 border border-yellow-200">
              {data.renameCount} {data.renameCount === 1 ? 'column' : 'columns'} renamed
            </span>
          )}
          <span className={badgeClass}>{role}</span>
        </div>
      </div>

      <div className="px-4 py-3 grid grid-cols-2 gap-x-3 gap-y-2">
        <div className="text-[11px] text-gray-600">
          <div className="text-[10px] uppercase tracking-[0.14em] text-gray-400 font-semibold">Rows</div>
          <div className="text-[12px] font-semibold text-gray-900">{formatCompactNumber(data.rowCount)}</div>
        </div>
        <div className="text-[11px] text-gray-600">
          <div className="text-[10px] uppercase tracking-[0.14em] text-gray-400 font-semibold">Columns</div>
          <div className="text-[12px] font-semibold text-gray-900">{formatCompactNumber(data.columnCount)}</div>
        </div>
        <div className="flex items-center gap-2">
          <span className={`inline-flex items-center px-2 py-0.5 rounded-full border text-[10px] font-semibold ${loadTypePillClass(data.loadType)}`}>
            {data.loadType}
          </span>
          <span className={`inline-flex items-center px-2 py-0.5 rounded-full border text-[10px] font-semibold ${statusPillClass(data.status)}`}>
            {toStatusLabel(data.status)}
          </span>
        </div>
        <div className="text-[11px] text-gray-600 text-right">
          <div className="text-[10px] uppercase tracking-[0.14em] text-gray-400 font-semibold">Updated</div>
          <div className="text-[12px] font-semibold text-gray-900 truncate" title={data.lastUpdated}>
            {new Date(data.lastUpdated).toLocaleString()}
          </div>
        </div>
      </div>

      <Handle
        type={handleType as 'source' | 'target'}
        position={handlePos}
        className={`!w-2.5 !h-2.5 !border-2 !border-white ${handleColor}`}
        style={{ top: '50%', transform: 'translateY(-50%)' }}
      />
    </div>
  )
})

export default TableNodeBase
