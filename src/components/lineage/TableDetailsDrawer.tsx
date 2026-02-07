import { X } from 'lucide-react'
import type { LineageNodeData } from './types'
import DbLogo from './DbLogo'
import { formatCompactNumber, loadTypePillClass, statusPillClass, toStatusLabel } from './utils'

// Define the type for column renames
interface ColumnRenameMap {
  [tableName: string]: {
    [oldColumnName: string]: string;
  };
}

type Props = {
  open: boolean
  role: 'SOURCE' | 'TARGET'
  node?: LineageNodeData | null
  columnRenames?: ColumnRenameMap
  onClose: () => void
}

const Field = ({ label, value }: { label: string; value: string }) => (
  <div className="grid grid-cols-3 gap-3 py-2">
    <div className="text-[11px] font-semibold uppercase tracking-[0.12em] text-gray-500">{label}</div>
    <div className="col-span-2 text-[12px] font-semibold text-gray-900 break-words">{value}</div>
  </div>
)

export default function TableDetailsDrawer({ open, role, node, columnRenames, onClose }: Props) {
  // Get renamed columns for this table if it exists
  const renamedColumns = node && columnRenames 
    ? columnRenames[`${node.schema}.${node.table}`] 
    : {};
  
  return (
    <div
      className={`fixed inset-0 z-50 ${open ? '' : 'pointer-events-none'}`}
      aria-hidden={!open}
      aria-label="Lineage node details"
    >
      <div
        className={`absolute inset-0 bg-black/30 transition-opacity ${open ? 'opacity-100' : 'opacity-0'}`}
        onClick={onClose}
      />
      <div
        className={`absolute right-0 top-0 h-full w-full max-w-[420px] bg-white shadow-2xl border-l border-gray-200 transition-transform ${
          open ? 'translate-x-0' : 'translate-x-full'
        }`}
        role="dialog"
        aria-modal="true"
      >
        <div className="px-5 py-4 border-b border-gray-100 flex items-center justify-between">
          <div className="flex items-center gap-3 min-w-0">
            {node ? <DbLogo databaseType={node.databaseType} className="w-6 h-6 object-contain filter grayscale brightness-0" /> : null}
            <div className="min-w-0">
              <div className="text-[11px] font-semibold uppercase tracking-[0.12em] text-gray-500 truncate">
                {role} Table
              </div>
              <div className="text-[15px] font-bold text-gray-900 truncate">
                {node ? `${node.schema}.${node.table}` : '—'}
              </div>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 rounded-lg hover:bg-gray-50 border border-transparent hover:border-gray-200"
            aria-label="Close"
          >
            <X size={18} />
          </button>
        </div>

        {!node ? (
          <div className="p-6 text-sm text-gray-600">No node selected.</div>
        ) : (
          <div className="p-5 overflow-auto h-[calc(100%-64px)]">
            <div className="rounded-xl border border-gray-200 p-4 bg-gradient-to-br from-white to-gray-50">
              <div className="flex flex-wrap items-center gap-2">
                <span className={`inline-flex items-center px-2.5 py-1 rounded-full border text-[11px] font-semibold ${loadTypePillClass(node.loadType)}`}>
                  {node.loadType}
                </span>
                <span className={`inline-flex items-center px-2.5 py-1 rounded-full border text-[11px] font-semibold ${statusPillClass(node.status)}`}>
                  {toStatusLabel(node.status)}
                </span>
              </div>
              <div className="mt-4 divide-y divide-gray-100">
                <Field label="Database" value={node.database} />
                <Field label="Schema" value={node.schema} />
                <Field label="Table" value={node.table} />
                <Field label="Rows" value={formatCompactNumber(node.rowCount)} />
                <Field label="Columns" value={formatCompactNumber(node.columnCount)} />
                <Field label="Last Updated" value={new Date(node.lastUpdated).toLocaleString()} />
              </div>
            </div>

            {/* Show renamed columns if any exist */}
            {Object.keys(renamedColumns).length > 0 && (
              <div className="mt-4 rounded-xl border border-blue-200 bg-blue-50 p-4">
                <div className="text-[12px] font-semibold text-blue-900">Renamed Columns</div>
                <div className="mt-2 space-y-1">
                  {Object.entries(renamedColumns).map(([oldName, newName]) => (
                    <div key={oldName} className="text-[12px] text-gray-700 font-mono">
                      {oldName} → {newName}
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div className="mt-4 rounded-xl border border-gray-200 p-4">
              <div className="text-[12px] font-semibold text-gray-900">Usage</div>
              <div className="mt-2 text-[12px] text-gray-600 leading-relaxed">
                This object participates in the current Source → Target mapping selection. Use this panel to confirm the database,
                schema, and high-level runtime metadata before migration or reconciliation.
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
