import { useMemo, useState } from 'react'
import { ArrowLeft, PlugZap, RefreshCcw, FileCode2, Copy, CheckCircle2, AlertTriangle } from 'lucide-react'
import { useNavigate } from 'react-router-dom'

export default function DatabricksAddon() {
  const navigate = useNavigate()
  const [host, setHost] = useState('')
  const [httpPath, setHttpPath] = useState('')
  const [accessToken, setAccessToken] = useState('')
  const [catalog, setCatalog] = useState('hive_metastore')
  const [schema, setSchema] = useState('default')
  const [oracleDdl, setOracleDdl] = useState('')
  const [objectName, setObjectName] = useState('')
  const [testing, setTesting] = useState(false)
  const [testResult, setTestResult] = useState<{ ok: boolean; message?: string } | null>(null)
  const [converting, setConverting] = useState(false)
  const [convertedDdl, setConvertedDdl] = useState('')
  const [copied, setCopied] = useState(false)

  const canTest = useMemo(() => !!host && !!httpPath && !!accessToken, [host, httpPath, accessToken])
  const canConvert = useMemo(() => !!oracleDdl.trim(), [oracleDdl])

  const testConnection = async () => {
    setTesting(true)
    setTestResult(null)
    try {
      const res = await fetch('/api/connections/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dbType: 'Databricks',
          name: 'Databricks Serving',
          credentials: {
            host,
            http_path: httpPath,
            access_token: accessToken,
            catalog,
            schema
          }
        })
      })
      const data = await res.json()
      setTestResult({ ok: !!data.ok, message: data.message || (data.ok ? 'Connection successful' : 'Connection failed') })
    } catch (err: any) {
      setTestResult({ ok: false, message: err?.message || 'Connection failed' })
    } finally {
      setTesting(false)
    }
  }

  const convertDdl = async () => {
    setConverting(true)
    setConvertedDdl('')
    try {
      const res = await fetch('/api/ddl/convert', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sourceDialect: 'Oracle',
          targetDialect: 'Databricks',
          sourceDdl: oracleDdl,
          objectName: objectName || 'oracle_object',
          objectKind: 'table'
        })
      })
      const data = await res.json()
      if (data.ok) {
        setConvertedDdl(data.target_sql || '')
      } else {
        setConvertedDdl(data.message || 'Conversion failed')
      }
    } catch (err: any) {
      setConvertedDdl(err?.message || 'Conversion failed')
    } finally {
      setConverting(false)
    }
  }

  const copyDdl = async () => {
    if (!convertedDdl) return
    await navigator.clipboard.writeText(convertedDdl)
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }

  return (
    <div className="max-w-6xl">
      <div className="flex items-start justify-between gap-4 mb-6">
        <div>
          <h1 className="text-3xl font-bold text-[#085690]">Databricks DDL Add-on</h1>
          <p className="text-sm text-gray-600">Connect to Databricks serving endpoints and convert Oracle DDL to Databricks SQL.</p>
        </div>
        <button
          type="button"
          onClick={() => navigate('/history')}
          className="flex items-center gap-2 px-4 py-2 rounded-lg border-2 border-[#085690] text-[#085690] bg-white hover:bg-[#085690] hover:text-white transition-all font-medium"
        >
          <ArrowLeft size={18} />
          Back
        </button>
      </div>

      <div className="space-y-6">
        <div className="bg-white rounded-lg shadow p-4 border-t-4 border-[#085690]">
          <div className="flex items-center gap-2 mb-3">
            <PlugZap size={18} className="text-[#085690]" />
            <h2 className="text-base font-semibold text-gray-900">Databricks Serving Point</h2>
          </div>

          <div className="flex flex-wrap items-end gap-3">
            <label className="text-xs text-gray-700">
              Hostname
              <input
                value={host}
                onChange={(e) => setHost(e.target.value)}
                className="mt-1 w-52 border border-gray-200 rounded-md px-2 py-1.5 text-xs"
                placeholder="adb-xxx.azuredatabricks.net"
              />
            </label>
            <label className="text-xs text-gray-700">
              HTTP Path
              <input
                value={httpPath}
                onChange={(e) => setHttpPath(e.target.value)}
                className="mt-1 w-56 border border-gray-200 rounded-md px-2 py-1.5 text-xs"
                placeholder="/sql/1.0/warehouses/..."
              />
            </label>
            <label className="text-xs text-gray-700">
              Access Token
              <input
                type="password"
                value={accessToken}
                onChange={(e) => setAccessToken(e.target.value)}
                className="mt-1 w-44 border border-gray-200 rounded-md px-2 py-1.5 text-xs"
                placeholder="dapi..."
              />
            </label>
            <label className="text-xs text-gray-700">
              Catalog
              <input
                value={catalog}
                onChange={(e) => setCatalog(e.target.value)}
                className="mt-1 w-36 border border-gray-200 rounded-md px-2 py-1.5 text-xs"
              />
            </label>
            <label className="text-xs text-gray-700">
              Schema
              <input
                value={schema}
                onChange={(e) => setSchema(e.target.value)}
                className="mt-1 w-32 border border-gray-200 rounded-md px-2 py-1.5 text-xs"
              />
            </label>
            <button
              type="button"
              onClick={testConnection}
              disabled={!canTest || testing}
              className="inline-flex items-center gap-2 px-3 py-1.5 rounded-md bg-[#085690] text-white text-xs font-semibold hover:shadow disabled:opacity-50"
            >
              <RefreshCcw size={14} className={testing ? 'animate-spin' : ''} />
              {testing ? 'Testing...' : 'Test'}
            </button>
            {testResult && (
              <div className={`inline-flex items-center gap-2 text-xs ${testResult.ok ? 'text-emerald-600' : 'text-rose-600'}`}>
                {testResult.ok ? <CheckCircle2 size={14} /> : <AlertTriangle size={14} />}
                {testResult.message}
              </div>
            )}
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6 border-t-4 border-[#ec6225]">
          <div className="flex items-center gap-2 mb-4">
            <FileCode2 size={20} className="text-[#ec6225]" />
            <h2 className="text-lg font-semibold text-gray-900">Oracle to Databricks DDL</h2>
          </div>

          <label className="text-sm text-gray-700">
            Object Name (optional)
            <input
              value={objectName}
              onChange={(e) => setObjectName(e.target.value)}
              className="mt-1 w-full border border-gray-200 rounded-md px-3 py-2 text-sm"
              placeholder="HR.EMPLOYEES"
            />
          </label>

          <div className="mt-4 grid grid-cols-1 lg:grid-cols-2 gap-4 lg:auto-rows-fr">
            <label className="text-sm text-gray-700 block h-full">
              <div className="font-medium">Source Oracle DDL</div>
              <textarea
                value={oracleDdl}
                onChange={(e) => setOracleDdl(e.target.value)}
                className="mt-1 w-full h-full border border-gray-200 rounded-md px-3 py-2 text-xs font-mono min-h-[240px]"
                placeholder="CREATE TABLE ..."
              />
            </label>

            <div className="text-sm text-gray-700 flex flex-col h-full">
              <div className="font-medium">Target Databricks DDL</div>
              <div className="mt-1 bg-gray-50 border border-gray-200 rounded-md p-3 flex-1 min-h-[240px]">
                <pre className="text-xs font-mono text-gray-900 whitespace-pre-wrap">
                  {convertedDdl || 'Converted DDL will appear here.'}
                </pre>
              </div>
            </div>
          </div>

          <div className="mt-4 flex items-center gap-3">
            <button
              type="button"
              onClick={convertDdl}
              disabled={!canConvert || converting}
              className="inline-flex items-center gap-2 px-4 py-2 rounded-md bg-[#ec6225] text-white text-sm font-semibold hover:shadow disabled:opacity-50"
            >
              {converting ? 'Converting...' : 'Convert DDL'}
            </button>
            <button
              type="button"
              onClick={copyDdl}
              disabled={!convertedDdl}
              className="inline-flex items-center gap-2 px-3 py-2 rounded-md border border-gray-200 text-sm text-gray-700 hover:bg-gray-50 disabled:opacity-50"
            >
              <Copy size={14} />
              {copied ? 'Copied' : 'Copy Result'}
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
