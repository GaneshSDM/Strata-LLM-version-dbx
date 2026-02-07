import { useState, useEffect, useRef } from 'react'
import { Settings, Database, Plus, Edit2, Trash2, X, ChevronDown, Copy } from 'lucide-react'
import AmazonS3Icon from '../assets/AmazonS3.png'
import BigqueryIcon from '../assets/Bigquery.png'
import DatabricksIcon from '../assets/Databricks.jpeg'
import MysqlIcon from '../assets/Mysql.png'
import OracleIcon from '../assets/Oracle.png'
import PostgresqlIcon from '../assets/Postgresql.png'
import SnowflakeIcon from '../assets/Snowflake.png'

const DB_TYPES = [
  'MySQL',
  'Snowflake',
  'Databricks',
  'Oracle'
]

type Connection = {
  id: number
  name: string
  db_type: string
  created_at: string
}

type Props = {
  onClose: () => void
  onSaved: () => void
}

const getDatabaseIcon = (dbType: string) => {
  switch (dbType) {
    case 'PostgreSQL':
      return PostgresqlIcon;
    case 'MySQL':
      return MysqlIcon;
    case 'Snowflake':
      return SnowflakeIcon;
    case 'Databricks':
      return DatabricksIcon;
    case 'Oracle':
      return OracleIcon;
    case 'SQL Server':
      return undefined; // No icon for SQL Server in assets
    case 'Teradata':
      return undefined; // No icon for Teradata in assets
    case 'Google BigQuery':
      return BigqueryIcon;
    case 'AWS S3':
      return AmazonS3Icon;
    default:
      return undefined;
  }
};

const getDatabaseDisplayName = (dbType: string) => {
  switch (dbType) {
    case 'Google BigQuery':
      return 'BigQuery';
    case 'AWS S3':
      return 'Amazon S3';
    default:
      return dbType;
  }
};

export function ConnectionModal({ onClose, onSaved }: Props) {
  const [view, setView] = useState<'list' | 'form'>('list')
  const [connections, setConnections] = useState<Connection[]>([])
  const [editingId, setEditingId] = useState<number | null>(null)

  const [name, setName] = useState('')
  const [dbType, setDbType] = useState('')
  const [credentials, setCredentials] = useState<any>({})
  const [tested, setTested] = useState(false)
  const [testing, setTesting] = useState(false)
  const [testMessage, setTestMessage] = useState<{type: 'success' | 'error', text: string} | null>(null)
  const [showDbTypeDropdown, setShowDbTypeDropdown] = useState(false)

  const [loading, setLoading] = useState(false)
  const [showUploadPopup, setShowUploadPopup] = useState(false)
  const [successMessage, setSuccessMessage] = useState<{ text: string; id: number } | null>(null)
  const [uploadStatus, setUploadStatus] = useState<{ state: 'idle' | 'selected' | 'uploading' | 'success' | 'failed', message?: string }>({ state: 'idle' })
  const [showConnectionNameModal, setShowConnectionNameModal] = useState(false)
  const [pendingFile, setPendingFile] = useState<File | null>(null)
  const [connectionNameInput, setConnectionNameInput] = useState('')
  const [uploadTemplateType, setUploadTemplateType] = useState<'MySQL' | 'Snowflake' | 'Databricks' | 'Oracle'>('MySQL')
  const successTimeoutRef = useRef<number | null>(null)

  useEffect(() => {
    loadConnections()
  }, [])

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (showDbTypeDropdown && !(event.target as Element).closest('.db-type-dropdown')) {
        setShowDbTypeDropdown(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [showDbTypeDropdown]);

  useEffect(() => {
    return () => {
      if (successTimeoutRef.current) {
        clearTimeout(successTimeoutRef.current)
      }
    }
  }, [])

  const clearSuccessMessage = () => {
    if (successTimeoutRef.current) {
      clearTimeout(successTimeoutRef.current)
      successTimeoutRef.current = null
    }
    setSuccessMessage(null)
  }

  const showSuccessMessage = (message: string) => {
    if (successTimeoutRef.current) {
      clearTimeout(successTimeoutRef.current)
    }
    setSuccessMessage({ text: message, id: Date.now() })
    successTimeoutRef.current = window.setTimeout(() => clearSuccessMessage(), 5000)
  }

  const normalizeCredentialsForDb = (type: string, creds: any) => {
    const copy = { ...(creds || {}) }
    if (type === 'MySQL') {
      if (typeof copy.ssl === 'string') {
        const val = copy.ssl.trim().toLowerCase()
        copy.ssl = ['true', '1', 'yes', 'y', 'on', 'required', 'require'].includes(val)
      } else {
        copy.ssl = !!copy.ssl
      }
    }
    if (type === 'PostgreSQL' && typeof copy.sslmode === 'string') {
      copy.sslmode = copy.sslmode.trim().toLowerCase()
    }
    return copy
  }

  const uploadTemplates: Record<typeof uploadTemplateType, string> = {
    MySQL: `database type: MySQL
host:
port:
username:
password:
database:
ssl: true`,
    Snowflake: `database type: Snowflake
account:
user:
password:
warehouse:
db:
schema:`,
    Databricks: `database type: Databricks
server hostname:
http path:
access token:
catalog:
schema:`,
    Oracle: `database type: Oracle
host:
port:
service name:
username:
password:
schema:`
  }

  const copyTemplate = async () => {
    try {
      await navigator.clipboard.writeText(uploadTemplates[uploadTemplateType])
      setUploadStatus({ state: 'selected', message: 'Template copied' })
    } catch (err) {
      console.error('Failed to copy template', err)
    }
  }

  const loadConnections = async () => {
    try {
      const res = await fetch('/api/connections')
      const data = await res.json()
      if (data.ok) {
        setConnections(data.data)
      }
    } catch (err) {
      console.error('Failed to load connections:', err)
    }
  }

  const resetForm = () => {
    setName('')
    setDbType('')
    setCredentials({})
    setTested(false)
    setTesting(false)
    setTestMessage(null)
    setEditingId(null)
  }

  const saveConnection = async () => {
    if (!tested) return
    
    try {
      const normalizedCreds = normalizeCredentialsForDb(dbType, credentials)
      // For editing existing connections, use PUT endpoint
      if (editingId) {
        const res = await fetch(`/api/connections/${editingId}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name,
            dbType,
            credentials: normalizedCreds
          })
        })
        const data = await res.json()
        if (data.ok) {
          resetForm()
          setView('list')
          loadConnections()
          onSaved()
        } else {
          alert('Failed to update connection: ' + data.message)
        }
      } else {
        // For new connections, use POST endpoint
        const normalizedCreds = normalizeCredentialsForDb(dbType, credentials)
        const res = await fetch('/api/connections/save', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name,
            dbType,
            credentials: normalizedCreds
          })
        })
        const data = await res.json()
        if (data.ok) {
          showSuccessMessage('Connection established successfully!')
          resetForm()
          setView('list')
          loadConnections()
          onSaved()
        } else {
          alert('Failed to save connection: ' + data.message)
        }
      }
    } catch (err) {
      console.error('Failed to save connection:', err)
      alert('Failed to save connection')
    }
  }

  const testConnection = async () => {
    setTesting(true)
    setTestMessage(null)
    try {
      const normalizedCreds = normalizeCredentialsForDb(dbType, credentials)
      const res = await fetch('/api/connections/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dbType, name, credentials: normalizedCreds })
      })
      const data = await res.json()
      if (data.ok) {
        setTested(true)
        setTestMessage({ type: 'success', text: data.message || 'Connection established successfully!' })
      } else {
        setTestMessage({ type: 'error', text: data.message || 'Connection failed' })
      }
    } catch (err) {
      console.error('Connection test failed:', err)
      setTestMessage({ type: 'error', text: 'Connection test failed' })
    } finally {
      setTesting(false)
    }
  }

  const deleteConnection = async (id: number) => {
    if (!confirm('Are you sure you want to delete this connection?')) return
    
    try {
      const res = await fetch(`/api/connections/${id}`, { method: 'DELETE' })
      const data = await res.json()
      if (data.ok) {
        loadConnections()
      } else {
        alert('Failed to delete connection')
      }
    } catch (err) {
      console.error('Failed to delete connection:', err)
      alert('Failed to delete connection')
    }
  }

  const editConnection = async (conn: Connection) => {
    setEditingId(conn.id)
    setName(conn.name)
    setDbType(conn.db_type)
    setTested(true) // Assume existing connections are already tested
    setLoading(true)
    
    // Fetch full connection details with credentials
    try {
      const res = await fetch(`/api/connections/${conn.id}`)
      const data = await res.json()
      if (data.ok && data.data.credentials) {
        setCredentials(normalizeCredentialsForDb(conn.db_type, data.data.credentials))
      } else {
        console.error('Failed to load credentials:', data.message)
      }
    } catch (err) {
      console.error('Failed to load connection details:', err)
    } finally {
      setLoading(false)
      setView('form')
    }
  }

  const handleUploadConnection = () => {
    // Show the custom popup instead of alert
    setShowUploadPopup(true);
  }
  
  const initiateFileUpload = async () => {
    // Close the popup
    setShowUploadPopup(false);
    
    // Create a hidden file input
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = '.txt';
    
    input.onchange = async (e) => {
      const file = (e.target as HTMLInputElement).files?.[0];
      if (!file) return;
      setUploadStatus({ state: 'selected', message: `Selected ${file.name}` })
      
      // Show user that only .txt files are supported
      if (!file.name.toLowerCase().endsWith('.txt')) {
        alert('Only .txt files are supported');
        return;
      }
      
      // Store the file and show the connection name modal
      setPendingFile(file);
      setConnectionNameInput(''); // Reset input
      setShowConnectionNameModal(true);
    };
    
    // Show the file picker
    input.click();
  };
  
  const handleConnectionNameSubmit = async () => {
    if (!pendingFile || !connectionNameInput.trim()) {
      alert('Connection name is required');
      return;
    }
    
    // Create FormData to send the file and name
    const trimmedName = connectionNameInput.trim()
    const formData = new FormData();
    formData.append('file', pendingFile);
    formData.append('name', trimmedName);
    
    try {
      setUploadStatus({ state: 'uploading', message: `Uploading ${pendingFile.name}...` })
      const res = await fetch(`/api/connections/upload?name=${encodeURIComponent(trimmedName)}`, {
        method: 'POST',
        body: formData,
      });
      
      // Check if the response is ok at the HTTP level
      if (!res.ok) {
        throw new Error(`HTTP error! status: ${res.status}`);
      }
      
      // Attempt to parse JSON response
      let data;
      try {
        data = await res.json();
      } catch (parseErr) {
        console.error('Failed to parse JSON response:', parseErr);
        throw new Error('Invalid response format from server');
      }
      
      if (data.ok) {
        showSuccessMessage('Connection established successfully!')
        loadConnections(); // Refresh the connection list
        onSaved(); // Trigger any saved callbacks
        setUploadStatus({ state: 'success', message: `Uploaded ${pendingFile.name} successfully` })
        setTimeout(() => setUploadStatus({ state: 'idle' }), 3000)
      } else {
        const errorMessage = data.message || 'Unknown error occurred';
        setUploadStatus({ state: 'failed', message: `Failed: ${errorMessage}` })
        alert(`Failed to upload connection: ${errorMessage}`);
        setTimeout(() => setUploadStatus({ state: 'idle' }), 3000)
      }
    } catch (err) {
      console.error('Upload failed:', err);
      setUploadStatus({ state: 'failed', message: 'Upload failed' })
      alert('Upload failed');
      setTimeout(() => setUploadStatus({ state: 'idle' }), 3000)
    }
    
    // Close the modal and reset
    setShowConnectionNameModal(false);
    setPendingFile(null);
    setConnectionNameInput('');
  };
  
  const cancelUpload = () => {
    // Close the popup without initiating upload
    setShowUploadPopup(false);
    setUploadStatus({ state: 'idle' })
  };
  
  const renderFields = () => {
    switch (dbType) {
      case 'PostgreSQL':
        return (
          <>
            <input placeholder="Host" className="input-modern mb-2" value={credentials.host || ''} onChange={e => setCredentials({...credentials, host: e.target.value})} />
            <input placeholder="Port (5432)" className="input-modern mb-2" value={credentials.port || ''} onChange={e => setCredentials({...credentials, port: e.target.value})} />
            <input placeholder="Database" className="input-modern mb-2" value={credentials.database || ''} onChange={e => setCredentials({...credentials, database: e.target.value})} />
            <input placeholder="Username" className="input-modern mb-2" value={credentials.username || ''} onChange={e => setCredentials({...credentials, username: e.target.value})} />
            <input type="password" placeholder="Password" className="input-modern mb-2" value={credentials.password || ''} onChange={e => setCredentials({...credentials, password: e.target.value})} />
            <select
              className="input-modern mb-2"
              value={credentials.sslmode || 'require'}
              onChange={e => setCredentials({...credentials, sslmode: e.target.value})}
            >
              <option value="require">SSL: Required</option>
              <option value="disable">SSL: Not Required</option>
            </select>
          </>
        )
      case 'MySQL':
        return (
          <>
            <input placeholder="Host" className="input-modern mb-2" value={credentials.host || ''} onChange={e => setCredentials({...credentials, host: e.target.value})} />
            <input placeholder="Port (3306)" className="input-modern mb-2" value={credentials.port || ''} onChange={e => setCredentials({...credentials, port: e.target.value})} />
            <input placeholder="Username" className="input-modern mb-2" value={credentials.username || ''} onChange={e => setCredentials({...credentials, username: e.target.value})} />
            <input type="password" placeholder="Password" className="input-modern mb-2" value={credentials.password || ''} onChange={e => setCredentials({...credentials, password: e.target.value})} />
            <select className="input-modern mb-2" value={credentials.ssl === true ? 'true' : 'false'} onChange={e => setCredentials({...credentials, ssl: e.target.value === 'true'})}>
              <option value="false">SSL: Disabled</option>
              <option value="true">SSL: Enabled</option>
            </select>
          </>
        )
      case 'Snowflake':
        return (
          <>
            <input placeholder="Account" className="input-modern mb-2" value={credentials.account || ''} onChange={e => setCredentials({...credentials, account: e.target.value})} />
            <input placeholder="User" className="input-modern mb-2" value={credentials.username || ''} onChange={e => setCredentials({...credentials, username: e.target.value})} />
            <input type="password" placeholder="Password" className="input-modern mb-2" value={credentials.password || ''} onChange={e => setCredentials({...credentials, password: e.target.value})} />
            <input placeholder="Warehouse" className="input-modern mb-2" value={credentials.warehouse || ''} onChange={e => setCredentials({...credentials, warehouse: e.target.value})} />
            <input placeholder="Database (default: SNOWFLAKE)" className="input-modern mb-2" value={credentials.database || ''} onChange={e => setCredentials({...credentials, database: e.target.value})} />
            <input placeholder="Schema (default: PUBLIC)" className="input-modern mb-2" value={credentials.schema || ''} onChange={e => setCredentials({...credentials, schema: e.target.value})} />
          </>
        )
      case 'Databricks':
        return (
          <>
            <input placeholder="Server Hostname" className="input-modern mb-2" value={credentials.server_hostname || ''} onChange={e => setCredentials({...credentials, server_hostname: e.target.value})} />
            <input placeholder="HTTP Path" className="input-modern mb-2" value={credentials.http_path || ''} onChange={e => setCredentials({...credentials, http_path: e.target.value})} />
            <input type="password" placeholder="Access Token" className="input-modern mb-2" value={credentials.access_token || ''} onChange={e => setCredentials({...credentials, access_token: e.target.value})} />
            <input placeholder="Catalog (optional)" className="input-modern mb-2" value={credentials.catalog || ''} onChange={e => setCredentials({...credentials, catalog: e.target.value})} />
            <input placeholder="Schema (optional)" className="input-modern mb-2" value={credentials.schema || ''} onChange={e => setCredentials({...credentials, schema: e.target.value})} />
          </>
        )
      case 'Oracle':
        return (
          <>
            <input placeholder="Host" className="input-modern mb-2" value={credentials.host || ''} onChange={e => setCredentials({...credentials, host: e.target.value})} />
            <input placeholder="Port (1521)" className="input-modern mb-2" value={credentials.port || ''} onChange={e => setCredentials({...credentials, port: e.target.value})} />
            <input placeholder="Service Name" className="input-modern mb-2" value={credentials.service_name || ''} onChange={e => setCredentials({...credentials, service_name: e.target.value})} />
            <input placeholder="Username" className="input-modern mb-2" value={credentials.username || ''} onChange={e => setCredentials({...credentials, username: e.target.value})} />
            <input type="password" placeholder="Password" className="input-modern mb-2" value={credentials.password || ''} onChange={e => setCredentials({...credentials, password: e.target.value})} />
            <input placeholder="Schema Name (optional)" className="input-modern mb-2" value={credentials.schema || ''} onChange={e => setCredentials({...credentials, schema: e.target.value})} />
          </>
        )
      case 'SQL Server':
        return (
          <>
            <input placeholder="Host" className="input-modern mb-2" value={credentials.host || ''} onChange={e => setCredentials({...credentials, host: e.target.value})} />
            <input placeholder="Port (1433)" className="input-modern mb-2" value={credentials.port || ''} onChange={e => setCredentials({...credentials, port: e.target.value})} />
            <input placeholder="Database" className="input-modern mb-2" value={credentials.database || ''} onChange={e => setCredentials({...credentials, database: e.target.value})} />
            <input placeholder="Username" className="input-modern mb-2" value={credentials.username || ''} onChange={e => setCredentials({...credentials, username: e.target.value})} />
            <input type="password" placeholder="Password" className="input-modern mb-2" value={credentials.password || ''} onChange={e => setCredentials({...credentials, password: e.target.value})} />
            <input placeholder="Driver (ODBC Driver 18 for SQL Server)" className="input-modern mb-2" value={credentials.driver || ''} onChange={e => setCredentials({...credentials, driver: e.target.value})} />
          </>
        )
      case 'Teradata':
        return (
          <>
            <input placeholder="Host" className="input-modern mb-2" value={credentials.host || ''} onChange={e => setCredentials({...credentials, host: e.target.value})} />
            <input placeholder="Username" className="input-modern mb-2" value={credentials.username || ''} onChange={e => setCredentials({...credentials, username: e.target.value})} />
            <input type="password" placeholder="Password" className="input-modern mb-2" value={credentials.password || ''} onChange={e => setCredentials({...credentials, password: e.target.value})} />
            <input placeholder="Database (optional)" className="input-modern mb-2" value={credentials.database || ''} onChange={e => setCredentials({...credentials, database: e.target.value})} />
          </>
        )
      case 'Google BigQuery':
        return (
          <>
            <input placeholder="Project ID" className="input-modern mb-2" value={credentials.project_id || ''} onChange={e => setCredentials({...credentials, project_id: e.target.value})} />
            <input placeholder="Dataset (optional)" className="input-modern mb-2" value={credentials.dataset || ''} onChange={e => setCredentials({...credentials, dataset: e.target.value})} />
            <textarea placeholder="Service Account JSON (optional, leave empty for ADC)" rows={4} className="input-modern mb-2" value={credentials.credentials_json || ''} onChange={e => setCredentials({...credentials, credentials_json: e.target.value})} />
          </>
        )
      case 'AWS S3':
        return (
          <>
            <input placeholder="Bucket Name" className="input-modern mb-2" value={credentials.bucket_name || ''} onChange={e => setCredentials({...credentials, bucket_name: e.target.value})} />
            <input placeholder="Region (e.g., us-east-1)" className="input-modern mb-2" value={credentials.region || ''} onChange={e => setCredentials({...credentials, region: e.target.value})} />
            <input placeholder="Access Key ID" className="input-modern mb-2" value={credentials.access_key_id || ''} onChange={e => setCredentials({...credentials, access_key_id: e.target.value})} />
            <input type="password" placeholder="Secret Access Key" className="input-modern mb-2" value={credentials.secret_access_key || ''} onChange={e => setCredentials({...credentials, secret_access_key: e.target.value})} />
          </>
        )
      default:
        return <div className="text-gray-500">Select a database type to see credential fields</div>
    }
  }

  const uploadPopup = showUploadPopup ? (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-[60] p-4">
      <div className="glass-card rounded-2xl p-6 w-[720px] shadow-glass-lg">
        <div className="mb-4 flex items-center justify-between">
          <div>
            <h3 className="text-lg font-bold text-gray-800">Upload Connection</h3>
            <p className="text-sm text-gray-600 mt-1">Only .txt files are supported for connection uploads.</p>
          </div>
          <button
            onClick={copyTemplate}
            className="flex items-center gap-2 px-3 py-1.5 rounded-lg border border-gray-200 text-sm text-gray-700 hover:bg-gray-50 transition-all"
            aria-label="Copy template"
          >
            <Copy size={16} />
            Copy
          </button>
        </div>
        
        <div className="grid grid-cols-[160px,1fr] gap-4 mb-6">
          <div className="space-y-2">
            {(['MySQL','Snowflake','Databricks','Oracle'] as const).map(type => (
              <button
                key={type}
                onClick={() => setUploadTemplateType(type)}
                className={`w-full text-left px-3 py-2 rounded-lg border ${
                  uploadTemplateType === type
                    ? 'border-primary-200 bg-primary-50 text-primary-700'
                    : 'border-gray-200 hover:bg-gray-50 text-gray-700'
                }`}
              >
                {type}
              </button>
            ))}
          </div>
          
          <div className="bg-gray-50 border border-gray-200 rounded-xl p-4">
            <p className="text-sm font-semibold text-gray-700 mb-2">Format</p>
            <pre className="bg-white border border-gray-200 p-3 rounded-lg text-xs overflow-x-auto whitespace-pre-wrap">
              {uploadTemplates[uploadTemplateType]}
            </pre>
          </div>
        </div>
        
        <p className="text-xs text-gray-500 mb-6">Supported keys include: database type, name, host, port, username, password, database, schema, account, warehouse, server hostname, http path, access token, service name, project id, dataset, bucket name, region, access key id, secret access key, sslmode, ssl, catalog, driver, credentials json.</p>
        
        <div className="flex justify-end space-x-3">
          <button
            onClick={cancelUpload}
            className="px-4 py-2 border-2 border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 font-medium transition-all"
          >
            Cancel
          </button>
          <button
            onClick={initiateFileUpload}
            className="px-4 py-2 btn-accent rounded-lg font-medium transition-all"
          >
            OK
          </button>
        </div>
      </div>
    </div>
  ) : null
  
  const connectionNameModal = showConnectionNameModal ? (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-[60] p-4">
      <div className="glass-card rounded-2xl p-6 w-[500px] shadow-glass-lg">
        <div className="mb-4">
          <h3 className="text-lg font-bold text-gray-800">Enter Connection Name</h3>
          <p className="text-sm text-gray-600 mt-1">Provide a name for your database connection.</p>
        </div>
        
        <div className="mb-6">
          <input
            type="text"
            value={connectionNameInput}
            onChange={(e) => setConnectionNameInput(e.target.value)}
            placeholder="My Database Connection"
            className="input-modern w-full"
            autoFocus
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                handleConnectionNameSubmit();
              } else if (e.key === 'Escape') {
                setShowConnectionNameModal(false);
                setPendingFile(null);
                setConnectionNameInput('');
              }
            }}
          />
        </div>
        
        <div className="flex justify-end space-x-3">
          <button
            onClick={() => {
              setShowConnectionNameModal(false);
              setPendingFile(null);
              setConnectionNameInput('');
            }}
            className="px-4 py-2 border-2 border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 font-medium transition-all"
          >
            Cancel
          </button>
          <button
            onClick={handleConnectionNameSubmit}
            className="px-4 py-2 btn-accent rounded-lg font-medium transition-all"
          >
            Save Connection
          </button>
        </div>
      </div>
    </div>
  ) : null

  const successBanner = successMessage ? (
    <div className="fixed top-6 left-1/2 -translate-x-1/2 z-[70] px-4 py-2 bg-green-600 text-white rounded-lg shadow-2xl flex items-center gap-3">
      <span>{successMessage.text}</span>
      <button
        type="button"
        onClick={clearSuccessMessage}
        className="text-white/80 hover:text-white focus:outline-none"
        aria-label="Close success message"
      >
        ×
      </button>
    </div>
  ) : null

  // Render upload status chip with timed auto-dismiss for success/failed
  const uploadStatusBadge = (() => {
    if (uploadStatus.state === 'idle') return null
    const color =
      uploadStatus.state === 'success'
        ? 'bg-green-50 text-green-700 border-green-200'
        : uploadStatus.state === 'failed'
          ? 'bg-red-50 text-red-700 border-red-200'
          : uploadStatus.state === 'uploading'
            ? 'bg-blue-50 text-blue-700 border-blue-200'
            : 'bg-gray-50 text-gray-700 border-gray-200'
    return (
      <div className={`inline-flex items-center gap-2 px-3 py-1 rounded-full border text-xs font-medium ${color}`}>
        <span className="w-2 h-2 rounded-full bg-current opacity-60"></span>
        <span>{uploadStatus.message || uploadStatus.state}</span>
      </div>
    )
  })()

  if (view === 'list') {
    return (
      <>
        {successBanner}
        {uploadPopup}
        {connectionNameModal}
        <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-50 p-4">
          <div className="glass-card rounded-2xl w-[900px] max-h-[80vh] overflow-auto shadow-glass-lg">
            <div className="sticky top-0 glass-card border-b border-white/30 p-6">
              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center space-x-3">
                  <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-primary-100 to-accent-100 flex items-center justify-center">
                    <Settings className="w-6 h-6 text-primary-600" />
                  </div>
                  <div>
                    <h2 className="text-2xl font-bold bg-gradient-to-r from-primary-600 to-accent-600 bg-clip-text text-transparent">Settings</h2>
                    <p className="text-sm text-gray-600">Configure your application preferences</p>
                  </div>
                </div>
                <button onClick={onClose} className="p-2 rounded-xl hover:bg-gray-100 text-gray-400 hover:text-gray-600 transition-all">
                  <X size={24} />
                </button>
              </div>
            </div>

            <div className="p-6">
              <div className="flex items-center justify-between mb-4">
                <div>
                  <h3 className="text-lg font-bold text-gray-800">Database Connections</h3>
                  <p className="text-sm text-gray-500">Manage your database connections for real-time query execution</p>
                </div>
                <div className="flex items-center space-x-3">
                  {uploadStatusBadge}
                  <button
                    onClick={() => setView('form')}
                    className="flex items-center space-x-2 btn-primary shadow-md"
                  >
                    <Plus size={18} />
                    <span>Add Connection</span>
                  </button>
                  <button
                    onClick={handleUploadConnection}
                    className="flex items-center space-x-2 btn-accent shadow-md"
                  >
                    <span>Upload Connection</span>
                  </button>
                </div>
              </div>

              <div className="space-y-3">
                {connections.length === 0 ? (
                  <div className="text-center py-16 text-gray-500">
                    <div className="w-20 h-20 mx-auto mb-4 rounded-2xl bg-gradient-to-br from-primary-50 to-accent-50 flex items-center justify-center">
                      <Database className="w-10 h-10 text-primary-400" />
                    </div>
                    <p className="font-semibold text-gray-700 mb-1">No database connections yet</p>
                    <p className="text-sm text-gray-500">Click "Add Connection" to create your first connection</p>
                  </div>
                ) : (
                  connections.map(conn => (
                    <div key={conn.id} className="card-modern p-4 hover:shadow-glass transition-all">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center space-x-4 flex-1 min-w-0">
                          {getDatabaseIcon(conn.db_type) ? (
                            <img src={getDatabaseIcon(conn.db_type)} alt="" className="w-5 h-5 flex-shrink-0" />
                          ) : (
                            <Database className="w-5 h-5 text-[#085690] flex-shrink-0" />
                          )}
                          <div className="flex-1 grid grid-cols-3 gap-4 min-w-0">
                            <div className="min-w-0">
                              <p className="font-semibold text-gray-800 line-clamp-2 break-words">{conn.name}</p>
                              <p className="text-xs text-gray-500 line-clamp-1 break-words">Type: {getDatabaseDisplayName(conn.db_type)}</p>
                            </div>
                            <div>
                              <p className="text-xs text-gray-500">Host:</p>
                              <p className="text-sm text-gray-700">***</p>
                            </div>
                            <div>
                              <p className="text-xs text-gray-500">Created:</p>
                              <p className="text-sm text-gray-700">{new Date(conn.created_at).toLocaleDateString()}</p>
                            </div>
                          </div>
                        </div>
                        <div className="flex items-center space-x-2 flex-shrink-0">
                          <button
                            onClick={() => editConnection(conn)}
                            className="p-2 rounded-lg text-gray-400 hover:text-primary-600 hover:bg-primary-50 transition-all"
                          >
                            <Edit2 size={16} />
                          </button>
                          <button
                            onClick={() => deleteConnection(conn.id)}
                            className="p-2 rounded-lg text-gray-400 hover:text-accent-600 hover:bg-accent-50 transition-all"
                          >
                            <Trash2 size={16} />
                          </button>
                        </div>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>
        </div>
      </>
    )
  }

  return (
    <>
    {successBanner}
    {uploadPopup}
    
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-50 p-4">
      <div className="glass-card rounded-2xl p-8 w-[600px] max-h-[80vh] overflow-auto shadow-glass-lg">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-2xl font-bold bg-gradient-to-r from-primary-600 to-accent-600 bg-clip-text text-transparent">
            {editingId ? 'Edit Database Connection' : 'Add Database Connection'}
          </h2>
          <button onClick={() => { resetForm(); setView('list'); }} className="p-2 rounded-xl hover:bg-gray-100 text-gray-400 hover:text-gray-600 transition-all">
            <X size={24} />
          </button>
        </div>
        
        <div className="space-y-4">
          {loading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
              <span className="ml-3 text-gray-600">Loading connection details...</span>
            </div>
          ) : (
            <>
              <div>
                <label className="block text-sm font-semibold mb-2 text-primary-700">Connection Name</label>
                <input 
                  value={name}
                  onChange={e => setName(e.target.value)}
                  className="input-modern" 
                  placeholder="My Database" 
                />
              </div>
              
              <div>
                <label className="block text-sm font-semibold mb-2 text-primary-700">Database Type</label>
                <div className="relative db-type-dropdown">
                  <button 
                    type="button"
                    className="input-modern w-full flex items-center justify-between"
                    onClick={() => setShowDbTypeDropdown(!showDbTypeDropdown)}
                  >
                    <div className="flex items-center">
                      {dbType && (
                        <>
                          {getDatabaseIcon(dbType) && (
                            <img src={getDatabaseIcon(dbType)} alt="" className="w-5 h-5 mr-2" />
                          )}
                          <span>{getDatabaseDisplayName(dbType)}</span>
                        </>
                      )}
                      {!dbType && (
                        <span className="text-gray-500">Select type...</span>
                      )}
                    </div>
                    <ChevronDown className="w-4 h-4 text-gray-400" />
                  </button>
                  
                      {showDbTypeDropdown && (
                    <div className="absolute z-10 mt-1 w-full bg-white border border-gray-300 rounded-lg shadow-lg max-h-60 overflow-auto">
                      {DB_TYPES.map(type => {
                        return (
                          <div
                            key={type}
                            className="flex items-center px-4 py-2 hover:bg-gray-100 cursor-pointer"
                            onClick={() => {
                              setDbType(type);
                              // Pre-fill sensible defaults for Snowflake so database/schema are not empty
                              if (type === 'Snowflake') {
                                setCredentials((prev: any) => ({
                                  ...prev,
                                  database: prev?.database ?? 'SNOWFLAKE',
                                  schema: prev?.schema ?? 'PUBLIC'
                                }));
                              }
                              setShowDbTypeDropdown(false);
                            }}
                          >
                            {getDatabaseIcon(type) && (
                              <img src={getDatabaseIcon(type)} alt="" className="w-5 h-5 mr-2" />
                            )}
                            <span>{getDatabaseDisplayName(type)}</span>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              </div>

              {dbType && (
                <div>
                  <label className="block text-sm font-semibold mb-2 text-primary-700">Credentials</label>
                  {renderFields()}
                </div>
              )}
            </>
          )}

          {testMessage && (
            <div className={`p-4 rounded-xl border-l-4 ${
              testMessage.type === 'success' 
                ? 'bg-primary-50 border-primary-500 text-primary-700' 
                : 'bg-accent-50 border-accent-500 text-accent-700'
            }`}>
              <p className="text-sm font-semibold">{testMessage.text}</p>
            </div>
          )}
          
          <div className="flex gap-3 pt-4 border-t border-gray-200">
            <button
              onClick={testConnection}
              disabled={!dbType || testing}
              className={`px-6 py-2.5 rounded-lg font-medium transition-all ${
                !dbType || testing
                  ? 'bg-gray-200 text-gray-500 cursor-not-allowed'
                  : 'btn-primary'
              }`}
            >
              {testing ? 'Testing...' : 'Test Connection'}
            </button>
            
            <button
              onClick={saveConnection}
              disabled={!tested}
              className={`px-6 py-2.5 rounded-lg font-medium transition-all ${
                !tested
                  ? 'bg-gray-200 text-gray-500 cursor-not-allowed'
                  : 'btn-accent'
              }`}
            >
              Save Connection
            </button>
            
            <button
              onClick={() => setView('list')}
              className="px-6 py-2.5 border-2 border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 font-medium transition-all"
            >
              Cancel
            </button>
          </div>
        </div>
      </div>
    </div>
    </>
  )
}
