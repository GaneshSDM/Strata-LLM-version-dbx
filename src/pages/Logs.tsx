import { useState, useEffect } from 'react';
import { FileText, RefreshCw, Download, AlertCircle, Terminal, Activity, Clock, AlertTriangle, Info, XCircle, Zap } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

export default function Logs() {
  const [logs, setLogs] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState<string>('ALL');
  const refreshInterval = 5000;

  const fetchLogs = async () => {
    try {
      const response = await fetch('/api/logs');
      const data = await response.json();
      if (data.ok) {
        setLogs(data.data);
      } else {
        setError(data.message || 'Failed to fetch logs');
      }
    } catch (err) {
      setError('Failed to connect to backend');
      console.error('Log fetch error:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchLogs();
    
    const interval = setInterval(fetchLogs, refreshInterval);
    return () => clearInterval(interval);
  }, [refreshInterval]);

  const handleRefresh = () => {
    setLoading(true);
    fetchLogs();
  };

  const handleExport = () => {
    const logText = logs.map(log => 
      `${log.timestamp || ''} [${log.level || 'INFO'}] ${log.message || ''}`
    ).join('\n');
    
    const blob = new Blob([logText], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `strata-logs-${new Date().toISOString().split('T')[0]}.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const getLogIcon = (level: string) => {
    switch (level) {
      case 'ERROR':
        return <XCircle size={18} className="text-red-600" />;
      case 'WARNING':
        return <AlertTriangle size={18} className="text-yellow-600" />;
      case 'DEBUG':
        return <Info size={18} className="text-blue-600" />;
      default:
        return <Activity size={18} className="text-green-600" />;
    }
  };


  const getBadgeStyle = (level: string) => {
    switch (level) {
      case 'ERROR':
        return 'bg-red-500 text-white';
      case 'WARNING':
        return 'bg-yellow-500 text-white';
      case 'DEBUG':
        return 'bg-blue-500 text-white';
      default:
        return 'bg-green-500 text-white';
    }
  };

  const filteredLogs = filter === 'ALL' 
    ? logs 
    : logs.filter(log => log.level === filter);

  const logCounts = {
    ERROR: logs.filter(l => l.level === 'ERROR').length,
    WARNING: logs.filter(l => l.level === 'WARNING').length,
    DEBUG: logs.filter(l => l.level === 'DEBUG').length,
    INFO: logs.filter(l => !l.level || l.level === 'INFO').length,
  };

  return (
    <div className="max-w-7xl mx-auto">
      <motion.div
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="mb-8"
      >
        <div className="flex justify-between items-center mb-4">
          <div>
            <h1 className="text-4xl font-bold bg-gradient-to-r from-primary-600 to-accent-600 bg-clip-text text-transparent mb-2">
              System Logs
            </h1>
            <p className="text-gray-600">Real-time backend activity monitoring</p>
          </div>
          <div className="flex gap-3">
            <motion.button
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
              onClick={handleRefresh}
              disabled={loading}
              className="flex items-center gap-2 btn-primary shadow-md"
            >
              <RefreshCw size={18} className={loading ? 'animate-spin' : ''} />
              Refresh
            </motion.button>
            <motion.button
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
              onClick={handleExport}
              className="flex items-center gap-2 btn-accent shadow-md"
            >
              <Download size={18} />
              Export
            </motion.button>
          </div>
        </div>

        {/* Log level filter tabs */}
        <div className="flex gap-2 mb-6">
          {['ALL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'].map((level) => (
            <motion.button
              key={level}
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
              onClick={() => setFilter(level)}
              className={`px-4 py-2 rounded-lg font-semibold text-sm transition-all ${
                filter === level
                  ? 'bg-gradient-to-r from-primary-500 to-accent-500 text-white shadow-md'
                  : 'bg-white text-gray-700 hover:bg-gray-50 border border-gray-200'
              }`}
            >
              {level}
              {level !== 'ALL' && (
                <span className="ml-2 px-2 py-0.5 rounded-full bg-white/20 text-xs">
                  {logCounts[level as keyof typeof logCounts]}
                </span>
              )}
            </motion.button>
          ))}
        </div>
      </motion.div>

      <AnimatePresence>
        {error && (
          <motion.div
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            className="mb-6 glass-card rounded-xl p-4 border-l-4 border-red-500 flex items-start gap-3"
          >
            <div className="p-2 rounded-lg bg-red-100">
              <AlertCircle className="text-red-600" size={20} />
            </div>
            <div className="flex-1">
              <h4 className="font-bold text-red-900 mb-1">Connection Error</h4>
              <p className="text-sm text-red-700">{error}</p>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        className="card-glass p-6 relative overflow-hidden"
      >
        <div className="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-primary-500 to-accent-500" />
        
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center gap-3">
            <div className="p-3 rounded-xl bg-gradient-to-br from-primary-100 to-accent-100">
              <Terminal className="text-primary-600" size={24} />
            </div>
            <div>
              <h2 className="text-xl font-bold text-gray-900">Backend Activity Log</h2>
              <div className="flex items-center gap-2 text-sm text-gray-600 mt-1">
                <div className="status-online" />
                <span className="font-medium">Live Updates</span>
                <span className="text-xs text-gray-500">• Auto-refresh: {refreshInterval / 1000}s</span>
              </div>
            </div>
          </div>
          
          <div className="flex items-center gap-4 text-sm">
            <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-gray-100">
              <Clock size={14} className="text-gray-600" />
              <span className="font-semibold text-gray-700">{filteredLogs.length} entries</span>
            </div>
          </div>
        </div>

        <div className="border border-gray-200 rounded-xl overflow-hidden bg-gray-900">
          <div className="bg-gradient-to-r from-gray-800 to-gray-900 px-6 py-3 border-b border-gray-700 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <div className="flex gap-1.5">
                <div className="w-3 h-3 rounded-full bg-red-500" />
                <div className="w-3 h-3 rounded-full bg-yellow-500" />
                <div className="w-3 h-3 rounded-full bg-green-500" />
              </div>
              <span className="text-sm font-mono text-gray-400 ml-3">system.log</span>
            </div>
            <div className="flex items-center gap-2">
              <Zap size={14} className="text-green-400 animate-pulse" />
              <span className="text-xs font-mono text-green-400">LIVE</span>
            </div>
          </div>
          
          <div className="max-h-[600px] overflow-y-auto bg-gray-950 scrollbar-thin scrollbar-thumb-gray-700 scrollbar-track-gray-900">
            {loading ? (
              <div className="p-12 text-center">
                <motion.div
                  animate={{ rotate: 360 }}
                  transition={{ duration: 1, repeat: Infinity, ease: 'linear' }}
                  className="inline-block"
                >
                  <RefreshCw className="text-primary-400" size={32} />
                </motion.div>
                <p className="text-gray-400 mt-4 font-mono text-sm">Loading logs...</p>
              </div>
            ) : filteredLogs.length === 0 ? (
              <div className="p-12 text-center">
                <FileText className="mx-auto mb-4 text-gray-600" size={48} />
                <p className="text-gray-400 font-mono text-sm">No log entries found</p>
              </div>
            ) : (
              <div className="divide-y divide-gray-800">
                {filteredLogs.map((log, index) => (
                  <motion.div
                    key={index}
                    initial={{ opacity: 0, x: -20 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ delay: index * 0.02 }}
                    className="p-4 hover:bg-gray-900/50 transition-all group"
                  >
                    <div className="flex items-start gap-4">
                      <div className="flex-shrink-0 mt-1">
                        {getLogIcon(log.level)}
                      </div>
                      
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-3 mb-2">
                          <span className="text-xs font-mono text-gray-500 flex items-center gap-1.5">
                            <Clock size={12} />
                            {log.timestamp || 'Unknown time'}
                          </span>
                          <span className={`px-3 py-1 rounded-full text-xs font-bold ${getBadgeStyle(log.level)}`}>
                            {log.level || 'INFO'}
                          </span>
                        </div>
                        
                        <p className="text-sm font-mono text-gray-300 leading-relaxed break-all">
                          <span className="text-green-400">$</span> {log.message || 'No message'}
                        </p>
                        
                        {log.details && (
                          <p className="text-xs font-mono text-gray-500 mt-2 pl-4 border-l-2 border-gray-700">
                            {log.details}
                          </p>
                        )}
                      </div>
                    </div>
                  </motion.div>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Stats footer */}
        <div className="mt-6 grid grid-cols-4 gap-4">
          {Object.entries(logCounts).map(([level, count]) => (
            <div key={level} className="p-3 rounded-lg bg-gray-50 border border-gray-200">
              <div className="flex items-center justify-between">
                <span className="text-xs font-semibold text-gray-600">{level}</span>
                <span className="text-lg font-bold text-gray-900">{count}</span>
              </div>
            </div>
          ))}
        </div>
      </motion.div>
    </div>
  );
}