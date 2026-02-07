import { useState, useEffect } from 'react';
import { FileText, RefreshCw, Download, AlertCircle, X, Activity, AlertTriangle, Info, XCircle } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

type LogEntry = {
  timestamp?: string;
  level?: string;
  message?: string;
  details?: string;
};

type LogsModalProps = {
  isOpen: boolean;
  onClose: () => void;
};

export function LogsModal({ isOpen, onClose }: LogsModalProps) {
  const [logs, setLogs] = useState<LogEntry[]>([]);
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
    if (isOpen) {
      setLoading(true);
      fetchLogs();
      
      const interval = setInterval(fetchLogs, refreshInterval);
      return () => clearInterval(interval);
    }
  }, [isOpen, refreshInterval]);

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

  if (!isOpen) return null;

  return (
    <AnimatePresence>
      <div className="fixed inset-0 z-50 overflow-hidden">
        {/* Backdrop */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          className="absolute inset-0 bg-black/50 backdrop-blur-sm"
          onClick={onClose}
        />
        
        {/* Modal - File/Document Shape */}
        <div className="absolute inset-0 flex items-center justify-center p-8">
          <motion.div
            initial={{ opacity: 0, scale: 0.9, y: 30 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.9, y: 30 }}
            transition={{ type: 'spring', damping: 25, stiffness: 300 }}
            className="relative w-full max-w-4xl max-h-[85vh] flex flex-col"
          >
            {/* File tab/corner fold effect */}
            <div className="absolute -top-1 right-12 w-16 h-8 bg-white rounded-t-lg shadow-lg border-t-2 border-x-2 border-primary-500" />
            
            {/* Main file container */}
            <div className="bg-white rounded-lg shadow-2xl flex flex-col overflow-hidden border-2 border-gray-200 relative">
              {/* File header stripe */}
              <div className="absolute top-0 left-0 right-0 h-1 bg-gradient-to-r from-primary-500 via-accent-500 to-primary-500" />
            
              {/* Simple file header */}
              <div className="px-8 py-6 border-b border-gray-200 bg-gradient-to-b from-gray-50 to-white">
                <div className="flex justify-between items-center mb-4">
                  <div className="flex items-center gap-3">
                    <div className="p-2.5 rounded-lg bg-gradient-to-br from-primary-100 to-accent-100">
                      <FileText className="text-primary-600" size={24} />
                    </div>
                    <div>
                      <h3 className="text-xl font-bold text-gray-900">system.log</h3>
                      <p className="text-xs text-gray-500 mt-0.5">Backend Activity Logs</p>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <motion.button
                      whileHover={{ scale: 1.05 }}
                      whileTap={{ scale: 0.95 }}
                      onClick={handleRefresh}
                      disabled={loading}
                      className="p-2 text-gray-600 hover:bg-gray-100 rounded-lg transition-all"
                      title="Refresh"
                    >
                      <RefreshCw size={18} className={loading ? 'animate-spin' : ''} />
                    </motion.button>
                    <motion.button
                      whileHover={{ scale: 1.05 }}
                      whileTap={{ scale: 0.95 }}
                      onClick={handleExport}
                      className="p-2 text-gray-600 hover:bg-gray-100 rounded-lg transition-all"
                      title="Export"
                    >
                      <Download size={18} />
                    </motion.button>
                    <motion.button
                      whileHover={{ scale: 1.1 }}
                      whileTap={{ scale: 0.9 }}
                      onClick={onClose}
                      className="p-2 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-all"
                      title="Close"
                    >
                      <X size={20} />
                    </motion.button>
                  </div>
                </div>

                {/* Simple filter pills */}
                <div className="flex gap-2 flex-wrap">
                  {['ALL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'].map((level) => (
                    <button
                      key={level}
                      onClick={() => setFilter(level)}
                      className={`px-3 py-1.5 rounded-full text-xs font-semibold transition-all ${
                        filter === level
                          ? 'bg-gradient-to-r from-primary-500 to-accent-500 text-white'
                          : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                      }`}
                    >
                      {level}
                      {level !== 'ALL' && (
                        <span className={`ml-1.5 ${filter === level ? 'opacity-80' : 'opacity-60'}`}>
                          ({logCounts[level as keyof typeof logCounts]})
                        </span>
                      )}
                    </button>
                  ))}
                </div>

                {error && (
                  <div className="mt-3 p-3 bg-red-50 border border-red-200 rounded-lg flex items-center gap-2">
                    <AlertCircle className="text-red-500" size={16} />
                    <span className="text-red-700 text-xs font-medium">{error}</span>
                  </div>
                )}
              </div>

              {/* Simple log content area */}
              <div className="flex-1 overflow-hidden bg-white">
                <div className="h-full overflow-y-auto px-6 py-4">
                  {loading ? (
                    <div className="py-16 text-center">
                      <RefreshCw className="animate-spin mx-auto mb-3 text-primary-500" size={28} />
                      <p className="text-gray-500 text-sm">Loading logs...</p>
                    </div>
                  ) : filteredLogs.length === 0 ? (
                    <div className="py-16 text-center">
                      <FileText className="mx-auto mb-3 text-gray-300" size={40} />
                      <p className="text-gray-500 text-sm">No log entries found</p>
                    </div>
                  ) : (
                    <div className="space-y-2">
                      {filteredLogs.map((log, index) => (
                        <motion.div
                          key={index}
                          initial={{ opacity: 0, y: 10 }}
                          animate={{ opacity: 1, y: 0 }}
                          transition={{ delay: index * 0.03 }}
                          className="p-3 rounded-lg hover:bg-gray-50 transition-all border border-transparent hover:border-gray-200"
                        >
                          <div className="flex items-start gap-3">
                            <div className="flex-shrink-0 mt-0.5">
                              {getLogIcon(log.level || 'INFO')}
                            </div>
                            
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center gap-2 mb-1.5">
                                <span className="text-xs text-gray-500 font-medium">
                                  {log.timestamp || 'Unknown time'}
                                </span>
                                <span className={`px-2 py-0.5 rounded-md text-xs font-bold ${getBadgeStyle(log.level || 'INFO')}`}>
                                  {log.level || 'INFO'}
                                </span>
                              </div>
                              
                              <p className="text-sm text-gray-800 leading-relaxed">
                                {log.message || 'No message'}
                              </p>
                              
                              {log.details && (
                                <p className="text-xs text-gray-600 mt-1.5 pl-3 border-l-2 border-gray-300">
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

              {/* Simple footer */}
              <div className="px-8 py-4 border-t border-gray-200 bg-gray-50 flex items-center justify-between">
                <div className="flex items-center gap-2 text-xs text-gray-600">
                  <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
                  <span>Live • Auto-refresh: {refreshInterval / 1000}s</span>
                </div>
                <div className="flex gap-4">
                  {Object.entries(logCounts).map(([level, count]) => (
                    <div key={level} className="flex items-center gap-1.5">
                      <span className="text-xs font-medium text-gray-500">{level}:</span>
                      <span className="text-xs font-bold text-gray-700">{count}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </motion.div>
        </div>
      </div>
    </AnimatePresence>
  );
}