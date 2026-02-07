import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { ArrowLeft, CheckCircle, XCircle, AlertCircle, Loader2, FileJson, FileSpreadsheet, FileText, TrendingUp, Database, FileCheck, Activity, BarChart3, PieChart, Terminal } from 'lucide-react'
import { motion, AnimatePresence } from 'framer-motion'
import { useWizard } from '../components/WizardContext'

interface ValidationCheck {
  category: string
  status: string
  errorDetails: string
  suggestedFix: string
  confidence: number
}

interface TableComparison {
  table: string
  source_rows: number
  target_rows: number
  status: string
  accuracy: string
}

interface ValidationSummary {
  total_tables: number
  tables_matched: number
  total_checks: number
  checks_passed: number
  checks_failed: number
  overall_accuracy: number
}

interface ValidationReport {
  checks: ValidationCheck[]
  table_comparisons: TableComparison[]
  summary: ValidationSummary
  timestamp: string
}

type ReconcileProps = {
  setShowLogsModal: (show: boolean) => void;
};

export default function Reconcile({ setShowLogsModal }: ReconcileProps) {
  const { wizardResetId } = useWizard()
  const [report, setReport] = useState<ValidationReport | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const navigate = useNavigate()

  const getDefaultDetailsForCheck = (category: string): string => {
    if (category.includes('Row Count')) {
      return 'Source and target row counts match exactly'
    } else if (category.includes('Column Count')) {
      return 'Source and target have same number of columns'
    } else if (category.includes('Column Presence')) {
      return 'All source columns are present in target'
    } else if (category.includes('Datatype Match')) {
      return 'Column datatypes are compatible between source and target'
    } else if (category.includes('Length/Size Match')) {
      return 'Column lengths/sizes are compatible'
    } else if (category.includes('Precision/Scale Check')) {
      return 'Numeric precision and scale are compatible'
    } else if (category.includes('Nullability Constraint Check')) {
      return 'Nullability constraints match between source and target'
    } else if (category.includes('Primary Key Check')) {
      return 'Primary key constraints match'
    } else if (category.includes('Foreign Key Check')) {
      return 'Foreign key constraints match'
    } else if (category.includes('Unique Keys')) {
      return 'Unique key constraints match'
    } else if (category.includes('Index Comparison')) {
      return 'Indexes are properly migrated'
    } else if (category.includes('Default Values')) {
      return 'Column default values match'
    } else if (category.includes('Encoding Check')) {
      return 'Character encoding is compatible (UTF-8)'
    } else if (category.includes('View Definition Check')) {
      return 'View definitions are compatible'
    } else if (category.includes('Stored Procedure/Object Count Check')) {
      return 'Stored procedures and objects count match'
    } else if (category.includes('Schema Name Mapping')) {
      return 'Schema names are properly mapped'
    } else if (category.includes('Data Type Compatibility Rules')) {
      return 'Data type compatibility rules applied successfully'
    } else if (category.includes('Data Integrity Check')) {
      return 'Row-level data integrity verified'
    } else {
      return 'Validation check passed'
    }
  }
  useEffect(() => {
    setReport(null)
    setLoading(false)
    setError(null)
  }, [wizardResetId])

  const runValidation = async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await fetch('/api/validate/run', { method: 'POST' })
      const data = await res.json()
      if (data.ok) {
        setReport(data.data)
      } else {
        setError(data.message || 'Validation failed')
      }
    } catch (err: any) {
      setError(err.message || 'Network error during validation')
    } finally {
      setLoading(false)
    }
  }

  const exportFile = (format: string) => {
    window.open(`/api/export/${format}`, '_blank')
  }

  return (
    <div className="max-w-7xl mx-auto">
      <motion.div
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="mb-8"
      >
        <div className="flex justify-between items-start">
          <div>
            <h1 className="text-4xl font-bold bg-gradient-to-r from-primary-600 to-accent-600 bg-clip-text text-transparent mb-2">
              Validation & Reconciliation
            </h1>
            <p className="text-gray-600">Comprehensive post-migration data validation and integrity checks</p>
          </div>
          
          <div className="flex items-center gap-3">
            <motion.button
              whileHover={{ scale: 1.05, y: -2 }}
              whileTap={{ scale: 0.95 }}
              onClick={() => navigate('/migrate')}
              className="px-5 py-3 rounded-xl border-2 border-primary-200 text-primary-700 bg-white/80 shadow-sm font-semibold flex items-center gap-2"
              title="Back to Migration"
            >
              <ArrowLeft size={18} />
              Back
            </motion.button>

            {/* Attractive Logs Button */}
            <motion.button
              whileHover={{ scale: 1.05, y: -2 }}
              whileTap={{ scale: 0.95 }}
              onClick={() => setShowLogsModal(true)}
              className="relative group"
              title="View System Logs"
            >
              {/* Animated background glow */}
              <div className="absolute inset-0 bg-gradient-to-r from-primary-400 to-accent-400 rounded-xl blur-lg opacity-0 group-hover:opacity-30 transition-opacity duration-300" />
              
              {/* Button content */}
              <div className="relative flex items-center gap-2.5 px-5 py-3 bg-gradient-to-r from-primary-500 to-accent-500 text-white rounded-xl shadow-lg font-semibold overflow-hidden">
                {/* Shine effect */}
                <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/20 to-transparent -translate-x-full group-hover:translate-x-full transition-transform duration-700" />
                
                {/* Icon with animation */}
                <motion.div
                  animate={{ rotate: [0, 5, -5, 0] }}
                  transition={{ duration: 2, repeat: Infinity, repeatDelay: 3 }}
                >
                  <Terminal size={20} />
                </motion.div>
                
                {/* Text */}
                <span className="relative">System Logs</span>
                
                {/* Live indicator */}
                <div className="relative flex items-center gap-1.5">
                  <div className="w-1.5 h-1.5 rounded-full bg-green-300 animate-pulse" />
                  <span className="text-xs font-medium opacity-90">LIVE</span>
                </div>
              </div>
            </motion.button>
          </div>
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
              <h4 className="font-bold text-red-900 mb-1">Validation Error</h4>
              <p className="text-sm text-red-700">{error}</p>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
      
      {!report && (
        <motion.div
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          className="glass-card rounded-2xl shadow-glass-lg p-12 mb-6 relative overflow-hidden"
        >
          {/* Animated background gradient */}
          <div className="absolute inset-0 bg-gradient-to-br from-primary-500/5 to-accent-500/5 animate-pulse-soft" />
          
          <div className="relative z-10 text-center">
            <motion.div 
              initial={{ scale: 0 }}
              animate={{ scale: 1 }}
              transition={{ delay: 0.2, type: 'spring', stiffness: 200 }}
              className="mb-6 inline-flex p-6 rounded-2xl bg-gradient-to-br from-primary-100 to-accent-100"
            >
              <FileCheck className="text-primary-600" size={64} />
            </motion.div>
            
            <motion.h2 
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.3 }}
              className="text-3xl font-bold bg-gradient-to-r from-primary-600 to-accent-600 bg-clip-text text-transparent mb-4"
            >
              Post-Migration Data Validation
            </motion.h2>
            
            <motion.p 
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.4 }}
              className="text-gray-600 mb-8 max-w-2xl mx-auto leading-relaxed"
            >
              Run comprehensive validation checks to ensure your data migration was successful. 
              We'll compare row counts, verify schema structure, and validate data integrity 
              between source and target databases.
            </motion.p>
            
            <motion.button
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.5 }}
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
              onClick={runValidation}
              disabled={loading}
              className="btn-gradient px-12 py-4 rounded-xl text-lg font-bold shadow-lg flex items-center justify-center gap-3 mx-auto disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {loading ? (
                <>
                  <Loader2 className="animate-spin" size={24} />
                  Running Validation...
                </>
              ) : (
                <>
                  <Database size={24} />
                  Run Validation
                </>
              )}
            </motion.button>
          </div>
        </motion.div>
      )}
      
      {report && (
        <div className="space-y-6">
          {/* Summary Cards with animations */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.1 }}
              className="card-glass p-6 relative overflow-hidden group hover:shadow-glass-lg"
            >
              <div className="absolute top-0 right-0 w-32 h-32 bg-blue-500/10 rounded-full blur-2xl group-hover:bg-blue-500/20 transition-all" />
              <div className="relative z-10">
                <div className="flex items-center justify-between mb-3">
                  <div className="text-sm font-semibold text-gray-600">Total Tables</div>
                  <div className="p-2 rounded-lg bg-blue-100">
                    <Database className="text-blue-600" size={20} />
                  </div>
                </div>
                <div className="text-4xl font-bold text-gray-900">{report.summary.total_tables}</div>
              </div>
            </motion.div>
            
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2 }}
              className="card-glass p-6 relative overflow-hidden group hover:shadow-glass-lg"
            >
              <div className="absolute top-0 right-0 w-32 h-32 bg-green-500/10 rounded-full blur-2xl group-hover:bg-green-500/20 transition-all" />
              <div className="relative z-10">
                <div className="flex items-center justify-between mb-3">
                  <div className="text-sm font-semibold text-gray-600">Tables Matched</div>
                  <div className="p-2 rounded-lg bg-green-100">
                    <CheckCircle className="text-green-600" size={20} />
                  </div>
                </div>
                <div className="text-4xl font-bold text-green-600">{report.summary.tables_matched}</div>
              </div>
            </motion.div>
            
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.3 }}
              className="card-glass p-6 relative overflow-hidden group hover:shadow-glass-lg"
            >
              <div className="absolute top-0 right-0 w-32 h-32 bg-primary-500/10 rounded-full blur-2xl group-hover:bg-primary-500/20 transition-all" />
              <div className="relative z-10">
                <div className="flex items-center justify-between mb-3">
                  <div className="text-sm font-semibold text-gray-600">Checks Passed</div>
                  <div className="p-2 rounded-lg bg-primary-100">
                    <FileCheck className="text-primary-600" size={20} />
                  </div>
                </div>
                <div className="text-4xl font-bold text-primary-600">
                  {report.summary.checks_passed}<span className="text-2xl text-gray-400">/{report.summary.total_checks}</span>
                </div>
              </div>
            </motion.div>
            
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.4 }}
              className="card-glass p-6 relative overflow-hidden group hover:shadow-glass-lg"
            >
              <div className="absolute top-0 right-0 w-32 h-32 bg-accent-500/10 rounded-full blur-2xl group-hover:bg-accent-500/20 transition-all" />
              <div className="relative z-10">
                <div className="flex items-center justify-between mb-3">
                  <div className="text-sm font-semibold text-gray-600">Overall Accuracy</div>
                  <div className="p-2 rounded-lg bg-accent-100">
                    <TrendingUp className="text-accent-600" size={20} />
                  </div>
                </div>
                <div className="text-4xl font-bold text-accent-600">{report.summary.overall_accuracy.toFixed(1)}%</div>
              </div>
            </motion.div>
          </div>

          {/* Modern Visual Dashboard */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.5 }}
            className="card-glass p-8 relative overflow-hidden"
          >
            <div className="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-primary-500 to-accent-500" />
            
            <div className="flex items-center gap-3 mb-6">
              <div className="p-2 rounded-lg bg-gradient-to-br from-primary-100 to-accent-100">
                <Activity className="text-primary-600" size={24} />
              </div>
              <h3 className="font-bold text-gray-900 text-xl">Validation Dashboard</h3>
            </div>
            
            <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
              {/* Overall Accuracy with modern progress */}
              <div>
                <div className="flex justify-between mb-3">
                  <span className="text-sm font-semibold text-gray-700 flex items-center gap-2">
                    <BarChart3 size={16} className="text-primary-600" />
                    Overall Accuracy
                  </span>
                  <span className="text-sm font-bold text-primary-600">{report.summary.overall_accuracy.toFixed(1)}%</span>
                </div>
                <div className="relative w-full h-6 bg-gray-100 rounded-full overflow-hidden shadow-inner">
                  <motion.div 
                    initial={{ width: 0 }}
                    animate={{ width: `${report.summary.overall_accuracy}%` }}
                    transition={{ duration: 1, ease: 'easeOut' }}
                    className={`absolute inset-y-0 left-0 rounded-full ${
                      report.summary.overall_accuracy >= 90 ? 'bg-gradient-to-r from-green-400 to-green-600' : 
                      report.summary.overall_accuracy >= 70 ? 'bg-gradient-to-r from-yellow-400 to-yellow-600' : 
                      'bg-gradient-to-r from-red-400 to-red-600'
                    } shadow-lg`}
                  >
                    <div className="absolute inset-0 bg-white/20 animate-pulse-soft" />
                  </motion.div>
                </div>
              </div>
              
              {/* Pass/Fail Distribution with modern design */}
              <div>
                <div className="flex justify-between mb-3">
                  <span className="text-sm font-semibold text-gray-700 flex items-center gap-2">
                    <PieChart size={16} className="text-accent-600" />
                    Results Distribution
                  </span>
                  <span className="text-sm font-bold text-gray-700">
                    <span className="text-green-600">{report.summary.checks_passed}</span> / 
                    <span className="text-gray-700"> {report.summary.total_checks}</span>
                  </span>
                </div>
                <div className="relative flex h-6 rounded-full overflow-hidden shadow-inner">
                  <motion.div 
                    initial={{ width: 0 }}
                    animate={{ width: `${(report.summary.checks_passed / report.summary.total_checks) * 100}%` }}
                    transition={{ duration: 1, ease: 'easeOut' }}
                    className="bg-gradient-to-r from-green-400 to-green-600 relative"
                  >
                    <div className="absolute inset-0 bg-white/20 animate-pulse-soft" />
                  </motion.div>
                  <motion.div 
                    initial={{ width: 0 }}
                    animate={{ width: `${(report.summary.checks_failed / report.summary.total_checks) * 100}%` }}
                    transition={{ duration: 1, ease: 'easeOut', delay: 0.5 }}
                    className="bg-gradient-to-r from-red-400 to-red-600 relative"
                  >
                    <div className="absolute inset-0 bg-white/20 animate-pulse-soft" />
                  </motion.div>
                </div>
              </div>
            </div>
            
          </motion.div>

          {/* Export Buttons with modern design */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.6 }}
            className="card-glass p-6"
          >
            <div className="flex items-center justify-between flex-wrap gap-4">
              <h3 className="font-bold text-gray-900 flex items-center gap-2">
                <FileText className="text-primary-600" size={20} />
                Export Validation Report
              </h3>
              <div className="flex gap-3">
                <motion.button 
                  whileHover={{ scale: 1.05 }}
                  whileTap={{ scale: 0.95 }}
                  onClick={() => exportFile('json')} 
                  className="btn-export flex items-center gap-2 border-2 border-primary-500 text-primary-600 hover:bg-primary-500 hover:text-white shadow-sm"
                >
                  <FileJson size={18} />
                  JSON
                </motion.button>
                <motion.button 
                  whileHover={{ scale: 1.05 }}
                  whileTap={{ scale: 0.95 }}
                  onClick={() => exportFile('xlsx')} 
                  className="btn-export flex items-center gap-2 border-2 border-primary-500 text-primary-600 hover:bg-primary-500 hover:text-white shadow-sm"
                >
                  <FileSpreadsheet size={18} />
                  Excel
                </motion.button>
                <motion.button 
                  whileHover={{ scale: 1.05 }}
                  whileTap={{ scale: 0.95 }}
                  onClick={() => exportFile('pdf')} 
                  className="btn-export flex items-center gap-2 border-2 border-accent-500 text-accent-600 hover:bg-accent-500 hover:text-white shadow-sm"
                >
                  <FileText size={18} />
                  PDF
                </motion.button>
              </div>
            </div>
          </motion.div>

          {/* Table Comparisons with modern table design */}
          {report.table_comparisons && report.table_comparisons.length > 0 && (
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.7 }}
              className="card-glass p-8 relative overflow-hidden"
            >
              <div className="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-primary-500 to-accent-500" />
              
              <h3 className="font-bold text-gray-900 text-xl mb-6 flex items-center gap-3">
                <div className="p-2 rounded-lg bg-gradient-to-br from-primary-100 to-accent-100">
                  <Database size={24} className="text-primary-600" />
                </div>
                Row Count Comparison
              </h3>
              
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="bg-gradient-to-r from-primary-500 to-accent-500 text-white">
                      <th className="px-6 py-4 text-left font-bold rounded-tl-lg">Table Name</th>
                      <th className="px-6 py-4 text-right font-bold">Source Rows</th>
                      <th className="px-6 py-4 text-right font-bold">Target Rows</th>
                      <th className="px-6 py-4 text-center font-bold">Accuracy</th>
                      <th className="px-6 py-4 text-center font-bold rounded-tr-lg">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {report.table_comparisons.map((comparison, idx) => (
                      <motion.tr 
                        key={idx}
                        initial={{ opacity: 0, x: -20 }}
                        animate={{ opacity: 1, x: 0 }}
                        transition={{ delay: idx * 0.05 }}
                        className={`${idx % 2 === 0 ? 'bg-gray-50/50' : 'bg-white'} hover:bg-primary-50/30 transition-colors border-b border-gray-100`}
                      >
                        <td className="px-6 py-4 font-semibold text-gray-900">{comparison.table}</td>
                        <td className="px-6 py-4 text-right text-gray-700 font-medium">{comparison.source_rows.toLocaleString()}</td>
                        <td className="px-6 py-4 text-right text-gray-700 font-medium">{comparison.target_rows.toLocaleString()}</td>
                        <td className="px-6 py-4 text-center font-bold text-primary-600">{comparison.accuracy}</td>
                        <td className="px-6 py-4 text-center">
                          {comparison.status === 'Pass' ? (
                            <span className="inline-flex items-center gap-1.5 px-4 py-1.5 bg-gradient-to-r from-green-100 to-green-200 text-green-700 rounded-full text-sm font-bold shadow-sm">
                              <CheckCircle size={14} />
                              Pass
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1.5 px-4 py-1.5 bg-gradient-to-r from-red-100 to-red-200 text-red-700 rounded-full text-sm font-bold shadow-sm">
                              <XCircle size={14} />
                              Fail
                            </span>
                          )}
                        </td>
                      </motion.tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </motion.div>
          )}

          {/* Comprehensive DQ Checks Breakdown */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.8 }}
            className="card-glass p-8 relative overflow-hidden"
          >
            <div className="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-accent-500 to-primary-500" />
            
            <h3 className="font-bold text-gray-900 text-xl mb-6 flex items-center gap-3">
              <div className="p-2 rounded-lg bg-gradient-to-br from-accent-100 to-primary-100">
                <FileCheck size={24} className="text-accent-600" />
              </div>
              Comprehensive Data Quality Checks (16+ Checks)
            </h3>
            
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-8">
              {report.checks.map((check, idx) => (
                <motion.div
                  key={idx}
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: idx * 0.05 }}
                  className={`p-4 rounded-xl border-2 transition-all hover:shadow-md ${
                    check.status === 'Pass'
                      ? 'bg-green-50 border-green-200 hover:border-green-300'
                      : 'bg-red-50 border-red-200 hover:border-red-300'
                  }`}
                >
                  <div className="flex items-start gap-3">
                    <div className={`p-2 rounded-lg mt-1 ${
                      check.status === 'Pass' ? 'bg-green-100' : 'bg-red-100'
                    }`}>
                      {check.status === 'Pass' ? (
                        <CheckCircle className="text-green-600" size={18} />
                      ) : (
                        <XCircle className="text-red-600" size={18} />
                      )}
                    </div>
                    <div className="flex-1">
                      <h4 className="font-bold text-gray-900 text-sm mb-1">{check.category}</h4>
                      <div className="text-xs text-gray-600 mb-2">
                        {check.status === 'Pass' ? '✓ Validated' : '✗ Issues found'}
                      </div>
                      {check.status !== 'Pass' && (
                        <div
                          className="text-xs text-gray-600 mb-2 line-clamp-2"
                          title={check.errorDetails || check.suggestedFix || ''}
                        >
                          {check.errorDetails || check.suggestedFix || 'No details provided'}
                        </div>
                      )}
                      <div className={`text-xs font-bold px-2 py-1 rounded-full inline-block ${
                        check.status === 'Pass'
                          ? 'bg-green-100 text-green-700'
                          : 'bg-red-100 text-red-700'
                      }`}>
                        {(check.confidence * 100).toFixed(0)}% Confidence
                      </div>
                    </div>
                  </div>
                </motion.div>
              ))}
            </div>
            
            {/* Detailed Breakdown Table */}
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-gradient-to-r from-primary-500 to-accent-500 text-white">
                  <tr>
                    <th className="px-4 py-3 text-left font-bold rounded-tl-lg">Category</th>
                    <th className="px-4 py-3 text-left font-bold">Check Description</th>
                    <th className="px-4 py-3 text-center font-bold">Status</th>
                    <th className="px-4 py-3 text-center font-bold">Confidence</th>
                    <th className="px-4 py-3 text-center font-bold rounded-tr-lg">Details</th>
                  </tr>
                </thead>
                <tbody>
                  {report.checks.map((check, idx) => (
                    <motion.tr
                      key={idx}
                      initial={{ opacity: 0, x: -20 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: idx * 0.03 }}
                      className={`${idx % 2 === 0 ? 'bg-white' : 'bg-gray-50/50'} hover:bg-primary-50/30 transition-colors border-b border-gray-100`}
                    >
                      <td className="px-4 py-3 font-medium text-gray-900">{check.category.split(':')[0]}</td>
                      <td className="px-4 py-3 text-gray-700">{check.category.split(':').slice(1).join(':')}</td>
                      <td className="px-4 py-3 text-center">
                        {check.status === 'Pass' ? (
                          <span className="inline-flex items-center gap-1 px-3 py-1 bg-green-100 text-green-700 rounded-full text-xs font-bold">
                            <CheckCircle size={12} /> Pass
                          </span>
                        ) : (
                          <span className="inline-flex items-center gap-1 px-3 py-1 bg-red-100 text-red-700 rounded-full text-xs font-bold">
                            <XCircle size={12} /> Fail
                          </span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-center font-bold text-primary-600">
                        {(check.confidence * 100).toFixed(0)}%
                      </td>
                      <td className="px-4 py-3 text-center">
                          <span className="text-xs text-gray-700" title={check.errorDetails || check.suggestedFix || ''}>
                            {check.errorDetails || check.suggestedFix || getDefaultDetailsForCheck(check.category)}
                          </span>
                      </td>
                    </motion.tr>
                  ))}
                </tbody>
              </table>
            </div>
          </motion.div>

          {/* Navigation to migration history */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.9 }}
            className="flex justify-center pt-4"
          >
            <motion.button
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
              onClick={() => navigate('/history')}
              className="btn-gradient px-10 py-4 rounded-xl font-bold shadow-lg flex items-center gap-3"
            >
              <Database size={20} />
              View Migration History
            </motion.button>
          </motion.div>
        </div>
      )}
    </div>
  )
}
