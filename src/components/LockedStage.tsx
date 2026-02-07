import { Lock } from 'lucide-react'
import { Link } from 'react-router-dom'
import { motion } from 'framer-motion'

type LockedStageProps = {
  title: string
  message: string
  actionLabel: string
  actionPath: string
}

export default function LockedStage({ title, message, actionLabel, actionPath }: LockedStageProps) {
  return (
    <div className="flex items-center justify-center min-h-[60vh]">
      <motion.div 
        initial={{ opacity: 0, scale: 0.9, y: 20 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        transition={{ duration: 0.4 }}
        className="max-w-lg w-full glass-card rounded-3xl p-12 text-center shadow-glass-lg border-2 border-dashed border-accent-200"
      >
        <motion.div 
          initial={{ scale: 0 }}
          animate={{ scale: 1 }}
          transition={{ delay: 0.2, type: 'spring', stiffness: 200 }}
          className="mx-auto mb-6 w-20 h-20 rounded-2xl bg-gradient-to-br from-primary-100 to-accent-100 flex items-center justify-center"
        >
          <Lock className="text-primary-600" size={32} />
        </motion.div>
        
        <motion.h2 
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
          className="text-3xl font-bold bg-gradient-to-r from-primary-600 to-accent-600 bg-clip-text text-transparent mb-4"
        >
          {title}
        </motion.h2>
        
        <motion.p 
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.4 }}
          className="text-gray-600 mb-8 leading-relaxed"
        >
          {message}
        </motion.p>
        
        <motion.div
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.5 }}
        >
          <Link
            to={actionPath}
            className="inline-flex items-center justify-center px-8 py-3 rounded-xl text-white btn-gradient font-semibold shadow-lg"
          >
            {actionLabel}
          </Link>
        </motion.div>
      </motion.div>
    </div>
  )
}