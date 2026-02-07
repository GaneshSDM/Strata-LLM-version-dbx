import { Settings, LogOut, X } from 'lucide-react'
import Sidebar from './Sidebar'
import dmLogo from '../assets/DM Logo Only_1757430956435_1763392245558.png'
import { StageProgress } from '../types/workflow'
import { motion, AnimatePresence } from 'framer-motion'

type LayoutProps = {
  children: React.ReactNode
  onOpenSettings: () => void
  onLogout: () => void
  stageProgress: StageProgress
  onBlockedNavigation: (message: string) => void
  notification?: string | null
  onDismissNotification?: () => void
  modal?: React.ReactNode
}

export default function Layout({
  children,
  onOpenSettings,
  onLogout,
  stageProgress,
  onBlockedNavigation,
  notification,
  onDismissNotification,
  modal
}: LayoutProps) {
  return (
    <div className="h-screen bg-gradient-to-br from-gray-50 via-blue-50/30 to-orange-50/30 flex flex-col relative overflow-hidden">
      {/* Animated background elements */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        <div className="absolute top-0 right-0 w-[500px] h-[500px] bg-primary-500/5 rounded-full blur-3xl animate-float" />
        <div className="absolute bottom-0 left-0 w-[400px] h-[400px] bg-accent-500/5 rounded-full blur-3xl animate-float" style={{ animationDelay: '1s' }} />
      </div>

      <div className="flex flex-1 overflow-hidden">
        <Sidebar
          stageProgress={stageProgress}
          onBlockedNavigation={onBlockedNavigation}
        />

        <div className="flex-1 flex flex-col relative z-10 min-w-0">
          {/* Compact modern header with glass effect */}
          <motion.header 
            initial={{ y: -20, opacity: 0 }}
            animate={{ y: 0, opacity: 1 }}
            className="glass-card border-b border-white/50 px-4 sm:px-6 lg:px-8 py-2 flex items-center justify-end sticky top-0 z-20 shrink-0"
          >
            <div className="flex items-center gap-2 sm:gap-3">
              <div className="flex items-center gap-1.5 sm:gap-2 px-2.5 sm:px-3 py-1.5 glass rounded-full border border-primary-200/50 shadow-sm">
                <div className="status-online" />
                <span className="text-[10px] sm:text-xs font-semibold text-primary-600 whitespace-nowrap">System Online</span>
              </div>
              
              <motion.button
                whileHover={{ scale: 1.05 }}
                whileTap={{ scale: 0.95 }}
                onClick={onOpenSettings}
                className="p-1.5 sm:p-2 glass hover:glass-dark rounded-lg transition-all group"
                title="Settings"
              >
                <Settings className="w-4 h-4 sm:w-5 sm:h-5 text-gray-600 group-hover:text-primary-600 transition-colors" />
              </motion.button>
              
              <motion.button
                whileHover={{ scale: 1.05 }}
                whileTap={{ scale: 0.95 }}
                onClick={onLogout}
                className="p-1.5 sm:p-2 glass hover:glass-dark rounded-lg transition-all group"
                title="Logout"
              >
                <LogOut className="w-4 h-4 sm:w-5 sm:h-5 text-gray-600 group-hover:text-accent-600 transition-colors" />
              </motion.button>
            </div>
          </motion.header>

          {/* Notification banner with animation */}
          <AnimatePresence>
            {notification && (
              <motion.div
                initial={{ height: 0, opacity: 0 }}
                animate={{ height: 'auto', opacity: 1 }}
                exit={{ height: 0, opacity: 0 }}
                className="overflow-hidden shrink-0"
              >
                <div className="bg-gradient-to-r from-amber-50 to-orange-50 border-b border-amber-200/50 px-4 sm:px-6 lg:px-8 py-2 flex items-center justify-between">
                  <div className="flex items-center gap-2 sm:gap-3 min-w-0">
                    <div className="w-1.5 h-1.5 rounded-full bg-amber-500 animate-pulse shrink-0" />
                    <span className="text-xs sm:text-sm font-medium text-amber-900 truncate">{notification}</span>
                  </div>
                  <motion.button
                    whileHover={{ scale: 1.1 }}
                    whileTap={{ scale: 0.9 }}
                    type="button"
                    onClick={onDismissNotification}
                    className="p-1 sm:p-1.5 rounded-lg hover:bg-amber-100/50 transition-colors shrink-0"
                    aria-label="Dismiss notification"
                  >
                    <X size={14} className="text-amber-700" />
                  </motion.button>
                </div>
              </motion.div>
            )}
          </AnimatePresence>

          {/* Main content with animation */}
          <main className="flex-1 p-4 sm:p-5 lg:p-6 overflow-auto">
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.4 }}
              className="h-full"
            >
              {children}
            </motion.div>
          </main>

          {/* Compact modern footer with glass effect */}
          <motion.footer 
            initial={{ y: 20, opacity: 0 }}
            animate={{ y: 0, opacity: 1 }}
            className="glass-card border-t border-white/50 py-1.5 px-4 sm:px-6 lg:px-8 shrink-0"
          >
            <div className="flex items-center justify-center gap-2">
              <img src={dmLogo} alt="DecisionMinds" className="h-4 sm:h-5 opacity-80 hover:opacity-100 transition-opacity" />
              <p className="text-[10px] sm:text-xs text-gray-500 font-medium">Powered by DecisionMinds</p>
            </div>
          </motion.footer>
        </div>
      </div>
      {modal}
    </div>
  );
}
