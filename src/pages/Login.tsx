import { Settings } from 'lucide-react';
import strataLogo from '../assets/image (1)_1763392280085.png';
import dmLogo from '../assets/DM Logo Only_1757430956435_1763392245558.png';
import { motion } from 'framer-motion';

interface LoginProps {
  onLogin: () => void;
}

export default function Login({ onLogin }: LoginProps) {
  return (
    <div className="min-h-screen bg-gradient-to-br from-primary-50 via-white to-accent-50 flex items-center justify-center p-4 relative overflow-hidden">
      {/* Animated background elements */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        <div className="absolute top-0 right-0 w-[600px] h-[600px] bg-primary-500/10 rounded-full blur-3xl animate-float" />
        <div className="absolute bottom-0 left-0 w-[500px] h-[500px] bg-accent-500/10 rounded-full blur-3xl animate-float" style={{ animationDelay: '2s' }} />
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[400px] h-[400px] bg-primary-300/5 rounded-full blur-3xl animate-pulse-soft" />
      </div>

      <motion.div 
        initial={{ opacity: 0, scale: 0.9, y: 20 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        transition={{ duration: 0.5 }}
        className="glass-card rounded-3xl shadow-glass-lg p-12 max-w-md w-full relative z-10"
      >
        <motion.button 
          whileHover={{ scale: 1.1, rotate: 90 }}
          whileTap={{ scale: 0.9 }}
          className="absolute top-6 right-6 text-gray-300 hover:text-primary-600 transition-colors"
        >
          <Settings size={24} />
        </motion.button>

        <div className="flex flex-col items-center space-y-8">
          <motion.img 
            initial={{ opacity: 0, y: -20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            src={strataLogo} 
            alt="Strata Logo" 
            className="h-16 object-contain"
          />

          <motion.div 
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.3 }}
            className="text-center space-y-2"
          >
            <h1 className="text-4xl font-bold bg-gradient-to-r from-primary-600 to-accent-600 bg-clip-text text-transparent">
              Strata
            </h1>
            <p className="text-gray-600 text-sm font-medium">Enterprise AI Translation Platform</p>
            <p className="text-gray-500 text-xs">Please sign in to continue</p>
          </motion.div>

          <motion.button
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.4 }}
            whileHover={{ scale: 1.02, boxShadow: '0 20px 40px rgba(8, 86, 144, 0.3)' }}
            whileTap={{ scale: 0.98 }}
            onClick={onLogin}
            className="w-full bg-gradient-to-r from-primary-500 to-accent-500 text-white font-medium rounded-lg transition-all duration-200 py-4 flex items-center justify-center gap-3 shadow-lg"
          >
            <svg className="w-5 h-5" viewBox="0 0 21 21" fill="none">
              <rect x="1" y="1" width="9" height="9" fill="white" />
              <rect x="11" y="1" width="9" height="9" fill="white" />
              <rect x="1" y="11" width="9" height="9" fill="white" />
              <rect x="11" y="11" width="9" height="9" fill="white" />
            </svg>
            <span className="font-semibold">Login with Office365</span>
          </motion.button>

          <motion.p 
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.5 }}
            className="text-xs text-gray-400 text-center flex items-center gap-2"
          >
            <div className="w-1 h-1 rounded-full bg-primary-500" />
            Secure authentication powered by Microsoft Azure AD
          </motion.p>

          <motion.div 
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.6 }}
            className="pt-8 border-t border-gray-200/50 w-full flex flex-col items-center space-y-2"
          >
            <img 
              src={dmLogo} 
              alt="DecisionMinds" 
              className="h-7 object-contain opacity-70 hover:opacity-100 transition-opacity"
            />
            <p className="text-xs text-gray-400">Powered by DecisionMinds</p>
          </motion.div>
        </div>
      </motion.div>
    </div>
  );
}
