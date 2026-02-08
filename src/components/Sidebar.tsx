import { Search, Download, Database, Zap, Terminal, SlidersHorizontal } from 'lucide-react';
import { Link, useLocation } from 'react-router-dom';
import strataLogo from '../assets/image (1)_1763392280085.png';
import { StageProgress } from '../types/workflow';
import { motion } from 'framer-motion';

const navigationItems = [
  {
    id: 1,
    title: 'Analyze',
    description: 'Database analysis and mapping',
    icon: Search,
    path: '/',
  },
  {
    id: 2,
    title: 'Extract',
    description: 'Data extraction and structure',
    icon: Download,
    path: '/extract',
  },
  {
    id: 3,
    title: 'View Logs',
    description: 'Session log feed',
    icon: Terminal,
    path: '/logs',
  },
  {
    id: 4,
    title: 'Data Types',
    description: 'Source/target datatype mappings',
    icon: SlidersHorizontal,
    path: '/datatypes',
  },
];

type SidebarProps = {
  stageProgress: StageProgress
  onBlockedNavigation: (message: string) => void
}

const getAccessMessage = (path: string, progress: StageProgress) => {
  switch (path) {
    case '/extract':
      return {
        allowed: progress.analysis,
        message: 'Complete the analysis step to unlock extraction.'
      };
    case '/logs':
      return {
        allowed: true,
        message: ''
      };
    default:
      return { allowed: true, message: '' };
  }
};

export default function Sidebar({ stageProgress, onBlockedNavigation }: SidebarProps) {
  const location = useLocation();

  return (
    <div className="w-44 sm:w-52 lg:w-60 glass-card border-r border-white/50 flex flex-col h-full relative z-20 shrink-0">
      {/* Compact logo section with animation */}
      <motion.div 
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="p-2.5 sm:p-3 border-b border-white/30 shrink-0"
      >
        <img src={strataLogo} alt="Strata" className="h-10 sm:h-12 lg:h-14 mb-2" />
        <p className="text-[10px] sm:text-xs text-gray-600 font-semibold tracking-wide leading-tight">Enterprise AI Translation Platform</p>
      </motion.div>

      {/* Navigation with stagger animation */}
      <nav className="flex-1 px-2 py-2.5 space-y-0.5">
        {navigationItems.map((item, index) => {
          const Icon = item.icon;
          const isActive = location.pathname === item.path;
          const access = getAccessMessage(item.path, stageProgress);
          const isDisabled = !access.allowed;

          const handleClick = (e: React.MouseEvent<HTMLAnchorElement>) => {
            if (isDisabled) {
              e.preventDefault();
              onBlockedNavigation(access.message);
            }
          };

          return (
            <motion.div
              key={item.id}
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: index * 0.1 }}
            >
              <Link
                to={item.path}
                onClick={handleClick}
                className={`
                  group relative block p-2 rounded-lg transition-all duration-300
                  ${isActive 
                    ? 'bg-gradient-to-r from-primary-500 to-accent-500 text-white shadow-md scale-[1.01]' 
                    : isDisabled
                      ? 'bg-white/40 text-gray-400 cursor-not-allowed'
                      : 'bg-white/60 hover:bg-white/80 text-gray-700 hover:shadow-sm hover:scale-[1.01]'
                  }
                `}
                aria-disabled={isDisabled}
              >
                <div className="flex items-start gap-2">
                  {/* Step number badge */}
                  <div className={`
                    flex-shrink-0 w-6 h-6 rounded-md flex items-center justify-center font-bold text-xs
                    transition-all duration-300
                    ${isActive 
                      ? 'bg-white/20 text-white' 
                      : isDisabled
                        ? 'bg-gray-200 text-gray-400'
                        : 'bg-gradient-to-br from-primary-500 to-primary-600 text-white group-hover:scale-110'
                    }
                  `}>
                    {item.id}
                  </div>
                  
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-1.5 mb-0.5">
                      <Icon 
                        size={13}
                        className={`
                          transition-all duration-300 shrink-0
                          ${isActive 
                            ? 'text-white' 
                            : isDisabled
                              ? 'text-gray-400'
                              : 'text-primary-600 group-hover:scale-110'
                          }
                        `} 
                      />
                      <h3 className="font-bold text-xs truncate">{item.title}</h3>
                    </div>
                    <p className={`
                      text-[9px] leading-tight line-clamp-2
                      ${isActive 
                        ? 'text-white/90' 
                        : isDisabled
                          ? 'text-gray-400'
                          : 'text-gray-600'
                      }
                    `}>
                      {item.description}
                    </p>
                  </div>

                  {/* Active indicator */}
                  {isActive && (
                    <motion.div
                      layoutId="activeIndicator"
                      className="absolute right-1 sm:right-2 top-0 bottom-0 w-0.5 sm:w-1 bg-white/40 rounded-full"
                    />
                  )}
                </div>
              </Link>
            </motion.div>
          );
        })}
      </nav>

      {/* Compact system status with glass effect */}
      <motion.div 
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        className="p-2 border-t border-white/30 glass shrink-0"
      >
        <div className="mb-2">
          <div className="flex items-center justify-between mb-1">
            <h3 className="text-xs sm:text-sm font-bold text-gray-800">System Status</h3>
            <div className="status-online" />
          </div>
          <p className="text-[10px] sm:text-xs text-gray-600">All services operational</p>
        </div>
        
        <div className="space-y-1">
          <div className="flex items-center justify-between p-1 rounded-md bg-white/50">
            <div className="flex items-center gap-1 min-w-0">
              <Database size={11} className="text-primary-600 shrink-0" />
              <span className="text-[9px] sm:text-[10px] font-medium text-gray-700 truncate">Database</span>
            </div>
            <span className="text-[9px] sm:text-[10px] font-semibold text-primary-600 shrink-0">Active</span>
          </div>
          <div className="flex items-center justify-between p-1 rounded-md bg-white/50">
            <div className="flex items-center gap-1 min-w-0">
              <Zap size={11} className="text-accent-600 shrink-0" />
              <span className="text-[9px] sm:text-[10px] font-medium text-gray-700 truncate">AI Engine</span>
            </div>
            <span className="text-[9px] sm:text-[10px] font-semibold text-accent-600 shrink-0">Ready</span>
          </div>
        </div>
      </motion.div>
    </div>
  );
}
