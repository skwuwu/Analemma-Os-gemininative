/**
 * ContextualSideRail: Slim icon-based navigation rail
 * ===================================================
 * 
 * Replaces the "Insight Centre" button with a persistent side rail
 * that shows status indicators and auto-switches based on context.
 * 
 * Inspired by:
 * - VSCode Activity Bar
 * - Figma's right panel tabs
 * - JetBrains IDEs toolwindow buttons
 */
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { ShieldAlert, Activity, XCircle } from 'lucide-react';
import { cn } from '@/lib/utils';

export type RailTab = 'audit' | 'timeline';

interface ContextualSideRailProps {
  activeTab: RailTab;
  onTabChange: (tab: RailTab) => void;
  issueCount?: number;
  hasErrors?: boolean;
  isExecuting?: boolean;
  panelOpen: boolean;
  onTogglePanel: () => void;
}

export function ContextualSideRail({
  activeTab,
  onTabChange,
  issueCount = 0,
  hasErrors = false,
  isExecuting = false,
  panelOpen,
  onTogglePanel
}: ContextualSideRailProps) {
  
  const tabs = [
    {
      id: 'audit' as RailTab,
      icon: ShieldAlert,
      label: 'Audit',
      description: 'Real-time validation results',
      badge: issueCount > 0 ? issueCount : null,
      highlight: hasErrors
    },
    {
      id: 'timeline' as RailTab,
      icon: Activity,
      label: 'Timeline',
      description: 'Execution checkpoints',
      badge: null,
      highlight: isExecuting
    }
  ];

  const handleTabClick = (tabId: RailTab) => {
    if (panelOpen && activeTab === tabId) {
      // Toggle off if clicking active tab
      onTogglePanel();
    } else {
      // Switch tab and ensure panel is open
      if (!panelOpen) {
        onTogglePanel();
      }
      onTabChange(tabId);
    }
  };

  return (
    <div className="fixed top-1/2 right-6 -translate-y-1/2 z-30">
      <div className="flex flex-col gap-1 p-1.5 bg-slate-900/80 backdrop-blur-xl border border-slate-800 rounded-2xl shadow-2xl">
        {tabs.map((tab) => {
          const Icon = tab.icon;
          const isActive = panelOpen && activeTab === tab.id;
          
          return (
            <Tooltip key={tab.id}>
              <TooltipTrigger asChild>
                <Button
                  variant="ghost"
                  size="icon"
                  onClick={() => handleTabClick(tab.id)}
                  className={cn(
                    'relative h-12 w-12 rounded-xl transition-all',
                    isActive && 'bg-slate-800 text-blue-400',
                    !isActive && 'text-slate-400 hover:text-slate-100 hover:bg-slate-800/50',
                    tab.highlight && !isActive && 'text-amber-400'
                  )}
                >
                  <Icon className={cn(
                    'w-5 h-5',
                    tab.highlight && 'animate-pulse'
                  )} />
                  
                  {/* Badge for issue count or notifications */}
                  {tab.badge && (
                    <Badge 
                      className={cn(
                        'absolute -top-1 -right-1 h-5 min-w-5 px-1 text-[10px] font-bold',
                        hasErrors && tab.id === 'audit' 
                          ? 'bg-red-500 text-white' 
                          : 'bg-amber-500 text-white'
                      )}
                    >
                      {tab.badge}
                    </Badge>
                  )}

                  {/* Active indicator line */}
                  {isActive && (
                    <div className="absolute left-0 top-1/2 -translate-y-1/2 w-1 h-6 bg-blue-500 rounded-r-full" />
                  )}
                </Button>
              </TooltipTrigger>
              <TooltipContent side="left" className="font-semibold">
                <p className="font-black text-xs uppercase tracking-wider">{tab.label}</p>
                <p className="text-[10px] text-slate-400 font-normal mt-0.5">{tab.description}</p>
              </TooltipContent>
            </Tooltip>
          );
        })}

        {/* Close button (only shown when panel is open) */}
        {panelOpen && (
          <>
            <div className="h-px bg-slate-800 my-1" />
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="ghost"
                  size="icon"
                  onClick={onTogglePanel}
                  className="h-10 w-10 rounded-xl text-slate-500 hover:text-slate-100 hover:bg-slate-800/50"
                >
                  <XCircle className="w-4 h-4" />
                </Button>
              </TooltipTrigger>
              <TooltipContent side="left">
                <p className="text-xs font-semibold">Close Panel</p>
              </TooltipContent>
            </Tooltip>
          </>
        )}
      </div>
    </div>
  );
}
