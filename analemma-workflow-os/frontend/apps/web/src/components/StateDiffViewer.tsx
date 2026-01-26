/**
 * State Diff Viewer
 * 
 * 두 체크포인트 간의 상태 차이를 시각화하는 컴포넌트입니다.
 */

import React, { useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Plus,
  Minus,
  ChevronRight,
  ChevronDown,
  GitCompare,
  Copy,
  Check,
  ArrowLeftRight
} from 'lucide-react';
import { cn } from '@/lib/utils';
import type { StateDiff, CheckpointCompareResult } from '@/lib/types';

interface StateDiffViewerProps {
  diff: StateDiff | null;
  compareResult?: CheckpointCompareResult;
  loading?: boolean;
  sourceLabel?: string;
  targetLabel?: string;
}

interface DiffRowProps {
  keyName: string;
  type: 'added' | 'removed' | 'modified';
  value?: any;
  oldValue?: any;
  newValue?: any;
}

const formatValue = (value: any): string => {
  if (value === null) return 'null';
  if (value === undefined) return 'undefined';
  if (typeof value === 'string') return value;
  return JSON.stringify(value, null, 2);
};

const isComplexValue = (value: any): boolean => {
  return typeof value === 'object' && value !== null;
};

const DiffRow: React.FC<DiffRowProps> = ({ keyName, type, value, oldValue, newValue }) => {
  const [isOpen, setIsOpen] = useState(false);
  const [copied, setCopied] = useState(false);

  const config = {
    added: {
      icon: Plus,
      bgClass: 'bg-green-50 dark:bg-green-950',
      borderClass: 'border-green-200 dark:border-green-800',
      labelClass: 'text-green-700 dark:text-green-300',
      label: '추가됨'
    },
    removed: {
      icon: Minus,
      bgClass: 'bg-red-50 dark:bg-red-950',
      borderClass: 'border-red-200 dark:border-red-800',
      labelClass: 'text-red-700 dark:text-red-300',
      label: '삭제됨'
    },
    modified: {
      icon: ArrowLeftRight,
      bgClass: 'bg-yellow-50 dark:bg-yellow-950',
      borderClass: 'border-yellow-200 dark:border-yellow-800',
      labelClass: 'text-yellow-700 dark:text-yellow-300',
      label: '변경됨'
    },
  };

  const { icon: Icon, bgClass, borderClass, labelClass, label } = config[type];
  const displayValue = type === 'modified' ? newValue : value;
  const hasComplexValue = isComplexValue(displayValue) || (type === 'modified' && isComplexValue(oldValue));

  const handleCopy = async () => {
    const textToCopy = type === 'modified'
      ? `${keyName}:\n  이전: ${formatValue(oldValue)}\n  이후: ${formatValue(newValue)}`
      : `${keyName}: ${formatValue(displayValue)}`;

    await navigator.clipboard.writeText(textToCopy);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  if (hasComplexValue) {
    return (
      <Collapsible open={isOpen} onOpenChange={setIsOpen}>
        <div className={cn("border rounded-md overflow-hidden mb-2", borderClass)}>
          <CollapsibleTrigger asChild>
            <div className={cn(
              "flex items-center justify-between p-2 cursor-pointer hover:opacity-80",
              bgClass
            )}>
              <div className="flex items-center gap-2">
                <Icon className={cn("w-4 h-4", labelClass)} />
                <code className="text-sm font-mono">{keyName}</code>
                <Badge variant="outline" className="text-xs">{label}</Badge>
              </div>
              <div className="flex items-center gap-1">
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-6 w-6"
                  onClick={(e) => { e.stopPropagation(); handleCopy(); }}
                >
                  {copied ? <Check className="w-3 h-3" /> : <Copy className="w-3 h-3" />}
                </Button>
                {isOpen ? (
                  <ChevronDown className="w-4 h-4 text-muted-foreground" />
                ) : (
                  <ChevronRight className="w-4 h-4 text-muted-foreground" />
                )}
              </div>
            </div>
          </CollapsibleTrigger>
          <CollapsibleContent>
            <div className="p-3 bg-background border-t">
              {type === 'modified' ? (
                <div className="space-y-3">
                  <div>
                    <div className="text-xs text-red-600 font-medium mb-1">이전 값:</div>
                    <pre className="text-xs bg-red-50 dark:bg-red-950 p-2 rounded overflow-auto max-h-40">
                      {formatValue(oldValue)}
                    </pre>
                  </div>
                  <div>
                    <div className="text-xs text-green-600 font-medium mb-1">이후 값:</div>
                    <pre className="text-xs bg-green-50 dark:bg-green-950 p-2 rounded overflow-auto max-h-40">
                      {formatValue(newValue)}
                    </pre>
                  </div>
                </div>
              ) : (
                <pre className="text-xs overflow-auto max-h-60">
                  {formatValue(displayValue)}
                </pre>
              )}
            </div>
          </CollapsibleContent>
        </div>
      </Collapsible>
    );
  }

  return (
    <div className={cn("border rounded-md p-2 mb-2", bgClass, borderClass)}>
      <div className="flex items-start justify-between gap-2">
        <div className="flex items-start gap-2 flex-1 min-w-0">
          <Icon className={cn("w-4 h-4 mt-0.5 shrink-0", labelClass)} />
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 flex-wrap">
              <code className="text-sm font-mono">{keyName}</code>
              <Badge variant="outline" className="text-xs">{label}</Badge>
            </div>
            {type === 'modified' ? (
              <div className="mt-1 text-sm">
                <span className="text-red-600 line-through">{formatValue(oldValue)}</span>
                <span className="mx-2">→</span>
                <span className="text-green-600">{formatValue(newValue)}</span>
              </div>
            ) : (
              <div className="mt-1 text-sm text-muted-foreground truncate">
                {formatValue(displayValue)}
              </div>
            )}
          </div>
        </div>
        <Button
          variant="ghost"
          size="icon"
          className="h-6 w-6 shrink-0"
          onClick={handleCopy}
        >
          {copied ? <Check className="w-3 h-3" /> : <Copy className="w-3 h-3" />}
        </Button>
      </div>
    </div>
  );
};

export const StateDiffViewer: React.FC<StateDiffViewerProps> = ({
  diff,
  compareResult,
  loading = false,
  sourceLabel = '이전',
  targetLabel = '이후',
}) => {
  const activeDiff = diff || (compareResult as StateDiff);
  const addedEntries = Object.entries(activeDiff?.added || {});
  const removedEntries = Object.entries(activeDiff?.removed || {});
  const modifiedEntries = Object.entries(activeDiff?.modified || {});

  const totalChanges = addedEntries.length + removedEntries.length + modifiedEntries.length;

  if (loading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <GitCompare className="w-4 h-4" />
            상태 비교
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            <Skeleton className="h-12 w-full" />
            <Skeleton className="h-12 w-full" />
            <Skeleton className="h-12 w-full" />
          </div>
        </CardContent>
      </Card>
    );
  }

  if (!diff || totalChanges === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <GitCompare className="w-4 h-4" />
            상태 비교
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-8 text-muted-foreground">
            <GitCompare className="w-8 h-8 mx-auto mb-2 opacity-50" />
            <p>변경 사항이 없습니다.</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="text-base flex items-center gap-2">
            <GitCompare className="w-4 h-4" />
            상태 비교
          </CardTitle>
          <div className="flex items-center gap-2">
            {addedEntries.length > 0 && (
              <Badge className="bg-green-100 text-green-700">
                +{addedEntries.length}
              </Badge>
            )}
            {removedEntries.length > 0 && (
              <Badge className="bg-red-100 text-red-700">
                -{removedEntries.length}
              </Badge>
            )}
            {modifiedEntries.length > 0 && (
              <Badge className="bg-yellow-100 text-yellow-700">
                ~{modifiedEntries.length}
              </Badge>
            )}
          </div>
        </div>
        {compareResult && (
          <CardDescription>
            {compareResult.source_node_name} → {compareResult.target_node_name}
          </CardDescription>
        )}
      </CardHeader>
      <CardContent>
        <Tabs defaultValue="all">
          <TabsList className="grid w-full grid-cols-4 mb-4">
            <TabsTrigger value="all">전체 ({totalChanges})</TabsTrigger>
            <TabsTrigger value="added" disabled={addedEntries.length === 0}>
              추가 ({addedEntries.length})
            </TabsTrigger>
            <TabsTrigger value="removed" disabled={removedEntries.length === 0}>
              삭제 ({removedEntries.length})
            </TabsTrigger>
            <TabsTrigger value="modified" disabled={modifiedEntries.length === 0}>
              변경 ({modifiedEntries.length})
            </TabsTrigger>
          </TabsList>

          <ScrollArea className="max-h-[400px] pr-4">
            <TabsContent value="all" className="mt-0">
              {addedEntries.map(([key, value]) => (
                <DiffRow key={`added-${key}`} keyName={key} type="added" value={value} />
              ))}
              {removedEntries.map(([key, value]) => (
                <DiffRow key={`removed-${key}`} keyName={key} type="removed" value={value} />
              ))}
              {modifiedEntries.map(([key, change]: [string, any]) => (
                <DiffRow
                  key={`modified-${key}`}
                  keyName={key}
                  type="modified"
                  oldValue={change.old}
                  newValue={change.new}
                />
              ))}
            </TabsContent>

            <TabsContent value="added" className="mt-0">
              {addedEntries.map(([key, value]) => (
                <DiffRow key={`added-${key}`} keyName={key} type="added" value={value} />
              ))}
            </TabsContent>

            <TabsContent value="removed" className="mt-0">
              {removedEntries.map(([key, value]) => (
                <DiffRow key={`removed-${key}`} keyName={key} type="removed" value={value} />
              ))}
            </TabsContent>

            <TabsContent value="modified" className="mt-0">
              {modifiedEntries.map(([key, change]: [string, any]) => (
                <DiffRow
                  key={`modified-${key}`}
                  keyName={key}
                  type="modified"
                  oldValue={change.old}
                  newValue={change.new}
                />
              ))}
            </TabsContent>
          </ScrollArea>
        </Tabs>
      </CardContent>
    </Card>
  );
};

export default StateDiffViewer;
