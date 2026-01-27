import React from 'react';
import { Loader2 } from 'lucide-react';
import { cn } from '@/lib/utils';

interface LoadingSpinnerProps {
  /** 스피너 크기 */
  size?: 'sm' | 'md' | 'lg' | 'xl';
  /** 로딩 라벨 텍스트 */
  label?: string;
  /** 추가 클래스명 */
  className?: string;
  /** 전체 화면 오버레이 모드 */
  fullScreen?: boolean;
}

const sizeMap = {
  sm: 'w-4 h-4',
  md: 'w-6 h-6',
  lg: 'w-8 h-8',
  xl: 'w-12 h-12',
};

const textSizeMap = {
  sm: 'text-xs',
  md: 'text-sm',
  lg: 'text-base',
  xl: 'text-lg',
};

/**
 * 표준화된 로딩 스피너 컴포넌트
 * 앱 전체에서 일관된 로딩 상태를 표시합니다.
 */
export const LoadingSpinner: React.FC<LoadingSpinnerProps> = ({
  size = 'md',
  label,
  className,
  fullScreen = false,
}) => {
  const content = (
    <div className={cn('flex items-center justify-center gap-2', className)}>
      <Loader2 className={cn(sizeMap[size], 'animate-spin text-primary')} />
      {label && (
        <span className={cn('text-muted-foreground', textSizeMap[size])}>
          {label}
        </span>
      )}
    </div>
  );

  if (fullScreen) {
    return (
      <div className="fixed inset-0 flex items-center justify-center bg-background/80 backdrop-blur-sm z-50">
        {content}
      </div>
    );
  }

  return content;
};

interface LoadingOverlayProps {
  /** 로딩 중 여부 */
  isLoading: boolean;
  /** 자식 요소 */
  children: React.ReactNode;
  /** 로딩 라벨 */
  label?: string;
}

/**
 * 컨텐츠 위에 로딩 오버레이를 표시하는 컴포넌트
 */
export const LoadingOverlay: React.FC<LoadingOverlayProps> = ({
  isLoading,
  children,
  label,
}) => {
  return (
    <div className="relative">
      {children}
      {isLoading && (
        <div className="absolute inset-0 flex items-center justify-center bg-background/60 backdrop-blur-[2px] z-10">
          <LoadingSpinner size="lg" label={label} />
        </div>
      )}
    </div>
  );
};

interface PageLoadingProps {
  /** 로딩 메시지 */
  message?: string;
}

/**
 * 페이지 레벨 로딩 컴포넌트
 * Suspense fallback으로 사용됩니다.
 */
export const PageLoading: React.FC<PageLoadingProps> = ({
  message = 'Loading...',
}) => {
  return (
    <div className="min-h-screen flex flex-col items-center justify-center gap-4">
      <LoadingSpinner size="xl" />
      <p className="text-muted-foreground animate-pulse">{message}</p>
    </div>
  );
};

export default LoadingSpinner;
