import React, { useState, useEffect, Suspense } from "react";
import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import ErrorBoundary from "@/components/ErrorBoundary";
import { PageLoading } from "@/components/ui/loading";

// Pages - Lazy loaded for code splitting
const Index = React.lazy(() => import("./pages/Index"));
const TaskManager = React.lazy(() => import("./pages/TaskManager"));
const NotFound = React.lazy(() => import("./pages/NotFound"));

// Custom Auth Component
import { GlobalStatusBar } from "./components/GlobalStatusBar";
import { AuthProvider, useAuth } from "@/contexts/AuthProvider";
import { AuthDialog } from "@/components/AuthDialog";

const queryClient = new QueryClient();

function AppContent() {
  const { user, isChecking, isLoading, signIn, signUp, signOut, resetPassword, confirmResetPassword } = useAuth();
  const [isAuthDialogOpen, setIsAuthDialogOpen] = useState(false);

  // Show auth dialog if not checking, no user, and explicitly requested (or default behavior)
  // Here we mimic the original behavior: if no user, show dialog.
  useEffect(() => {
    if (!isChecking && !user) {
      setIsAuthDialogOpen(true);
    } else {
      setIsAuthDialogOpen(false);
    }
  }, [isChecking, user]);

  if (isChecking) {
    return <div className="flex items-center justify-center h-screen">Loading...</div>;
  }

  return (
    <>
      <AuthDialog
        open={!user && isAuthDialogOpen}
        isLoading={isLoading}
        onClose={() => { /* Prevent closing if forced login is desired */ }}
        onGoogleSignIn={() => { /* Implement Google Sign In */ }}
        onEmailSignIn={signIn}
        onSignUp={signUp}
        onResetPassword={resetPassword}
        onConfirmResetPassword={confirmResetPassword}
      />

      {user ? (
        <ErrorBoundary>
          <Suspense fallback={<PageLoading message="Loading page..." />}>
            <Routes>
              <Route path="/" element={<Index signOut={signOut} />} />
              {/* /workflows → /tasks 리다이렉트 (Task Manager 통합) */}
              <Route path="/workflows" element={<Navigate to="/tasks" replace />} />
              <Route path="/tasks" element={<TaskManager signOut={signOut} />} />
              <Route path="*" element={<NotFound />} />
            </Routes>
          </Suspense>
        </ErrorBoundary>
      ) : (
        <div className="h-screen w-full flex items-center justify-center bg-gray-50">
          <p className="text-muted-foreground">Please sign in to continue...</p>
        </div>
      )}
    </>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <Toaster />
        <Sonner />
        <AuthProvider>
          <BrowserRouter>
            <AppContent />
          </BrowserRouter>
        </AuthProvider>
      </TooltipProvider>
    </QueryClientProvider>
  );
}

export default App;
