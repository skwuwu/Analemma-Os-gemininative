import React, { useState, useEffect, useMemo, lazy, Suspense } from 'react';
import { useNavigate } from 'react-router-dom';
import { useShallow } from 'zustand/react/shallow';

// Lazy load heavy components to prevent them from being bundled in Index chunk
// This avoids circular dependency initialization issues
const WorkflowCanvas = lazy(() => import('@/components/WorkflowCanvas.tsx').then(m => ({ default: m.WorkflowCanvas })));
const BlockLibrary = lazy(() => import('@/components/BlockLibrary.tsx').then(m => ({ default: m.BlockLibrary })));
const SavedWorkflows = lazy(() => import('@/components/SavedWorkflows.tsx').then(m => ({ default: m.SavedWorkflows })));
const WorkflowChat = lazy(() => import('@/components/WorkflowChat').then(m => ({ default: m.WorkflowChat })));

import { ActiveWorkflowIndicator } from '@/components/ActiveWorkflowIndicator.tsx';
import { Button } from '@/components/ui/button.tsx';
import { Badge } from '@/components/ui/badge';
import { LogOut, Activity } from 'lucide-react';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger, DropdownMenuSeparator } from '@/components/ui/dropdown-menu';
import { User, BarChart3, Key } from 'lucide-react';
import { updatePassword } from 'aws-amplify/auth';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';

import { toast } from 'sonner';
import { useNotifications } from '@/hooks/useNotifications';
import { useWorkflowStore } from '@/lib/workflowStore';
import { Node, Edge } from '@xyflow/react';

// signOut을 props로 받도록 인터페이스 정의
interface IndexProps {
  signOut?: () => void;
}

interface WorkflowData {
  id?: string;
  workflowId?: string;
  name?: string;
  nodes: Node[];
  edges: Edge[];
  inputs?: Record<string, any>;
}


const Index = ({ signOut }: IndexProps) => {
  const navigate = useNavigate();
  const {
    nodes,
    edges,
    loadWorkflow,
    currentWorkflowId,
    currentWorkflowName,
    currentWorkflowInputs,
    setCurrentWorkflow
  } = useWorkflowStore(
    useShallow((state) => ({
      nodes: state.nodes,
      edges: state.edges,
      loadWorkflow: state.loadWorkflow,
      currentWorkflowId: state.currentWorkflowId,
      currentWorkflowName: state.currentWorkflowName,
      currentWorkflowInputs: state.currentWorkflowInputs,
      setCurrentWorkflow: state.setCurrentWorkflow,
    }))
  );

  const [isPasswordDialogOpen, setIsPasswordDialogOpen] = useState(false);

  const [oldPassword, setOldPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');

  const currentWorkflow = useMemo(() => ({
    id: currentWorkflowId,
    name: currentWorkflowName,
    nodes,
    edges,
    inputs: currentWorkflowInputs,
  }), [currentWorkflowId, currentWorkflowName, nodes, edges, currentWorkflowInputs]);
  // canvas ref removed — store manages workflow state
  const { notifications } = useNotifications({});

  const handleLoadWorkflow = (workflow: WorkflowData) => {
    setCurrentWorkflow(workflow.id, workflow.name, workflow.inputs);
    loadWorkflow({ nodes: workflow.nodes, edges: workflow.edges });
  };

  const handleSignOut = async () => {
    try {
      if (signOut) {
        await signOut();
      } else {
        toast.error('Sign out function is not available.');
      }
    } catch (error) {
      toast.error('Failed to log out. Please try again.');
    }
  };

  const handleChangePassword = () => {
    setIsPasswordDialogOpen(true);
  };

  const handlePasswordSubmit = async () => {
    if (newPassword !== confirmPassword) {
      toast.error('New passwords do not match.');
      return;
    }

    if (newPassword.length < 8) {
      toast.error('Password must be at least 8 characters.');
      return;
    }

    try {
      await updatePassword({
        oldPassword,
        newPassword
      });
      toast.success('Password changed successfully.');
      setIsPasswordDialogOpen(false);
      setOldPassword('');
      setNewPassword('');
      setConfirmPassword('');
    } catch (error: any) {
      console.error('Password change error:', error);
      toast.error(error.message || 'Failed to change password.');
    }
  };

  return (
    <>
      <div className="flex flex-col h-screen w-full bg-background overflow-hidden">
        <div className="flex justify-between p-4 border-b items-center gap-2 bg-background z-20">
          <div className="font-bold text-xl tracking-tight text-foreground">Analemma</div>
          
          {/* 알림 및 로그아웃 버튼 */}
          <div className="flex items-center gap-2">
            <ActiveWorkflowIndicator />
            
            <Button
              variant="outline"
              size="sm"
              onClick={() => navigate('/tasks')}
              className="flex items-center gap-2"
            >
              <Activity className="w-4 h-4" />
              Task Manager
              {notifications && notifications.filter(n => !n.read).length > 0 && (
                <Badge className="ml-2">{notifications.filter(n => !n.read).length}</Badge>
              )}
            </Button>
            {/* Notifications dialog removed; monitor shows notifications */}
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button
                  variant="outline"
                  size="sm"
                  className="flex items-center gap-2"
                >
                  <User className="w-4 h-4" />
                  Account
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem onClick={handleSignOut} className="flex items-center gap-2">
                  <LogOut className="w-4 h-4" />
                  Sign Out
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem onClick={() => toast.info('Usage statistics coming soon')} className="flex items-center gap-2">
                  <BarChart3 className="w-4 h-4" />
                  Usage
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => setIsPasswordDialogOpen(true)} className="flex items-center gap-2">
                  <Key className="w-4 h-4" />
                  Change Password
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
            
            {/* Password Dialog */}
            <Dialog open={isPasswordDialogOpen} onOpenChange={setIsPasswordDialogOpen}>
              <DialogContent className="sm:max-w-[425px]">
                <DialogHeader>
                  <DialogTitle>Change Password</DialogTitle>
                  <DialogDescription>
                    Enter your current password and new password.
                  </DialogDescription>
                </DialogHeader>
                <div className="grid gap-4 py-4">
                  <div className="grid grid-cols-4 items-center gap-4">
                    <Label htmlFor="old-password" className="text-right">
                      Current Password
                    </Label>
                    <Input
                      id="old-password"
                      type="password"
                      value={oldPassword}
                      onChange={(e) => setOldPassword(e.target.value)}
                      className="col-span-3"
                    />
                  </div>
                  <div className="grid grid-cols-4 items-center gap-4">
                    <Label htmlFor="new-password" className="text-right">
                      New Password
                    </Label>
                    <Input
                      id="new-password"
                      type="password"
                      value={newPassword}
                      onChange={(e) => setNewPassword(e.target.value)}
                      className="col-span-3"
                    />
                  </div>
                  <div className="grid grid-cols-4 items-center gap-4">
                    <Label htmlFor="confirm-password" className="text-right">
                      Confirm Password
                    </Label>
                    <Input
                      id="confirm-password"
                      type="password"
                      value={confirmPassword}
                      onChange={(e) => setConfirmPassword(e.target.value)}
                      className="col-span-3"
                    />
                  </div>
                </div>
                <DialogFooter>
                  <Button type="submit" onClick={handlePasswordSubmit}>
                    Change Password
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>


          </div>
        </div>

        {/* 4-Pane Layout */}
        <div className="flex flex-1 overflow-hidden">
          {/* Left: Node Inventory (260px) */}
          <div className="w-[260px] flex-shrink-0 border-r border-border bg-background flex flex-col z-10">
            <Suspense fallback={<div className="flex items-center justify-center h-full"><div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary" /></div>}>
              <BlockLibrary />
            </Suspense>
          </div>

          {/* Center: Main Canvas */}
          <div className="flex-1 relative bg-background bg-[radial-gradient(#1e293b_1px,transparent_1px)] [background-size:20px_20px]">
            <Suspense fallback={<div className="flex items-center justify-center h-full"><div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary" /></div>}>
              <WorkflowCanvas />
            </Suspense>
          </div>

          {/* Right: CRUD Operations (60%) + AI Chat (40%) */}
          <div className="w-[350px] flex-shrink-0 border-l border-border flex flex-col bg-background z-10">
            {/* Top: Saved Workflows CRUD */}
            <div className="h-[60%] border-b border-border overflow-hidden">
              <Suspense fallback={<div className="flex items-center justify-center h-full"><div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary" /></div>}>
                <SavedWorkflows
                  currentWorkflow={currentWorkflow}
                  onLoadWorkflow={handleLoadWorkflow}
                />
              </Suspense>
            </div>
            
            {/* Bottom: AI Codesigner Chat */}
            <div className="h-[40%] overflow-hidden">
              <Suspense fallback={<div className="flex items-center justify-center h-full"><div className="animate-spin rounded-full h-6 w-6 border-b-2 border-primary" /></div>}>
                <WorkflowChat />
              </Suspense>
            </div>
          </div>
        </div>
      </div>
    </>
  );
};

export default Index;
