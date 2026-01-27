import React, { useState, useEffect, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { useShallow } from 'zustand/react/shallow';

// Static imports to avoid module initialization order issues in bundled output
import { WorkflowCanvas } from '@/components/WorkflowCanvas.tsx';
import { BlockLibrary } from '@/components/BlockLibrary.tsx';
import { SavedWorkflows } from '@/components/SavedWorkflows.tsx';
import { WorkflowChat } from '@/components/WorkflowChat';
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
      toast.error('새 비밀번호가 일치하지 않습니다.');
      return;
    }

    if (newPassword.length < 8) {
      toast.error('비밀번호는 최소 8자 이상이어야 합니다.');
      return;
    }

    try {
      await updatePassword({
        oldPassword,
        newPassword
      });
      toast.success('비밀번호가 성공적으로 변경되었습니다.');
      setIsPasswordDialogOpen(false);
      setOldPassword('');
      setNewPassword('');
      setConfirmPassword('');
    } catch (error: any) {
      console.error('Password change error:', error);
      toast.error(error.message || '비밀번호 변경에 실패했습니다.');
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
                  계정 관리
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem onClick={handleSignOut} className="flex items-center gap-2">
                  <LogOut className="w-4 h-4" />
                  로그아웃
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem className="flex items-center gap-2">
                  <BarChart3 className="w-4 h-4" />
                  사용량
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => setIsPasswordDialogOpen(true)} className="flex items-center gap-2">
                  <Key className="w-4 h-4" />
                  비밀번호 변경
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
            
            {/* Password Dialog */}
            <Dialog open={isPasswordDialogOpen} onOpenChange={setIsPasswordDialogOpen}>
              <DialogContent className="sm:max-w-[425px]">
                <DialogHeader>
                  <DialogTitle>비밀번호 변경</DialogTitle>
                  <DialogDescription>
                    현재 비밀번호와 새 비밀번호를 입력하세요.
                  </DialogDescription>
                </DialogHeader>
                <div className="grid gap-4 py-4">
                  <div className="grid grid-cols-4 items-center gap-4">
                    <Label htmlFor="old-password" className="text-right">
                      현재 비밀번호
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
                      새 비밀번호
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
                      비밀번호 확인
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
                    변경하기
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
            <BlockLibrary />
          </div>

          {/* Center: Main Canvas */}
          <div className="flex-1 relative bg-background bg-[radial-gradient(#1e293b_1px,transparent_1px)] [background-size:20px_20px]">
            <WorkflowCanvas />
          </div>

          {/* Right: CRUD Operations (60%) + AI Chat (40%) */}
          <div className="w-[350px] flex-shrink-0 border-l border-border flex flex-col bg-background z-10">
            {/* Top: Saved Workflows CRUD */}
            <div className="h-[60%] border-b border-border overflow-hidden">
              <SavedWorkflows
                currentWorkflow={currentWorkflow}
                onLoadWorkflow={handleLoadWorkflow}
              />
            </div>
            
            {/* Bottom: AI Codesigner Chat */}
            <div className="h-[40%] overflow-hidden">
              <WorkflowChat />
            </div>
          </div>
        </div>
      </div>
    </>
  );
};

export default Index;
