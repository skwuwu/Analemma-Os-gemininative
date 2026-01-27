import React, { createContext, useContext, useEffect, useState } from 'react';
import { getCurrentUser, signOut as amplifySignOut, signIn as amplifySignIn, signUp as amplifySignUp, resetPassword as amplifyResetPassword, confirmResetPassword as amplifyConfirmResetPassword } from 'aws-amplify/auth';
import { Hub } from 'aws-amplify/utils';
import { AuthUser, SignUpData } from '@/types/auth';
import { useToast } from '@/hooks/use-toast';

interface AuthContextType {
    user: AuthUser | null;
    isLoading: boolean;
    isChecking: boolean;
    signIn: (email: string, pass: string) => Promise<void>;
    signUp: (data: SignUpData) => Promise<void>;
    signOut: () => Promise<void>;
    resetPassword: (email: string) => Promise<void>;
    confirmResetPassword: (email: string, code: string, newPassword: string) => Promise<void>;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: React.ReactNode }) {
    const [user, setUser] = useState<AuthUser | null>(null);
    const [isChecking, setIsChecking] = useState(true);
    const [isLoading, setIsLoading] = useState(false);
    const { toast } = useToast();

    const checkUser = async () => {
        try {
            setIsChecking(true);
            const currentUser = await getCurrentUser();
            setUser(currentUser);
        } catch (error) {
            setUser(null);
        } finally {
            setIsChecking(false);
        }
    };

    useEffect(() => {
        checkUser();

        const listener = Hub.listen('auth', (data) => {
            switch (data.payload.event) {
                case 'signedIn':
                    checkUser();
                    break;
                case 'signedOut':
                    setUser(null);
                    break;
            }
        });

        return () => listener();
    }, []);

    const signIn = async (email: string, pass: string) => {
        try {
            setIsLoading(true);
            const { isSignedIn, nextStep } = await amplifySignIn({ username: email, password: pass });

            if (isSignedIn) {
                toast({ title: "Login successful", description: "Welcome!" });
                await checkUser();
            } else {
                console.log('Login next step:', nextStep);
                // Handle other steps if necessary
            }
        } catch (error: unknown) {
            const errorMessage = error instanceof Error ? error.message : 'An unknown error occurred';
            toast({ title: "Login failed", description: errorMessage, variant: "destructive" });
            throw error;
        } finally {
            setIsLoading(false);
        }
    };

    const signUp = async (data: SignUpData) => {
        try {
            setIsLoading(true);
            const { nextStep } = await amplifySignUp({
                username: data.email,
                password: data.password,
                options: {
                    userAttributes: {
                        email: data.email,
                        name: data.name,
                        nickname: data.nickname,
                    },
                },
            });

            if (nextStep.signUpStep === 'CONFIRM_SIGN_UP') {
                toast({ title: "인증 필요", description: "이메일로 전송된 코드를 확인해주세요." });
            }
        } catch (error: unknown) {
            const errorMessage = error instanceof Error ? error.message : 'An unknown error occurred';
            toast({ title: "회원가입 실패", description: errorMessage, variant: "destructive" });
            throw error;
        } finally {
            setIsLoading(false);
        }
    };

    const signOut = async () => {
        try {
            await amplifySignOut();
            setUser(null);
        } catch (error) {
            console.error("Error signing out: ", error);
        }
    };

    const resetPassword = async (email: string) => {
        try {
            setIsLoading(true);
            await amplifyResetPassword({ username: email });
            toast({ title: "인증 코드 발송", description: "이메일로 인증 코드를 전송했습니다." });
        } catch (error: unknown) {
            const errorMessage = error instanceof Error ? error.message : '인증 코드 발송에 실패했습니다.';
            toast({ title: "오류", description: errorMessage, variant: "destructive" });
            throw error;
        } finally {
            setIsLoading(false);
        }
    };

    const confirmResetPassword = async (email: string, code: string, newPassword: string) => {
        try {
            setIsLoading(true);
            await amplifyConfirmResetPassword({
                username: email,
                confirmationCode: code,
                newPassword
            });
            toast({ title: "비밀번호 재설정 완료", description: "비밀번호가 성공적으로 변경되었습니다." });
        } catch (error: unknown) {
            const errorMessage = error instanceof Error ? error.message : '비밀번호 재설정에 실패했습니다.';
            toast({ title: "오류", description: errorMessage, variant: "destructive" });
            throw error;
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <AuthContext.Provider value={{ user, isLoading, isChecking, signIn, signUp, signOut, resetPassword, confirmResetPassword }}>
            {children}
        </AuthContext.Provider>
    );
}

export const useAuth = () => {
    const context = useContext(AuthContext);
    if (context === undefined) {
        throw new Error('useAuth must be used within an AuthProvider');
    }
    return context;
};
