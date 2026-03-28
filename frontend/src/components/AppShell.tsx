import type { PropsWithChildren, ReactNode } from "react";

interface AppShellProps extends PropsWithChildren {
  sidebar: ReactNode;
}

export function AppShell({ sidebar, children }: AppShellProps) {
  return (
    <div className="min-h-screen bg-app text-text">
      <div className="relative mx-auto flex min-h-screen max-w-[1820px] gap-0 px-3 py-3 sm:px-4 lg:px-5">
        <div className="pointer-events-none absolute inset-x-0 top-0 h-64 bg-[radial-gradient(circle_at_24%_10%,rgba(139,92,246,0.26),transparent_30%),radial-gradient(circle_at_76%_6%,rgba(34,211,238,0.10),transparent_20%)]" />
        <aside className="relative hidden w-[300px] shrink-0 lg:block">{sidebar}</aside>
        <div className="relative flex min-h-[calc(100vh-1.5rem)] flex-1 flex-col overflow-hidden rounded-[2rem] border border-white/10 bg-panel/78 shadow-shell backdrop-blur-2xl">
          <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_left,rgba(168,85,247,0.16),transparent_28%),radial-gradient(circle_at_top_right,rgba(59,130,246,0.10),transparent_24%),linear-gradient(180deg,rgba(255,255,255,0.03),transparent_16%)]" />
          <div className="relative flex min-h-[calc(100vh-1.5rem)] flex-col">{children}</div>
        </div>
      </div>
    </div>
  );
}
