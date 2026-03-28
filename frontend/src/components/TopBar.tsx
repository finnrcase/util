import type { ReactNode } from "react";

interface TopBarProps {
  title: string;
  subtitle?: string;
  statusItems?: Array<{ label: string; value: string }>;
  action?: ReactNode;
}

export function TopBar({ title, subtitle, statusItems = [], action }: TopBarProps) {
  return (
    <div className="flex flex-col gap-5 rounded-[1.9rem] border border-white/10 bg-[linear-gradient(180deg,rgba(15,14,28,0.92),rgba(10,10,21,0.92))] px-6 py-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.04),0_24px_60px_rgba(36,12,86,0.24)] lg:flex-row lg:items-end lg:justify-between">
      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.24em] text-violet-200">Dashboard</p>
        <h2 className="mt-3 text-2xl font-semibold tracking-[-0.03em] text-text">{title}</h2>
        {subtitle ? <p className="mt-2 max-w-3xl text-sm leading-6 text-muted">{subtitle}</p> : null}
        {statusItems.length ? (
          <div className="mt-4 flex flex-wrap gap-2.5">
            {statusItems.map((item) => (
              <span key={item.label} className="rounded-full border border-white/10 bg-white/[0.04] px-3 py-1.5 text-xs text-slate-100/90">
                <span className="text-muted">{item.label}</span>
                <span className="ml-2 font-medium text-text">{item.value}</span>
              </span>
            ))}
          </div>
        ) : null}
      </div>
      {action ? <div className="shrink-0">{action}</div> : null}
    </div>
  );
}

