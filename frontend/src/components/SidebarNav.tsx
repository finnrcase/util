import { useState, type ComponentType } from "react";
import type { LucideProps } from "lucide-react";
import { CUSTOM_BRAND_LOGO_PATH } from "../lib/branding";

export interface SidebarItem {
  id: string;
  label: string;
  description: string;
  icon: ComponentType<LucideProps>;
}

interface SidebarNavProps {
  items: SidebarItem[];
  activeItemId: string;
  onNavigate: (id: string) => void;
}

function SidebarIcon({ icon: Icon, active }: { icon: ComponentType<LucideProps>; active: boolean }) {
  return (
    <span className={`relative flex h-10 w-10 items-center justify-center rounded-2xl border ${active ? "border-violet-300/30 bg-[linear-gradient(180deg,rgba(167,139,250,0.24),rgba(96,165,250,0.08))] text-violet-100 shadow-[0_0_28px_rgba(139,92,246,0.22)]" : "border-white/10 bg-white/[0.04] text-muted"}`}>
      <Icon className="h-[18px] w-[18px]" strokeWidth={2.1} />
    </span>
  );
}

function BrandMark() {
  const [logoFailed, setLogoFailed] = useState(false);

  if (!logoFailed) {
    return (
      <div className="flex h-20 w-20 items-center justify-center overflow-hidden rounded-[1.7rem] border border-violet-200/15 bg-[linear-gradient(180deg,rgba(255,255,255,0.08),rgba(255,255,255,0.02))] shadow-[0_18px_38px_rgba(139,92,246,0.26)]">
        <img
          src={CUSTOM_BRAND_LOGO_PATH}
          alt="UTIL logo"
          className="h-full w-full object-contain p-2.5"
          onError={() => setLogoFailed(true)}
        />
      </div>
    );
  }

  return (
    <div className="flex h-20 w-20 items-center justify-center rounded-[1.7rem] bg-gradient-to-br from-violet-300 via-fuchsia-400 to-cyan-300 text-2xl font-semibold text-slate-950 shadow-[0_18px_38px_rgba(139,92,246,0.34)]">
      U
    </div>
  );
}

export function SidebarNav({ items, activeItemId, onNavigate }: SidebarNavProps) {
  return (
    <div className="sticky top-3 flex h-[calc(100vh-1.5rem)] flex-col overflow-hidden rounded-[2rem] border border-white/10 bg-sidebar/92 px-5 py-6 shadow-sidebar backdrop-blur-2xl">
      <div className="absolute inset-0 rounded-[2rem] bg-[radial-gradient(circle_at_top_left,rgba(168,85,247,0.18),transparent_26%),radial-gradient(circle_at_bottom_right,rgba(34,211,238,0.07),transparent_22%)]" />

      <div className="relative shrink-0 rounded-[1.7rem] border border-white/10 bg-white/[0.04] p-4 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
        <div className="grid grid-cols-[auto_1fr] items-center gap-4">
          <BrandMark />
          <div className="flex justify-center pr-1">
            <p className="text-[1.85rem] font-black uppercase tracking-[0.16em] text-violet-50">UTIL</p>
          </div>
        </div>
      </div>

      <div className="relative mt-8 flex min-h-0 flex-1 flex-col overflow-hidden">
        <p className="shrink-0 px-2 text-xs uppercase tracking-[0.22em] text-muted">Workspace</p>
        <nav className="sidebar-scroll mt-4 flex-1 space-y-2.5 overflow-y-auto overflow-x-hidden pr-1">
          {items.map((item) => {
            const active = item.id === activeItemId;
            return (
              <button
                key={item.id}
                type="button"
                onClick={() => onNavigate(item.id)}
                className={`flex w-full items-center gap-3 rounded-[1.45rem] border px-3 py-3 text-left transition duration-200 ${active ? "border-violet-300/20 bg-[linear-gradient(90deg,rgba(139,92,246,0.22),rgba(34,211,238,0.08))] shadow-[inset_0_1px_0_rgba(255,255,255,0.05),0_16px_30px_rgba(75,0,130,0.16)]" : "border-transparent bg-transparent hover:border-white/10 hover:bg-white/[0.04]"}`}
              >
                <SidebarIcon icon={item.icon} active={active} />
                <span className="min-w-0">
                  <span className={`block text-sm font-medium ${active ? "text-text" : "text-slate-100/90"}`}>{item.label}</span>
                  <span className="mt-1 block text-xs leading-5 text-muted">{item.description}</span>
                </span>
              </button>
            );
          })}
        </nav>
      </div>

      <div className="relative mt-5 shrink-0 rounded-[1.5rem] border border-white/10 bg-white/[0.04] p-4 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
        <p className="text-xs uppercase tracking-[0.18em] text-violet-200">Signals</p>
        <div className="mt-3 flex items-center justify-between">
          <span className="text-sm text-slate-100/90">Price</span>
          <span className="rounded-full border border-violet-300/20 bg-violet-300/10 px-2.5 py-1 text-xs text-violet-100">Live-capable</span>
        </div>
        <div className="mt-2 flex items-center justify-between">
          <span className="text-sm text-slate-100/90">Carbon</span>
          <span className="rounded-full border border-cyan-300/20 bg-cyan-300/10 px-2.5 py-1 text-xs text-cyan-100">Forecast</span>
        </div>
      </div>
    </div>
  );
}
