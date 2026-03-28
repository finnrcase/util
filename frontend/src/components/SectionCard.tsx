import type { PropsWithChildren, ReactNode } from "react";

interface SectionCardProps extends PropsWithChildren {
  title: string;
  subtitle?: string;
  eyebrow?: string;
  action?: ReactNode;
  className?: string;
  bodyClassName?: string;
}

export function SectionCard({ title, subtitle, eyebrow, action, className = "", bodyClassName = "", children }: SectionCardProps) {
  return (
    <section className={`group relative overflow-hidden rounded-[1.8rem] border border-border/80 bg-card-surface shadow-panel ${className}`.trim()}>
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(98,163,255,0.08),transparent_26%),linear-gradient(180deg,rgba(255,255,255,0.025),transparent_30%)] opacity-90" />
      <div className="relative flex items-start justify-between gap-4 border-b border-border/80 px-6 py-5 sm:px-7">
        <div>
          {eyebrow ? <p className="text-[11px] uppercase tracking-[0.24em] text-accent/90">{eyebrow}</p> : null}
          <h2 className="mt-1 text-lg font-semibold tracking-[-0.02em] text-text">{title}</h2>
          {subtitle ? <p className="mt-2 max-w-2xl text-sm leading-6 text-muted">{subtitle}</p> : null}
        </div>
        {action}
      </div>
      <div className={`relative px-6 py-6 sm:px-7 sm:py-7 ${bodyClassName}`.trim()}>{children}</div>
    </section>
  );
}
