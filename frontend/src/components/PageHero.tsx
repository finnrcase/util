import type { ReactNode } from "react";

interface PageHeroProps {
  eyebrow: string;
  title: string;
  description: string;
  meta?: Array<{ label: string; value: string }>;
  action?: ReactNode;
}

export function PageHero({ eyebrow, title, description, meta = [], action }: PageHeroProps) {
  return (
    <section className="relative overflow-hidden rounded-[2.15rem] border border-white/10 bg-hero-panel px-6 py-7 shadow-panel sm:px-7 lg:px-8">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_left,rgba(168,85,247,0.22),transparent_24%),radial-gradient(circle_at_top_right,rgba(96,165,250,0.12),transparent_22%),radial-gradient(circle_at_70%_80%,rgba(192,132,252,0.10),transparent_26%)]" />
      <div className="absolute inset-[1px] rounded-[2.05rem] border border-white/6" />
      <div className="relative flex flex-col gap-6 xl:flex-row xl:items-end xl:justify-between">
        <div className="max-w-3xl">
          <p className="text-xs uppercase tracking-[0.34em] text-violet-200">{eyebrow}</p>
          <h1 className="mt-4 text-3xl font-semibold tracking-[-0.04em] text-text sm:text-4xl xl:text-[2.75rem]">{title}</h1>
          <p className="mt-4 max-w-2xl text-sm leading-7 text-muted sm:text-base">{description}</p>
          {meta.length ? (
            <div className="mt-6 flex flex-wrap gap-3">
              {meta.map((item) => (
                <div key={item.label} className="rounded-full border border-white/10 bg-white/[0.05] px-4 py-2 text-sm text-slate-100/90 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
                  <span className="text-muted">{item.label}</span>
                  <span className="ml-2 font-medium text-text">{item.value}</span>
                </div>
              ))}
            </div>
          ) : null}
        </div>
        {action ? <div className="relative">{action}</div> : null}
      </div>
    </section>
  );
}
