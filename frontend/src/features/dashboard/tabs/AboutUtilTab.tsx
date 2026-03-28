import { SectionCard } from "../../../components/SectionCard";
import { CoveragePanel } from "../../../components/CoveragePanel";
import type { CoverageResponse } from "../../../types/api";

interface AboutUtilTabProps {
  coverage?: CoverageResponse;
  isCoverageLoading: boolean;
  coverageError?: string;
}

export function AboutUtilTab({ coverage, isCoverageLoading, coverageError }: AboutUtilTabProps) {
  return (
    <div className="space-y-6">
      <SectionCard title="About Util" subtitle="" eyebrow="Product Context">
        <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_320px]">
          <div className="rounded-[1.6rem] border border-white/10 bg-white/[0.04] p-5">
            <p className="text-sm leading-7 text-slate-100/90"><strong>Util</strong> is a compute scheduling and optimization product designed to help users run workloads at the best possible times and locations in order to minimize electricity costs and carbon emissions.</p>
            
          </div>
          <div className="grid gap-3">
            <div className="rounded-[1.4rem] border border-white/10 bg-white/[0.04] p-4"><p className="text-[11px] uppercase tracking-[0.16em] text-muted">Live Carbon APIs</p><p className="mt-2 text-sm text-emerald-100">Complete</p></div>
            <div className="rounded-[1.4rem] border border-white/10 bg-white/[0.04] p-4"><p className="text-[11px] uppercase tracking-[0.16em] text-muted">Electricity Pricing APIs</p><p className="mt-2 text-sm text-text">Available for supported routes</p></div>
            <div className="rounded-[1.4rem] border border-white/10 bg-white/[0.04] p-4"><p className="text-[11px] uppercase tracking-[0.16em] text-muted">System Auto-Detection</p><p className="mt-2 text-sm text-muted">Planned</p></div>
            <div className="rounded-[1.4rem] border border-white/10 bg-white/[0.04] p-4"><p className="text-[11px] uppercase tracking-[0.16em] text-muted">Automated Control</p><p className="mt-2 text-sm text-muted">Planned</p></div>
          </div>
        </div>
      </SectionCard>

      <SectionCard title="Methodology and Coverage" subtitle="" eyebrow="Coverage">
        <CoveragePanel coverage={coverage} isLoading={isCoverageLoading} errorMessage={coverageError} />
      </SectionCard>

      <SectionCard title="Future Expansion" subtitle="" eyebrow="Roadmap">
        <div className="rounded-[1.6rem] border border-white/10 bg-white/[0.04] p-5">
          
        </div>
      </SectionCard>
    </div>
  );
}





