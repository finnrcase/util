import { SectionCard } from "../../../components/SectionCard";
import { CoveragePanel } from "../../../components/CoveragePanel";
import type { CoverageResponse } from "../../../types/api";

interface CoverageTabProps {
  coverage?: CoverageResponse;
  isLoading: boolean;
  errorMessage?: string;
}

export function CoverageTab({ coverage, isLoading, errorMessage }: CoverageTabProps) {
  return (
    <SectionCard title="Market Coverage" subtitle="Supported live markets, partial support boundaries, and fallback behavior." eyebrow="Coverage">
      <CoveragePanel coverage={coverage} isLoading={isLoading} errorMessage={errorMessage} />
    </SectionCard>
  );
}
