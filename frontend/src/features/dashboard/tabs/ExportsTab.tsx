import { buildExportDownloadUrl } from "../../../lib/api";
import { DataSourcePanel } from "../../../components/DataSourcePanel";
import { SectionCard } from "../../../components/SectionCard";
import type { ExportArtifact, ExportRequest, ExportResponse, OptimizeResponse } from "../../../types/api";

interface ExportsTabProps {
  canExport: boolean;
  currentPayload: ExportRequest;
  latestRun?: OptimizeResponse;
  exportResult?: ExportResponse;
  isExporting: boolean;
  exportError?: string;
  onExport: () => void;
}

const PRIMARY_EXPORT_SPECS = [
  {
    filename: "util_optimization_recommendation.csv",
    title: "Recommendation CSV",
    description: "Final recommended run window, resolved region, projected totals, and primary decision summary.",
  },
  {
    filename: "util_region_comparison.csv",
    title: "Region Comparison CSV",
    description: "Region-level comparison view for the current run, including pricing and carbon context.",
  },
  {
    filename: "util_time_window_analysis.csv",
    title: "Time Window Analysis CSV",
    description: "Candidate execution windows ranked across the available horizon for the current scenario.",
  },
  {
    filename: "util_case_comparison.csv",
    title: "Case Comparison CSV",
    description: "Case-level summary for the selected optimization objective, weights, and resulting recommendation.",
  },
  {
    filename: "util_input_assumptions.csv",
    title: "Input Assumptions CSV",
    description: "Scenario assumptions, timing rules, workload settings, and optimization configuration used for the run.",
  },
  {
    filename: "util_run_summary.csv",
    title: "Run Summary CSV",
    description: "Compact run summary with the key optimized outputs for reporting and handoff.",
  },
] as const;

const SUPPLEMENTAL_EXPORT_SPECS = [
  {
    filename: "util_data_provenance_summary.csv",
    title: "Data Provenance Summary",
    description: "Supporting provenance artifact covering pricing source, market, node, coverage, and location lookup status.",
  },
] as const;

function formatFileSize(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(1)} KB`;
  }
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function artifactByFilename(exportResult?: ExportResponse): Map<string, ExportArtifact> {
  return new Map((exportResult?.artifacts ?? []).map((artifact) => [artifact.filename, artifact]));
}

function ArtifactCard({
  title,
  description,
  artifact,
  isPrimary = true,
}: {
  title: string;
  description: string;
  artifact?: ExportArtifact;
  isPrimary?: boolean;
}) {
  const downloadUrl = artifact ? buildExportDownloadUrl(artifact.reference_path) : null;

  return (
    <div className="rounded-[1.5rem] border border-white/10 bg-white/[0.04] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.03)]">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-sm font-semibold text-text">{title}</p>
          <p className="mt-2 text-sm leading-6 text-muted">{description}</p>
        </div>
        <span className={`shrink-0 rounded-full border px-2.5 py-1 text-xs ${artifact ? "border-emerald-300/20 bg-emerald-300/10 text-emerald-100" : isPrimary ? "border-white/10 bg-black/20 text-slate-300" : "border-violet-300/20 bg-violet-300/10 text-violet-100"}`}>
          {artifact ? "Ready" : isPrimary ? "Awaiting export" : "Supplemental"}
        </span>
      </div>

      <div className="mt-4 rounded-[1.2rem] border border-white/10 bg-black/20 p-4">
        {artifact ? (
          <div className="space-y-3">
            <div>
              <p className="truncate text-sm font-medium text-slate-100">{artifact.filename}</p>
              <p className="mt-1 truncate text-xs text-muted">{artifact.reference_path}</p>
            </div>
            <div className="flex flex-wrap items-center gap-2 text-xs text-slate-300">
              <span className="rounded-full border border-white/10 bg-white/[0.04] px-2.5 py-1">{artifact.artifact_type.toUpperCase()}</span>
              <span className="rounded-full border border-white/10 bg-white/[0.04] px-2.5 py-1">{formatFileSize(artifact.size_bytes)}</span>
            </div>
            <div className="pt-1">
              <a
                href={downloadUrl ?? undefined}
                download={artifact.filename}
                className="inline-flex items-center justify-center rounded-[1rem] border border-violet-300/20 bg-violet-300/10 px-3.5 py-2 text-sm font-medium text-violet-100 transition hover:bg-violet-300/16"
              >
                Download file
              </a>
            </div>
          </div>
        ) : (
          <p className="text-sm text-muted">Generate the export package to create this artifact for the current scenario.</p>
        )}
      </div>
    </div>
  );
}

export function ExportsTab({ canExport, currentPayload, latestRun, exportResult, isExporting, exportError, onExport }: ExportsTabProps) {
  const artifactMap = artifactByFilename(exportResult);
  const primaryArtifactsReady = PRIMARY_EXPORT_SPECS.filter((spec) => artifactMap.has(spec.filename)).length;

  return (
    <div className="space-y-6">
      <SectionCard title="Export Package" subtitle="Generate the structured CSV package for the current run and review the primary export outputs in one place." eyebrow="Exports">
        <div className="grid gap-6 xl:grid-cols-[minmax(0,1.15fr)_360px]">
          <div className="space-y-6">
            <div className="rounded-[1.6rem] border border-white/10 bg-[linear-gradient(180deg,rgba(255,255,255,0.05),rgba(255,255,255,0.03))] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
              <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                <div className="max-w-2xl">
                  <p className="text-[11px] uppercase tracking-[0.16em] text-violet-200">Current package</p>
                  <h3 className="mt-2 text-xl font-semibold text-text">Six primary export outputs, organized for reporting and handoff</h3>
                  <p className="mt-2 text-sm leading-6 text-muted">This package keeps the legacy export structure from the Streamlit MVP: recommendation, region comparison, time window analysis, case comparison, input assumptions, and run summary.</p>
                </div>
                <div className="flex flex-wrap gap-3">
                  <button type="button" onClick={onExport} disabled={!canExport || isExporting} className="inline-flex items-center justify-center rounded-[1.25rem] bg-gradient-to-r from-violet-300 via-fuchsia-400 to-cyan-300 px-5 py-3 text-sm font-semibold text-slate-950 shadow-[0_16px_36px_rgba(139,92,246,0.32)] transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-60">
                    {isExporting ? "Generating Export..." : "Generate Export Package"}
                  </button>
                </div>
              </div>

              <div className="mt-5 grid gap-3 sm:grid-cols-3">
                <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                  <p className="text-[11px] uppercase tracking-[0.16em] text-muted">ZIP</p>
                  <p className="mt-2 text-sm text-text">{currentPayload.zip_code}</p>
                </div>
                <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                  <p className="text-[11px] uppercase tracking-[0.16em] text-muted">Objective</p>
                  <p className="mt-2 text-sm text-text">{currentPayload.objective}</p>
                </div>
                <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                  <p className="text-[11px] uppercase tracking-[0.16em] text-muted">Cloud Upload</p>
                  <p className="mt-2 text-sm text-text">{currentPayload.enable_cloud_upload ? "Enabled" : "Disabled"}</p>
                </div>
              </div>

              {!canExport ? <div className="mt-4 rounded-2xl border border-white/10 bg-black/20 p-4 text-sm text-muted">Run the optimizer first so the export package reflects a live result instead of empty placeholders.</div> : null}
              {exportError ? <div className="mt-4 rounded-2xl border border-danger/25 bg-danger/10 p-4 text-sm text-red-100">{exportError}</div> : null}
            </div>

            <div className="space-y-4">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <p className="text-[11px] uppercase tracking-[0.16em] text-violet-200">Primary exports</p>
                  <p className="mt-1 text-sm text-muted">These are the six structured CSV outputs that existed in the previous app workflow.</p>
                </div>
                <div className="rounded-full border border-white/10 bg-white/[0.04] px-3 py-1.5 text-xs text-slate-200">
                  {primaryArtifactsReady} / {PRIMARY_EXPORT_SPECS.length} ready
                </div>
              </div>

              <div className="grid gap-4 xl:grid-cols-2">
                {PRIMARY_EXPORT_SPECS.map((spec) => (
                  <ArtifactCard key={spec.filename} title={spec.title} description={spec.description} artifact={artifactMap.get(spec.filename)} />
                ))}
              </div>
            </div>

            <div className="space-y-4">
              <div>
                <p className="text-[11px] uppercase tracking-[0.16em] text-violet-200">Supplemental export</p>
                <p className="mt-1 text-sm text-muted">Additional provenance output kept separate from the six primary CSVs.</p>
              </div>
              <div className="grid gap-4 xl:grid-cols-1">
                {SUPPLEMENTAL_EXPORT_SPECS.map((spec) => (
                  <ArtifactCard key={spec.filename} title={spec.title} description={spec.description} artifact={artifactMap.get(spec.filename)} isPrimary={false} />
                ))}
              </div>
            </div>
          </div>

          <div className="space-y-4">
            <div className="rounded-[1.6rem] border border-white/10 bg-white/[0.04] p-5">
              <p className="text-[11px] uppercase tracking-[0.16em] text-violet-200">Package status</p>
              <div className="mt-4 space-y-3">
                <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                  <p className="text-[11px] uppercase tracking-[0.16em] text-muted">Run ID</p>
                  <p className="mt-2 break-all text-sm text-text">{exportResult?.run_id ?? "Awaiting export generation"}</p>
                </div>
                <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                  <p className="text-[11px] uppercase tracking-[0.16em] text-muted">Export Directory</p>
                  <p className="mt-2 break-all text-sm text-text">{exportResult?.export_dir ?? "Will be created on export"}</p>
                </div>
                <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                  <p className="text-[11px] uppercase tracking-[0.16em] text-muted">Artifacts Generated</p>
                  <p className="mt-2 text-sm text-text">{exportResult?.summary?.artifact_count ?? 0}</p>
                </div>
                <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                  <p className="text-[11px] uppercase tracking-[0.16em] text-muted">Cloud Upload</p>
                  <p className="mt-2 text-sm text-text">{exportResult ? (exportResult.cloud_upload_enabled ? "Enabled for this export" : "Disabled for this export") : currentPayload.enable_cloud_upload ? "Enabled in form settings" : "Disabled"}</p>
                  {exportResult?.cloud_message ? <p className="mt-2 text-xs leading-5 text-muted">{exportResult.cloud_message}</p> : null}
                </div>
              </div>
            </div>

            {latestRun ? (
              <SectionCard title="Current Run Provenance" subtitle="The export package reflects the latest optimization result context and live/fallback routing state." eyebrow="Provenance">
                <DataSourcePanel provenance={latestRun.provenance} pricing={latestRun.pricing} />
              </SectionCard>
            ) : null}

            <div className="rounded-[1.6rem] border border-white/10 bg-white/[0.04] p-5">
              <p className="text-[11px] uppercase tracking-[0.16em] text-violet-200">What this package covers</p>
              <div className="mt-4 space-y-3 text-sm text-slate-100/90">
                <p>Recommendation and run-summary outputs for stakeholders</p>
                <p>Window-by-window analysis for deeper review</p>
                <p>Region, pricing, and provenance context for auditability</p>
                <p>Input assumptions for reproducibility</p>
              </div>
            </div>
          </div>
        </div>
      </SectionCard>
    </div>
  );
}
