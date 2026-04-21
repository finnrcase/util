import type { FormEventHandler } from "react";
import type { FieldErrors, UseFormRegister } from "react-hook-form";
import { SectionCard } from "../../../components/SectionCard";
import { AiDecisionSummaryCard } from "../../../components/AiDecisionSummaryCard";
import type { FormInputValues } from "../form";
import type { OptimizeResponse } from "../../../types/api";

interface OptimizerTabProps {
  register: UseFormRegister<FormInputValues>;
  errors: FieldErrors<FormInputValues>;
  onSubmit: FormEventHandler<HTMLFormElement>;
  isSubmitting: boolean;
  isBackendReady?: boolean;
  errorMessage?: string;
  lastRun?: OptimizeResponse;
  values: Partial<FormInputValues>;
}

function SectionLabel({ title, subtitle }: { title: string; subtitle?: string }) {
  return (
    <div className="mb-3 flex items-start justify-between gap-4 border-b border-white/8 pb-3">
      <div>
        <p className="text-[11px] uppercase tracking-[0.2em] text-violet-200/90">{title}</p>
        {subtitle ? <p className="mt-1.5 max-w-2xl text-sm leading-6 text-muted">{subtitle}</p> : null}
      </div>
    </div>
  );
}

function fieldClass(hasError?: boolean) {
  return `w-full rounded-[1.15rem] border px-4 py-3 text-slate-100 placeholder:text-slate-400/70 outline-none transition shadow-[inset_0_1px_0_rgba(255,255,255,0.03)] ${hasError ? "border-danger/50 bg-danger/5 focus:border-danger/60 focus:shadow-[0_0_0_3px_rgba(239,68,68,0.12)]" : "border-white/10 bg-[#0d1220]/88 focus:border-violet-300/45 focus:bg-[#10172a] focus:shadow-[0_0_0_3px_rgba(139,92,246,0.14)]"}`;
}

function selectClass(hasError?: boolean) {
  return `${fieldClass(hasError)} appearance-none bg-[linear-gradient(180deg,rgba(15,22,37,0.96),rgba(10,14,25,0.96))] pr-11`;
}

export function OptimizerTab({ register, errors, onSubmit, isSubmitting, isBackendReady = true, errorMessage, lastRun, values }: OptimizerTabProps) {
  const isExtendedMode = values.carbon_estimation_mode === "forecast_plus_historical_expectation";
  const runWindow = lastRun?.schedule.recommended_window;

  return (
    <div className="grid gap-6">
    {lastRun ? <AiDecisionSummaryCard optimizeResponse={lastRun} /> : null}
    <div className="grid gap-6 xl:grid-cols-[minmax(0,1.35fr)_360px] xl:items-start">
      <form className="grid gap-5" onSubmit={onSubmit}>
        <div className="flex flex-wrap items-center gap-3">
          <button
            type="submit"
            disabled={isSubmitting || !isBackendReady}
            className="inline-flex min-h-[3.5rem] items-center justify-center gap-2 rounded-[1.35rem] bg-gradient-to-r from-violet-200 via-fuchsia-300 to-cyan-200 px-6 py-3 text-base font-semibold text-slate-950 shadow-[0_18px_42px_rgba(139,92,246,0.38)] ring-1 ring-white/10 transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {isSubmitting ? (
              <span
                aria-hidden="true"
                className="relative inline-flex h-5 w-5 items-center justify-center"
              >
                <span className="absolute inset-0 rounded-full border-2 border-slate-950/20" />
                <span className="absolute inset-0 animate-spin rounded-full border-2 border-transparent border-t-slate-950/90 border-r-slate-950/60" />
              </span>
            ) : (
              <span className="text-lg leading-none">+</span>
            )}
            <span>{isSubmitting ? "Optimizing..." : "Run Optimization"}</span>
          </button>
          {isSubmitting ? (
            <div className="rounded-[1.2rem] border border-violet-300/14 bg-[linear-gradient(180deg,rgba(167,139,250,0.08),rgba(167,139,250,0.02))] px-4 py-3 text-sm leading-6 text-slate-100/90 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
              Processing: resolving location, loading forecast, fetching pricing, and solving the schedule.
            </div>
          ) : null}
          {lastRun ? <span className="rounded-full border border-emerald-300/20 bg-emerald-300/10 px-3 py-2 text-sm font-medium text-emerald-100">Optimization ran</span> : null}
        </div>

        <SectionCard
          title="Optimization Control Center"
          subtitle=""
          eyebrow="Optimizer"
          bodyClassName="space-y-5"
        >
          {errorMessage ? (
            <div className="rounded-[1.3rem] border border-danger/25 bg-danger/10 px-4 py-3 text-sm leading-6 text-red-100">
              {errorMessage}
            </div>
          ) : null}
          <section>
            <SectionLabel title="Workload" />
            <div className="grid gap-4 md:grid-cols-2">
              <label className="space-y-2">
                <span className="text-sm font-medium text-slate-100">Compute Hours Required</span>
                <input type="number" min={1} max={72} className={fieldClass(Boolean(errors.compute_hours_required))} {...register("compute_hours_required")} />
                {errors.compute_hours_required ? <span className="text-sm text-danger">{errors.compute_hours_required.message}</span> : null}
              </label>

              <label className="space-y-2">
                <span className="text-sm font-medium text-slate-100">Machine Wattage (Watts)</span>
                <input type="number" min={50} max={500000} className={fieldClass(Boolean(errors.machine_watts))} {...register("machine_watts")} />
                {errors.machine_watts ? <span className="text-sm text-danger">{errors.machine_watts.message}</span> : null}
              </label>

              <label className="space-y-2 md:col-span-2">
                <span className="text-sm font-medium text-slate-100">Optimization Objective</span>
                <div className="relative">
                  <select className={selectClass(Boolean(errors.objective))} {...register("objective")}>
                    <option value="cost">Minimize Cost</option>
                    <option value="carbon">Minimize Carbon</option>
                    <option value="balanced">Balanced</option>
                  </select>
                  <span className="pointer-events-none absolute inset-y-0 right-4 flex items-center text-slate-300">v</span>
                </div>
              </label>
            </div>
          </section>

          <section>
            <SectionLabel title="Location & Timing" />
            <div className="grid gap-4 md:grid-cols-2">
              <label className="space-y-2">
                <span className="text-sm font-medium text-slate-100">ZIP Code</span>
                <input className={fieldClass(Boolean(errors.zip_code))} {...register("zip_code")} />
                {errors.zip_code ? <span className="text-sm text-danger">{errors.zip_code.message}</span> : null}
              </label>

              <label className="space-y-2">
                <span className="text-sm font-medium text-slate-100">Deadline</span>
                <input type="datetime-local" className={fieldClass(Boolean(errors.deadline))} {...register("deadline")} />
                {errors.deadline ? <span className="text-sm text-danger">{errors.deadline.message}</span> : null}
              </label>
            </div>
            <div className="mt-4 rounded-[1.2rem] border border-violet-300/14 bg-[linear-gradient(180deg,rgba(167,139,250,0.08),rgba(167,139,250,0.02))] px-4 py-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
              <p className="text-xs text-slate-100/90">ZIP-driven region and pricing lookup is active.</p>
            </div>
          </section>

          <section>
            <SectionLabel title="Forecast Settings" />
            <div className="grid gap-4 lg:grid-cols-2">
              <fieldset className="space-y-2 rounded-[1.25rem] border border-white/10 bg-white/[0.04] p-4">
                <legend className="px-1 text-sm font-medium text-slate-100">Carbon Estimate Type</legend>
                <label className="flex cursor-pointer items-start gap-3 rounded-2xl border border-white/10 bg-black/20 p-3 transition hover:border-violet-300/20 hover:bg-white/[0.04]">
                  <input type="radio" value="forecast_only" className="mt-1 h-4 w-4 accent-violet-300" {...register("carbon_estimation_mode")} />
                  <span className="min-w-0 text-sm text-text">Short-Term (Live Data - 24 hour access)</span>
                </label>
                <label className="flex cursor-pointer items-start gap-3 rounded-2xl border border-white/10 bg-black/20 p-3 transition hover:border-violet-300/20 hover:bg-white/[0.04]">
                  <input type="radio" value="forecast_plus_historical_expectation" className="mt-1 h-4 w-4 accent-violet-300" {...register("carbon_estimation_mode")} />
                  <span className="min-w-0 text-sm text-text">Extended (Historical-Pattern Estimate)</span>
                </label>
              </fieldset>

              <fieldset className="space-y-2 rounded-[1.25rem] border border-white/10 bg-white/[0.04] p-4">
                <legend className="px-1 text-sm font-medium text-slate-100">Scheduling Strategy</legend>
                <label className="flex cursor-pointer items-start gap-3 rounded-2xl border border-white/10 bg-black/20 p-3 transition hover:border-violet-300/20 hover:bg-white/[0.04]">
                  <input type="radio" value="flexible" className="mt-1 h-4 w-4 accent-violet-300" {...register("schedule_mode")} />
                  <span className="min-w-0 text-sm text-text">Flexible</span>
                </label>
                <label className="flex cursor-pointer items-start gap-3 rounded-2xl border border-white/10 bg-black/20 p-3 transition hover:border-violet-300/20 hover:bg-white/[0.04]">
                  <input type="radio" value="block" className="mt-1 h-4 w-4 accent-violet-300" {...register("schedule_mode")} />
                  <span className="min-w-0 text-sm text-text">Continuous Block</span>
                </label>
              </fieldset>
            </div>
          </section>

          <section>
            <SectionLabel title="Output & Run Actions" />
            <div className="space-y-4">
              <label className="flex items-start gap-3 rounded-[1.25rem] border border-white/10 bg-white/[0.04] p-4">
                <input type="checkbox" className="mt-1 h-4 w-4 rounded accent-violet-300" {...register("enable_cloud_upload")} />
                <span className="min-w-0 text-sm text-slate-100">Save outputs to AWS cloud</span>
              </label>
            </div>
          </section>
        </SectionCard>
      </form>

      <SectionCard title="Optimizer Status" subtitle="" eyebrow="Status" bodyClassName="space-y-4">
        <div className="rounded-[1.35rem] border border-white/10 bg-[linear-gradient(180deg,rgba(167,139,250,0.10),rgba(167,139,250,0.02))] p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-violet-200">Current Mode</p>
          <p className="mt-2 text-lg font-semibold text-text">{values.schedule_mode === "block" ? "Continuous block scheduling" : "Flexible interval scheduling"}</p>
          <p className="mt-1.5 text-sm text-muted">{isExtendedMode ? "Extended mode active" : "Short-term live mode"}</p>
        </div>

        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-1">
          <div className="rounded-[1.25rem] border border-white/10 bg-white/[0.04] p-4">
            <p className="text-xs uppercase tracking-[0.16em] text-muted">ZIP / Region</p>
            <p className="mt-2 text-sm text-text">{lastRun?.location.resolved_region ? `${values.zip_code ?? ""} -> ${lastRun.location.resolved_region}` : values.zip_code ?? "Awaiting input"}</p>
          </div>
          <div className="rounded-[1.25rem] border border-white/10 bg-white/[0.04] p-4">
            <p className="text-xs uppercase tracking-[0.16em] text-muted">Pricing / Carbon</p>
            <p className="mt-2 text-sm text-text">{lastRun ? `${lastRun.pricing.pricing_status_label} / ${lastRun.forecast.carbon_signal_mix.join(", ")}` : "Will resolve after optimization"}</p>
          </div>
          <div className="rounded-[1.25rem] border border-white/10 bg-white/[0.04] p-4">
            <p className="text-xs uppercase tracking-[0.16em] text-muted">Runtime / Deadline</p>
            <p className="mt-2 text-sm text-text">{lastRun ? `${lastRun.input.compute_hours_required}h by ${new Date(lastRun.input.deadline).toLocaleString()}` : `${values.compute_hours_required ?? "-"}h configured`}</p>
          </div>
          <div className="rounded-[1.25rem] border border-white/10 bg-white/[0.04] p-4">
            <p className="text-xs uppercase tracking-[0.16em] text-muted">Cloud Export Preference</p>
            <p className="mt-2 text-sm text-text">{values.enable_cloud_upload ? "Enabled" : "Disabled"}</p>
          </div>
        </div>

        {lastRun?.summary.badges?.length ? (
          <div className="rounded-[1.25rem] border border-white/10 bg-white/[0.04] p-4">
            <p className="text-xs uppercase tracking-[0.16em] text-muted">Latest Run Badges</p>
            <div className="mt-3 flex flex-wrap gap-2.5">
              {lastRun.summary.badges.map((badge) => (
                <span key={badge.id} className={`rounded-full border px-3 py-1.5 text-xs ${badge.tone === "positive" ? "border-emerald-300/20 bg-emerald-300/10 text-emerald-100" : badge.tone === "warning" ? "border-amber-300/20 bg-amber-300/10 text-amber-100" : "border-white/10 bg-black/20 text-slate-100/90"}`}>
                  <span className="text-muted">{badge.label}</span>
                  <span className="ml-2 font-medium">{badge.value}</span>
                </span>
              ))}
            </div>
          </div>
        ) : null}

        {runWindow?.start && runWindow?.end ? (
          <div className="rounded-[1.25rem] border border-emerald-300/14 bg-[linear-gradient(180deg,rgba(74,222,128,0.10),rgba(74,222,128,0.02))] p-4 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
            <p className="text-xs uppercase tracking-[0.16em] text-emerald-200">Latest Recommended Window</p>
            <p className="mt-2 text-sm leading-6 text-slate-100/90">{new Date(runWindow.start).toLocaleString()} to {new Date(runWindow.end).toLocaleString()}</p>
          </div>
        ) : null}
      </SectionCard>
    </div>

    </div>
  );
}







