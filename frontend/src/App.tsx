import { useEffect, useMemo, useRef, useState } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Bolt, ChartColumn, Cpu, Download, Globe, Info, LineChart, ScanSearch } from "lucide-react";
import { AppShell } from "./components/AppShell";
import { SidebarNav, type SidebarItem } from "./components/SidebarNav";
import { TopBar } from "./components/TopBar";
import { API_BASE_URL, HEALTH_URL, RESOLVED_API_MODE, exportScenario, fetchCoverage, optimizeScenario, waitForBackendReady } from "./lib/api";
import type { ExportRequest, OptimizeRequest } from "./types/api";
import { formSchema, getDefaultDeadline, toExportPayload, toOptimizePayload, type FormInputValues, type FormValues } from "./features/dashboard/form";
import { compactRuntimeLabel, completionStatus } from "./features/dashboard/utils";
import { OptimizerTab } from "./features/dashboard/tabs/OptimizerTab";
import { ForecastTab } from "./features/dashboard/tabs/ForecastTab";
import { ExportsTab } from "./features/dashboard/tabs/ExportsTab";
import { SavingsAnalysisTab } from "./features/dashboard/tabs/SavingsAnalysisTab";
import { PowerEstimatorTab } from "./features/dashboard/tabs/PowerEstimatorTab";
import { MultiLocationTab } from "./features/dashboard/tabs/MultiLocationTab";
import { OpportunityScreeningTab } from "./features/dashboard/tabs/OpportunityScreeningTab";
import { AboutUtilTab } from "./features/dashboard/tabs/AboutUtilTab";

type DashboardTabId = "optimizer" | "forecast_visuals" | "savings_analysis" | "power_estimator" | "multi_location" | "opportunity_screening" | "exports" | "about_util";

const sidebarItems: SidebarItem[] = [
  { id: "optimizer", label: "Optimizer", icon: Bolt },
  { id: "forecast_visuals", label: "Forecast Visuals", icon: LineChart },
  { id: "savings_analysis", label: "Savings Analysis", icon: ChartColumn },
  { id: "power_estimator", label: "Power Estimator", icon: Cpu },
  { id: "multi_location", label: "Multi Location", icon: Globe },
  { id: "opportunity_screening", label: "Opportunity Screening", icon: ScanSearch },
  { id: "exports", label: "Exports", icon: Download },
  { id: "about_util", label: "About Util", icon: Info },
];

const tabConfig: Record<DashboardTabId, { title: string; subtitle: string }> = {
  optimizer: {
    title: "Optimizer",
    subtitle: "",
  },
  forecast_visuals: {
    title: "Forecast Visuals",
    subtitle: "",
  },
  savings_analysis: {
    title: "Savings Analysis",
    subtitle: "",
  },
  power_estimator: {
    title: "Power Estimator",
    subtitle: "",
  },
  multi_location: {
    title: "Multi Location",
    subtitle: "",
  },
  opportunity_screening: {
    title: "Opportunity Screening",
    subtitle: "",
  },
  exports: {
    title: "Exports",
    subtitle: "",
  },
  about_util: {
    title: "About Util",
    subtitle: "",
  },
};

const defaultFormValues: FormInputValues = {
  zip_code: "90012",
  compute_hours_required: 4,
  deadline: getDefaultDeadline(),
  objective: "cost",
  machine_watts: 1000,
  carbon_estimation_mode: "forecast_plus_historical_expectation",
  historical_days: 7,
  schedule_mode: "flexible",
  enable_cloud_upload: false,
};

export default function App() {
  const [activeTab, setActiveTab] = useState<DashboardTabId>("optimizer");
  const [lastSubmittedPayload, setLastSubmittedPayload] = useState<OptimizeRequest | null>(null);
  const [lastSubmittedExportPayload, setLastSubmittedExportPayload] = useState<ExportRequest | null>(null);
  const [isBackendReady, setIsBackendReady] = useState(false);
  const [backendError, setBackendError] = useState<string>();
  const [backendRetryKey, setBackendRetryKey] = useState(0);
  const mainContentRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    const content = mainContentRef.current;
    if (!content) {
      return;
    }

    content.scrollTop = 0;
  }, [activeTab]);

  useEffect(() => {
    let cancelled = false;

    setIsBackendReady(false);
    setBackendError(undefined);

    void waitForBackendReady()
      .then(() => {
        if (!cancelled) {
          setIsBackendReady(true);
        }
      })
      .catch((error: unknown) => {
        if (!cancelled) {
          setBackendError(error instanceof Error ? error.message : String(error));
        }
      });

    return () => {
      cancelled = true;
    };
  }, [backendRetryKey]);


  const {
    register,
    handleSubmit,
    setValue,
    getValues,
    watch,
    formState: { errors },
  } = useForm<FormInputValues, undefined, FormValues>({
    resolver: zodResolver(formSchema),
    defaultValues: defaultFormValues,
  });

  const formValues = watch();

  const optimizeMutation = useMutation({
    mutationFn: optimizeScenario,
  });

  const exportMutation = useMutation({
    mutationFn: exportScenario,
    onSuccess: () => setActiveTab("exports"),
  });

  const coverageQuery = useQuery({
    queryKey: ["coverage"],
    queryFn: fetchCoverage,
    staleTime: 5 * 60 * 1000,
    enabled: isBackendReady,
  });

  const currentRun = optimizeMutation.data;
  const optimizeError = optimizeMutation.error instanceof Error ? optimizeMutation.error.message : undefined;
  const exportError = exportMutation.error instanceof Error ? exportMutation.error.message : undefined;

  const statusItems = useMemo(() => {
    if (!currentRun) {
      return [
        { label: "API", value: RESOLVED_API_MODE },
        { label: "Backend", value: API_BASE_URL || "proxied /api" },
        { label: "Carbon Mode", value: formValues.carbon_estimation_mode === "forecast_plus_historical_expectation" ? "Extended" : "Short-Term" },
        { label: "Schedule", value: formValues.schedule_mode === "block" ? "Continuous Block" : "Flexible" },
      ];
    }

    return [
      { label: "Region", value: currentRun.location.resolved_region || "Pending" },
      { label: "Pricing", value: currentRun.pricing.pricing_status_label },
      { label: "Carbon", value: currentRun.forecast.carbon_signal_mix.join(", ") || "Forecast" },
      { label: "Runtime", value: compactRuntimeLabel(currentRun) },
      { label: "Deadline", value: completionStatus(currentRun) },
    ];
  }, [currentRun, formValues.carbon_estimation_mode, formValues.schedule_mode]);

  const handleOptimizeSubmit = handleSubmit((values) => {
    const payload = toOptimizePayload(values);
    const exportPayload = toExportPayload(values);
    setLastSubmittedPayload(payload);
    setLastSubmittedExportPayload(exportPayload);
    optimizeMutation.mutate(payload);
  });

  const handleExport = () => {
    if (!lastSubmittedExportPayload) {
      return;
    }
    exportMutation.mutate(lastSubmittedExportPayload);
  };

  const handleApplyEstimator = (watts: number) => {
    setValue("machine_watts", watts, { shouldDirty: true, shouldTouch: true, shouldValidate: true });
    setActiveTab("optimizer");
  };

  const handleUseBestLocation = (zip: string) => {
    setValue("zip_code", zip, { shouldDirty: true, shouldTouch: true, shouldValidate: true });
    setActiveTab("optimizer");
  };

  const currentExportPayload = useMemo(() => {
    const parsed = formSchema.safeParse(formValues);
    if (parsed.success) {
      return toExportPayload(parsed.data);
    }
    return lastSubmittedExportPayload ?? toExportPayload(formSchema.parse(defaultFormValues));
  }, [formValues, lastSubmittedExportPayload]);

  const renderTab = () => {
    switch (activeTab) {
      case "optimizer":
        return <OptimizerTab register={register} errors={errors} onSubmit={handleOptimizeSubmit} isSubmitting={optimizeMutation.isPending} isBackendReady={isBackendReady} errorMessage={optimizeError} lastRun={currentRun} values={formValues} />;
      case "forecast_visuals":
        return <ForecastTab data={currentRun} />;
      case "savings_analysis":
        return <SavingsAnalysisTab data={currentRun} isLoading={optimizeMutation.isPending} errorMessage={optimizeError} />;
      case "power_estimator":
        return <PowerEstimatorTab onApplyEstimator={handleApplyEstimator} />;
      case "multi_location":
        return <MultiLocationTab initialValues={formSchema.parse(getValues())} onUseBestLocation={handleUseBestLocation} isBackendReady={isBackendReady} />;
      case "opportunity_screening":
        return <OpportunityScreeningTab data={currentRun ?? null} />;
      case "exports":
        return (
          <ExportsTab
            canExport={Boolean(lastSubmittedExportPayload && currentRun)}
            currentPayload={currentExportPayload}
            latestRun={currentRun}
            exportResult={exportMutation.data}
            isExporting={exportMutation.isPending}
            exportError={exportError}
            onExport={handleExport}
          />
        );
      case "about_util":
        return <AboutUtilTab coverage={coverageQuery.data} isCoverageLoading={coverageQuery.isLoading} coverageError={coverageQuery.error instanceof Error ? coverageQuery.error.message : undefined} />;
      default:
        return null;
    }
  };

  return (
    <AppShell sidebar={<SidebarNav items={sidebarItems} activeItemId={activeTab} onNavigate={(id) => setActiveTab(id as DashboardTabId)} />}>
      <div className="border-b border-white/10 px-5 py-4 lg:hidden">
        <div className="flex items-center justify-between gap-4">
          <div>
            <p className="text-xs uppercase tracking-[0.28em] text-violet-200">Util</p>
            <p className="mt-2 text-lg font-semibold text-text">Optimization Dashboard</p>
          </div>
          <div className="rounded-full border border-violet-300/20 bg-violet-300/10 px-3 py-1.5 text-xs uppercase tracking-[0.18em] text-violet-100">
            {tabConfig[activeTab].title}
          </div>
        </div>
        <div className="mt-4 flex gap-2 overflow-x-auto pb-1">
          {sidebarItems.map((item) => (
            <button key={item.id} type="button" onClick={() => setActiveTab(item.id as DashboardTabId)} className={`shrink-0 rounded-full border px-3 py-1.5 text-xs ${item.id === activeTab ? "border-violet-300/20 bg-violet-300/10 text-violet-100" : "border-white/10 bg-white/[0.04] text-muted"}`}>
              {item.label}
            </button>
          ))}
        </div>
      </div>

      <main ref={mainContentRef} className="flex-1 overflow-y-auto px-4 py-4 sm:px-5 lg:px-7 lg:py-6">
        <div className="mx-auto flex w-full max-w-[1360px] flex-col gap-6 xl:gap-7">
          {!isBackendReady ? (
            <div className={`flex items-center justify-between gap-4 rounded-[1.25rem] border px-4 py-3 text-sm ${backendError ? "border-danger/25 bg-danger/10 text-red-100" : "border-violet-300/14 bg-[linear-gradient(180deg,rgba(167,139,250,0.08),rgba(167,139,250,0.02))] text-slate-100/90"}`}>
              <span>{backendError ? backendError : `Connecting to backend\u2026`}</span>
              {backendError ? (
                <button type="button" onClick={() => setBackendRetryKey((k) => k + 1)} className="shrink-0 rounded-full border border-white/10 bg-white/[0.06] px-3 py-1.5 text-xs text-slate-100 hover:bg-white/10">
                  Retry connection
                </button>
              ) : null}
            </div>
          ) : null}
          <TopBar title={tabConfig[activeTab].title} subtitle={tabConfig[activeTab].subtitle} statusItems={statusItems} />
          {renderTab()}
        </div>
      </main>
    </AppShell>
  );
}
