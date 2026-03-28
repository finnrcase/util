import { useEffect, useMemo, useRef, useState } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Bolt, ChartColumn, Cpu, Download, Globe, Info, LineChart } from "lucide-react";
import { AppShell } from "./components/AppShell";
import { SidebarNav, type SidebarItem } from "./components/SidebarNav";
import { TopBar } from "./components/TopBar";
import { API_BASE_URL, RESOLVED_API_MODE, exportScenario, fetchCoverage, optimizeScenario } from "./lib/api";
import type { ExportRequest, OptimizeRequest } from "./types/api";
import { formSchema, getDefaultDeadline, toExportPayload, toOptimizePayload, type FormInputValues, type FormValues } from "./features/dashboard/form";
import { compactRuntimeLabel, completionStatus } from "./features/dashboard/utils";
import { OptimizerTab } from "./features/dashboard/tabs/OptimizerTab";
import { ForecastTab } from "./features/dashboard/tabs/ForecastTab";
import { ExportsTab } from "./features/dashboard/tabs/ExportsTab";
import { SavingsAnalysisTab } from "./features/dashboard/tabs/SavingsAnalysisTab";
import { PowerEstimatorTab } from "./features/dashboard/tabs/PowerEstimatorTab";
import { MultiLocationTab } from "./features/dashboard/tabs/MultiLocationTab";
import { AboutUtilTab } from "./features/dashboard/tabs/AboutUtilTab";

type DashboardTabId = "optimizer" | "forecast_visuals" | "savings_analysis" | "power_estimator" | "multi_location" | "exports" | "about_util";

const sidebarItems: SidebarItem[] = [
  { id: "optimizer", label: "Optimizer", description: "Scenario inputs and optimization controls.", icon: Bolt },
  { id: "forecast_visuals", label: "Forecast Visuals", description: "Price and carbon forecast charts.", icon: LineChart },
  { id: "savings_analysis", label: "Savings Analysis", description: "KPI outcomes, savings, and impact summary.", icon: ChartColumn },
  { id: "power_estimator", label: "Power Estimator", description: "Estimate machine wattage for optimizer input.", icon: Cpu },
  { id: "multi_location", label: "Multi Location", description: "Location comparison across multiple ZIP codes.", icon: Globe },
  { id: "exports", label: "Exports", description: "Artifact generation and export package review.", icon: Download },
  { id: "about_util", label: "About Util", description: "Product context, methodology, and coverage.", icon: Info },
];

const tabConfig: Record<DashboardTabId, { title: string; subtitle: string }> = {
  optimizer: {
    title: "Optimizer",
    subtitle: "Configure workload, timing, location, and forecast settings, then run the existing optimization engine.",
  },
  forecast_visuals: {
    title: "Forecast Visuals",
    subtitle: "Inspect price and carbon forecast signals with clear recommendation markers on the lines.",
  },
  savings_analysis: {
    title: "Savings Analysis",
    subtitle: "Review cost, carbon, and recommendation impact in a compact operator-facing summary.",
  },
  power_estimator: {
    title: "Power Estimator",
    subtitle: "Estimate approximate system wattage when a machine power value is not already known.",
  },
  multi_location: {
    title: "Multi Location",
    subtitle: "Compare three candidate ZIP codes under shared scenario assumptions and pick the strongest location.",
  },
  exports: {
    title: "Exports",
    subtitle: "Generate and review the artifact package for the current run without leaving the dashboard shell.",
  },
  about_util: {
    title: "About Util",
    subtitle: "Product context, methodology, support boundaries, and roadmap summary.",
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
  const mainContentRef = useRef<HTMLElement | null>(null);

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
        return <OptimizerTab register={register} errors={errors} onSubmit={handleOptimizeSubmit} isSubmitting={optimizeMutation.isPending} errorMessage={optimizeError} lastRun={currentRun} values={formValues} />;
      case "forecast_visuals":
        return <ForecastTab data={currentRun} />;
      case "savings_analysis":
        return <SavingsAnalysisTab data={currentRun} isLoading={optimizeMutation.isPending} errorMessage={optimizeError} />;
      case "power_estimator":
        return <PowerEstimatorTab onApplyEstimator={handleApplyEstimator} />;
      case "multi_location":
        return <MultiLocationTab initialValues={formSchema.parse(getValues())} onUseBestLocation={handleUseBestLocation} />;
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
          <TopBar title={tabConfig[activeTab].title} subtitle={tabConfig[activeTab].subtitle} statusItems={statusItems} />
          {renderTab()}
        </div>
      </main>
    </AppShell>
  );
}



