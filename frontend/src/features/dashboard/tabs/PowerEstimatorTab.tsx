import { useMemo, useState } from "react";
import { SectionCard } from "../../../components/SectionCard";

interface PowerEstimatorTabProps {
  onApplyEstimator: (watts: number) => void;
}

const gpuModels: Record<string, number> = {
  "RTX 3060": 170,
  "RTX 3070": 220,
  "RTX 3080": 320,
  "RTX 3090": 350,
  "RTX 4070": 200,
  "RTX 4080": 320,
  "RTX 4090": 450,
  A100: 400,
  H100: 700,
  B200: 1000,
};

const intelCpuOptions: Record<string, number> = {
  "Intel i5 / equivalent": 95,
  "Intel i7 / equivalent": 125,
  "Intel i9 / equivalent": 180,
  "Intel Xeon (single socket)": 250,
  "Intel Xeon (dual socket)": 400,
};

const amdCpuOptions: Record<string, number> = {
  "AMD Ryzen 5 / equivalent": 90,
  "AMD Ryzen 7 / equivalent": 125,
  "AMD Ryzen 9 / equivalent": 170,
  "AMD Threadripper": 280,
  "AMD EPYC (single socket)": 280,
  "AMD EPYC (dual socket)": 450,
};

export function PowerEstimatorTab({ onApplyEstimator }: PowerEstimatorTabProps) {
  const [gpu, setGpu] = useState("RTX 4090");
  const [numGpus, setNumGpus] = useState(1);
  const [cpuBrand, setCpuBrand] = useState<"Intel" | "AMD">("Intel");
  const [cpuModel, setCpuModel] = useState("Intel i7 / equivalent");
  const [overhead, setOverhead] = useState(150);
  const [utilizationFactor, setUtilizationFactor] = useState(1);

  const cpuOptions = cpuBrand === "Intel" ? intelCpuOptions : amdCpuOptions;

  const { cpuWatts, estimatedPower, estimatedKwhPerHour, gpuTotalEstimated } = useMemo(() => {
    const selectedCpuWatts = cpuOptions[cpuModel] ?? Object.values(cpuOptions)[0];
    const gpuTotalNameplate = (gpuModels[gpu] ?? 0) * numGpus;
    const gpuTotal = gpuTotalNameplate * utilizationFactor;
    const total = Math.round(gpuTotal + selectedCpuWatts + overhead);
    return {
      cpuWatts: selectedCpuWatts,
      estimatedPower: total,
      estimatedKwhPerHour: total / 1000,
      gpuTotalEstimated: Math.round(gpuTotal),
    };
  }, [cpuModel, cpuOptions, gpu, numGpus, overhead, utilizationFactor]);

  return (
    <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_320px] xl:items-start">
      <div className="grid gap-6">
        <SectionCard title="Estimator Controls" subtitle="" eyebrow="Estimator">
          <div className="grid gap-5 md:grid-cols-2">
            <label className="space-y-2.5">
              <span className="text-sm font-medium text-slate-100">GPU Model</span>
              <select value={gpu} onChange={(event) => setGpu(event.target.value)} className="w-full rounded-[1.25rem] border border-white/10 bg-white/[0.05] px-4 py-3 text-text outline-none transition focus:border-violet-300/40">
                {Object.keys(gpuModels).map((option) => <option key={option} value={option}>{option}</option>)}
              </select>
            </label>
            <label className="space-y-2.5">
              <span className="text-sm font-medium text-slate-100">Number of GPUs</span>
              <input type="number" min={1} value={numGpus} onChange={(event) => setNumGpus(Number(event.target.value))} className="w-full rounded-[1.25rem] border border-white/10 bg-white/[0.05] px-4 py-3 text-text outline-none transition focus:border-violet-300/40" />
            </label>
            <label className="space-y-2.5">
              <span className="text-sm font-medium text-slate-100">CPU Brand</span>
              <select value={cpuBrand} onChange={(event) => {
                const nextBrand = event.target.value as "Intel" | "AMD";
                setCpuBrand(nextBrand);
                setCpuModel(Object.keys(nextBrand === "Intel" ? intelCpuOptions : amdCpuOptions)[0]);
              }} className="w-full rounded-[1.25rem] border border-white/10 bg-white/[0.05] px-4 py-3 text-text outline-none transition focus:border-violet-300/40">
                <option value="Intel">Intel</option>
                <option value="AMD">AMD</option>
              </select>
            </label>
            <label className="space-y-2.5">
              <span className="text-sm font-medium text-slate-100">CPU Type</span>
              <select value={cpuModel} onChange={(event) => setCpuModel(event.target.value)} className="w-full rounded-[1.25rem] border border-white/10 bg-white/[0.05] px-4 py-3 text-text outline-none transition focus:border-violet-300/40">
                {Object.keys(cpuOptions).map((option) => <option key={option} value={option}>{option}</option>)}
              </select>
            </label>
            <label className="space-y-2.5">
              <span className="text-sm font-medium text-slate-100">System Overhead (W)</span>
              <input type="range" min={50} max={5000} step={10} value={overhead} onChange={(event) => setOverhead(Number(event.target.value))} className="w-full" />
              <span className="text-xs text-muted">{overhead} W</span>
            </label>
            <label className="space-y-2.5">
              <span className="text-sm font-medium text-slate-100">Estimated Workload Intensity</span>
              <input type="range" min={0.1} max={1} step={0.05} value={utilizationFactor} onChange={(event) => setUtilizationFactor(Number(event.target.value))} className="w-full" />
              <span className="text-xs text-muted">{utilizationFactor.toFixed(2)}</span>
            </label>
          </div>
        </SectionCard>

        <SectionCard title="Estimator Breakdown" subtitle="" eyebrow="Breakdown">
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <div className="rounded-[1.5rem] border border-white/10 bg-white/[0.04] p-4"><p className="text-[11px] uppercase tracking-[0.16em] text-muted">Estimated Load</p><p className="mt-2 text-lg font-semibold text-text">{estimatedPower.toLocaleString()} W</p></div>
            <div className="rounded-[1.5rem] border border-white/10 bg-white/[0.04] p-4"><p className="text-[11px] uppercase tracking-[0.16em] text-muted">Energy Per Hour</p><p className="mt-2 text-lg font-semibold text-text">{estimatedKwhPerHour.toFixed(2)} kWh</p></div>
            <div className="rounded-[1.5rem] border border-white/10 bg-white/[0.04] p-4"><p className="text-[11px] uppercase tracking-[0.16em] text-muted">GPU Power Component</p><p className="mt-2 text-lg font-semibold text-text">{gpuTotalEstimated.toLocaleString()} W</p></div>
            <div className="rounded-[1.5rem] border border-white/10 bg-white/[0.04] p-4"><p className="text-[11px] uppercase tracking-[0.16em] text-muted">CPU Power</p><p className="mt-2 text-lg font-semibold text-text">{cpuWatts.toLocaleString()} W</p></div>
          </div>
          <div className="mt-5 flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            
            <button type="button" onClick={() => onApplyEstimator(estimatedPower)} className="inline-flex items-center justify-center rounded-[1.25rem] bg-gradient-to-r from-violet-300 via-fuchsia-400 to-cyan-300 px-5 py-3 text-sm font-semibold text-slate-950 shadow-[0_16px_36px_rgba(139,92,246,0.32)] transition hover:brightness-110">
              Use Estimator Value in Optimizer
            </button>
          </div>
        </SectionCard>
      </div>

      <SectionCard title="Estimator Recommendation" subtitle="" eyebrow="Result">
        <div className="rounded-[1.6rem] border border-emerald-300/14 bg-[linear-gradient(180deg,rgba(74,222,128,0.10),rgba(74,222,128,0.02))] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
          <p className="text-sm leading-7 text-slate-100/90"><strong>{estimatedPower.toLocaleString()} W</strong></p>
        </div>
      </SectionCard>
    </div>
  );
}





