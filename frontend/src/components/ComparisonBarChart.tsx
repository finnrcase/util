interface ComparisonBarChartProps {
  title: string;
  subtitle?: string;
  baselineLabel: string;
  optimizedLabel: string;
  baselineValue: number;
  optimizedValue: number;
  unitLabel: string;
  baselineColor?: string;
  optimizedColor?: string;
}

function formatChartValue(value: number, unitLabel: string): string {
  if (unitLabel === "USD") {
    return `$${value.toFixed(2)}`;
  }
  if (unitLabel === "kg CO2") {
    return `${value.toFixed(2)} kg CO2`;
  }
  if (unitLabel === "kWh") {
    return `${value.toFixed(2)} kWh`;
  }
  return `${value.toFixed(2)} ${unitLabel}`;
}

export function ComparisonBarChart({
  title,
  subtitle,
  baselineLabel,
  optimizedLabel,
  baselineValue,
  optimizedValue,
  unitLabel,
  baselineColor = "#7c8598",
  optimizedColor = "#8b5cf6",
}: ComparisonBarChartProps) {
  const width = 420;
  const height = 280;
  const chartTop = 22;
  const chartBottom = 210;
  const baselineBarHeight = 136;
  const optimizedBarHeight = 256;
  const maxValue = Math.max(baselineValue, optimizedValue, 0.0001);
  const scale = (heightValue: number) => ((heightValue / maxValue) * (chartBottom - chartTop));

  const bars = [
    { label: baselineLabel, value: baselineValue, color: baselineColor, x: 92, width: 82 },
    { label: optimizedLabel, value: optimizedValue, color: optimizedColor, x: 246, width: 82 },
  ].map((bar) => {
    const scaledHeight = scale(bar.value);
    const y = chartBottom - scaledHeight;
    return { ...bar, scaledHeight, y };
  });

  const yTicks = [0, maxValue / 2, maxValue];

  return (
    <div className="rounded-[1.5rem] border border-white/10 bg-white/[0.04] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.03)]">
      <div className="mb-5 space-y-3">
        <div>
          <h3 className="text-base font-semibold text-text">{title}</h3>
          {subtitle ? <p className="mt-1 text-sm leading-6 text-muted">{subtitle}</p> : null}
        </div>
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <span className="rounded-full border border-white/10 bg-black/20 px-3 py-1.5 text-slate-100/90">Unit: {unitLabel}</span>
          <span className="rounded-full border border-white/10 bg-black/20 px-3 py-1.5 text-slate-100/90">Grouped bar chart</span>
        </div>
      </div>

      <div className="overflow-hidden rounded-[1.35rem] border border-white/10 bg-[linear-gradient(180deg,rgba(10,14,25,0.84),rgba(7,10,18,0.92))] p-4">
        <svg viewBox={`0 0 ${width} ${height}`} className="w-full">
          {yTicks.map((tick, index) => {
            const y = chartBottom - ((tick / maxValue) * (chartBottom - chartTop));
            return (
              <g key={`${tick}-${index}`}>
                <line x1="46" y1={y} x2="386" y2={y} stroke="rgba(157,167,190,0.12)" strokeDasharray="5 7" />
                <text x="38" y={y + 4} textAnchor="end" fill="rgba(226,232,240,0.72)" fontSize="11">
                  {formatChartValue(tick, unitLabel).replace(" kg CO2", "").replace("$", "").replace(" kWh", "")}
                </text>
              </g>
            );
          })}

          <line x1="46" y1={chartBottom} x2="386" y2={chartBottom} stroke="rgba(157,167,190,0.22)" />
          <line x1="46" y1={chartTop} x2="46" y2={chartBottom} stroke="rgba(157,167,190,0.14)" />

          {bars.map((bar) => (
            <g key={bar.label}>
              <text x={bar.x + bar.width / 2} y={bar.y - 10} textAnchor="middle" fill="rgba(248,250,252,0.92)" fontSize="12" fontWeight="600">
                {formatChartValue(bar.value, unitLabel)}
              </text>
              <rect x={bar.x} y={bar.y} width={bar.width} height={bar.scaledHeight} rx="16" fill={bar.color} opacity="0.95" />
              <text x={bar.x + bar.width / 2} y="235" textAnchor="middle" fill="rgba(226,232,240,0.86)" fontSize="12">
                {bar.label}
              </text>
            </g>
          ))}
        </svg>
      </div>
    </div>
  );
}
