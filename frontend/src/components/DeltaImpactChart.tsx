interface DeltaImpactChartProps {
  title: string;
  subtitle?: string;
  items: Array<{
    label: string;
    value: number;
    displayValue: string;
    positiveIsGood?: boolean;
    tone?: "cost" | "carbon" | "neutral";
  }>;
}

function resolveColor(value: number, positiveIsGood: boolean, tone: "cost" | "carbon" | "neutral") {
  const isPositive = value >= 0;
  const good = positiveIsGood ? isPositive : !isPositive;

  if (!good) {
    return "#fb7185";
  }

  if (tone === "carbon") {
    return "#2dd4bf";
  }

  if (tone === "cost") {
    return "#8b5cf6";
  }

  return "#4ade80";
}

export function DeltaImpactChart({ title, subtitle, items }: DeltaImpactChartProps) {
  const width = 420;
  const height = 280;
  const chartTop = 22;
  const chartBottom = 210;
  const maxValue = Math.max(...items.map((item) => Math.abs(item.value)), 0.0001);

  const bars = items.map((item, index) => {
    const x = 54 + index * 154;
    const barWidth = 92;
    const scaledHeight = (Math.abs(item.value) / maxValue) * (chartBottom - chartTop);
    const y = chartBottom - scaledHeight;
    return {
      ...item,
      x,
      barWidth,
      scaledHeight,
      y,
      color: resolveColor(item.value, item.positiveIsGood ?? true, item.tone ?? "neutral"),
    };
  });

  const yTicks = [0, maxValue / 2, maxValue];

  return (
    <div className="rounded-[1.5rem] border border-white/10 bg-white/[0.04] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.03)]">
      <div className="mb-5 space-y-3">
        <div>
          <h3 className="text-base font-semibold text-text">{title}</h3>
          {subtitle ? <p className="mt-1 text-sm leading-6 text-muted">{subtitle}</p> : null}
        </div>
        <div className="inline-flex rounded-full border border-white/10 bg-black/20 px-3 py-1.5 text-xs text-slate-100/90">
          Bar height shows the size of the change versus baseline
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
                  {tick.toFixed(1)}
                </text>
              </g>
            );
          })}

          <line x1="46" y1={chartBottom} x2="386" y2={chartBottom} stroke="rgba(157,167,190,0.22)" />
          <line x1="46" y1={chartTop} x2="46" y2={chartBottom} stroke="rgba(157,167,190,0.14)" />

          {bars.map((bar) => (
            <g key={bar.label}>
              <text x={bar.x + bar.barWidth / 2} y={bar.y - 10} textAnchor="middle" fill="rgba(248,250,252,0.92)" fontSize="12" fontWeight="600">
                {bar.displayValue}
              </text>
              <rect x={bar.x} y={bar.y} width={bar.barWidth} height={bar.scaledHeight} rx="16" fill={bar.color} opacity="0.95" />
              <text x={bar.x + bar.barWidth / 2} y="235" textAnchor="middle" fill="rgba(226,232,240,0.86)" fontSize="12">
                {bar.label}
              </text>
            </g>
          ))}
        </svg>
      </div>
    </div>
  );
}
