import { SectionCard } from "../../../components/SectionCard";
import { ScheduleChartCard } from "../../../components/ScheduleChartCard";
import { ResultsTable } from "../../../components/ResultsTable";
import type { OptimizeResponse } from "../../../types/api";
import type { ScheduleDisplayRow } from "../utils";
import { deriveScheduleRows, formatDateTime, formatNumber, formatPercent } from "../utils";

interface ScheduleTabProps {
  data?: OptimizeResponse;
}

export function ScheduleTab({ data }: ScheduleTabProps) {
  if (!data) {
    return <SectionCard title="Schedule" subtitle="Run an optimization to view the execution plan."><div className="rounded-[1.6rem] border border-dashed border-white/10 bg-black/20 p-10 text-center text-muted">No schedule loaded yet. Run the optimizer to populate the run plan.</div></SectionCard>;
  }

  const rows = deriveScheduleRows(data);
  const columns = [
    { key: "timestamp", header: "Time", render: (row: ScheduleDisplayRow) => formatDateTime(row.timestamp) },
    { key: "runPercent", header: "Run %", align: "right" as const, render: (row: ScheduleDisplayRow) => formatPercent(row.runPercent) },
    { key: "pricePerKwh", header: "Price", align: "right" as const, render: (row: ScheduleDisplayRow) => formatNumber(row.pricePerKwh) },
    { key: "carbonPerKwh", header: "Carbon", align: "right" as const, render: (row: ScheduleDisplayRow) => formatNumber(row.carbonPerKwh) },
    { key: "recommended", header: "Recommended", render: (row: ScheduleDisplayRow) => row.recommended ? "Yes" : "No" },
  ];

  return (
    <div className="space-y-6">
      <SectionCard title="Run Plan" subtitle="Concrete schedule view showing recommended intervals across the forecast horizon." eyebrow="Schedule">
        <ScheduleChartCard
          title={data.charts.run_schedule_timeseries.title}
          subtitle={data.charts.run_schedule_timeseries.subtitle}
          rows={data.charts.run_schedule_timeseries.rows}
        />
      </SectionCard>

      <SectionCard title="Execution Table" subtitle="Interval-level guidance for time, run percentage, price, carbon, and recommendation status." eyebrow="Table">
        <ResultsTable
          rows={rows}
          columns={columns}
          emptyText="No schedule intervals are available for this run."
          getRowClassName={(row) => row.recommended ? "bg-emerald-400/[0.06]" : row.eligible ? "bg-white/[0.01]" : ""}
        />
      </SectionCard>
    </div>
  );
}
