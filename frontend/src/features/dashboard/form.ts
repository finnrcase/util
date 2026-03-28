import { z } from "zod";
import type { ExportRequest, OptimizeRequest } from "../../types/api";

export const formSchema = z.object({
  zip_code: z.string().length(5, "ZIP must be 5 digits"),
  compute_hours_required: z.coerce.number().int().positive("Compute hours must be positive"),
  deadline: z.string().min(1, "Deadline is required"),
  objective: z.enum(["carbon", "cost", "balanced"]),
  machine_watts: z.coerce.number().int().positive("Machine watts must be positive"),
  carbon_estimation_mode: z.enum(["forecast_only", "forecast_plus_historical_expectation"]),
  historical_days: z.coerce.number().int().min(1, "Historical lookback must be at least 1 day").max(14, "Historical lookback cannot exceed 14 days"),
  schedule_mode: z.enum(["flexible", "block"]),
  enable_cloud_upload: z.coerce.boolean().default(false),
});

export type FormValues = z.output<typeof formSchema>;
export type FormInputValues = z.input<typeof formSchema>;

export function getDefaultDeadline(): string {
  const now = new Date();
  now.setHours(now.getHours() + 8);
  const offset = now.getTimezoneOffset();
  const local = new Date(now.getTime() - offset * 60 * 1000);
  return local.toISOString().slice(0, 16);
}

export function toOptimizePayload(values: FormValues): OptimizeRequest {
  return {
    zip_code: values.zip_code,
    compute_hours_required: values.compute_hours_required,
    deadline: values.deadline,
    objective: values.objective,
    machine_watts: values.machine_watts,
    forecast_mode: "live_carbon",
    schedule_mode: values.schedule_mode,
    carbon_estimation_mode: values.carbon_estimation_mode,
    historical_days: values.historical_days,
  };
}

export function toExportPayload(values: FormValues): ExportRequest {
  return {
    ...toOptimizePayload(values),
    enable_cloud_upload: values.enable_cloud_upload,
  };
}
