import type { CoverageResponse, ExportRequest, ExportResponse, OptimizeRequest, OptimizeResponse } from "../types/api";

const rawApiBaseUrl = import.meta.env.VITE_API_BASE_URL as string | undefined;
const isAbsoluteApiBase = typeof rawApiBaseUrl === "string" && /^https?:\/\//i.test(rawApiBaseUrl);
const API_BASE_URL = (rawApiBaseUrl ?? "").replace(/\/+$/, "");
const API_PREFIX = import.meta.env.DEV ? "" : API_BASE_URL;
const IS_DEV = import.meta.env.DEV;

function logDev(label: string, value: unknown): void {
  if (IS_DEV) {
    console.info(`[util-api] ${label}`, value);
  }
}

function buildApiUrl(path: string): string {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${API_PREFIX}${normalizedPath}`;
}

async function buildResponseError(response: Response, finalUrl: string): Promise<Error> {
  const contentType = response.headers.get("content-type") ?? "";
  const isJson = contentType.includes("application/json");
  const responseBody = isJson ? await response.json().catch(() => null) : await response.text().catch(() => "");
  const detail = typeof responseBody === "string" ? responseBody : JSON.stringify(responseBody);

  console.error("[util-api] response error", {
    url: finalUrl,
    status: response.status,
    statusText: response.statusText,
    body: detail,
  });

  if (response.status === 422) {
    return new Error(`API validation error (422): ${detail || "Validation failed."}`);
  }

  if (response.status === 404) {
    return new Error(`API endpoint not found (404): ${finalUrl}`);
  }

  return new Error(`API request failed (${response.status} ${response.statusText}): ${detail || "No response body."}`);
}

function buildNetworkError(error: unknown, finalUrl: string): Error {
  const message = error instanceof Error ? error.message : String(error);
  console.error("[util-api] fetch/network error", {
    url: finalUrl,
    error,
  });
  return new Error([
    `Unable to reach the Util API at ${finalUrl}`,
    `Original error: ${message}`,
  ].join("\n"));
}

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const finalUrl = buildApiUrl(path);
  logDev("resolved API base URL", API_BASE_URL || "(relative /api proxy)");
  logDev("request URL", finalUrl);
  if (init?.body) {
    logDev("request body", init.body);
  }

  let response: Response;
  try {
    response = await fetch(finalUrl, init);
  } catch (error) {
    throw buildNetworkError(error, finalUrl);
  }

  if (!response.ok) {
    throw await buildResponseError(response, finalUrl);
  }

  return response.json() as Promise<T>;
}

export async function optimizeScenario(payload: OptimizeRequest): Promise<OptimizeResponse> {
  return requestJson<OptimizeResponse>("/api/v1/optimize", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}

export async function fetchCoverage(): Promise<CoverageResponse> {
  return requestJson<CoverageResponse>("/api/v1/coverage");
}

export async function exportScenario(payload: ExportRequest): Promise<ExportResponse> {
  return requestJson<ExportResponse>("/api/v1/export", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}

export function buildExportDownloadUrl(path: string): string {
  return buildApiUrl(`/api/v1/export/download?path=${encodeURIComponent(path)}`);
}

export { API_BASE_URL };
export const RESOLVED_API_MODE = import.meta.env.DEV ? "vite-proxy" : isAbsoluteApiBase ? "absolute" : API_BASE_URL || "relative";
