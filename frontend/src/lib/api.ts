import type { AiInterpretRequest, AiInterpretResponse, CoverageResponse, ExportRequest, ExportResponse, OptimizeRequest, OptimizeResponse } from "../types/api";

const rawApiBaseUrl = import.meta.env.VITE_API_BASE_URL as string | undefined;
const isAbsoluteApiBase = typeof rawApiBaseUrl === "string" && /^https?:\/\//i.test(rawApiBaseUrl);
const API_BASE_URL = (rawApiBaseUrl ?? "").replace(/\/+$/, "");
const API_PREFIX = import.meta.env.DEV ? "" : API_BASE_URL;
const IS_DEV = import.meta.env.DEV;

if (!IS_DEV && !API_BASE_URL) {
  console.warn("[util-api] No VITE_API_BASE_URL is configured for a non-dev build. Desktop packaging should set VITE_API_BASE_URL to the local backend address.");
}

type HealthResponse = {
  status: string;
  service: string;
};

function logDev(label: string, value: unknown): void {
  if (IS_DEV) {
    console.info(`[util-api] ${label}`, value);
  }
}

function buildApiUrl(path: string): string {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${API_PREFIX}${normalizedPath}`;
}

function classifyNetworkError(error: unknown): string {
  if (error instanceof DOMException && error.name === "AbortError") {
    return "abort";
  }
  if (error instanceof Error) {
    const lowered = error.message.toLowerCase();
    if (error.name === "AbortError" || lowered.includes("aborted")) {
      return "abort";
    }
    if (lowered.includes("timeout")) {
      return "timeout";
    }
    if (lowered.includes("failed to fetch") || lowered.includes("networkerror")) {
      return "network";
    }
  }
  return "unknown_network";
}

async function parseResponseBody(response: Response): Promise<unknown> {
  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) {
    return response.json().catch(() => null);
  }
  return response.text().catch(() => "");
}

async function buildResponseError(response: Response, finalUrl: string): Promise<Error> {
  const responseBody = await parseResponseBody(response);
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
  const kind = classifyNetworkError(error);
  console.error("[util-api] fetch/network error", {
    url: finalUrl,
    kind,
    error,
  });
  return new Error([
    `Optimize request failed (${kind}) at ${finalUrl}`,
    `Original error: ${message}`,
  ].join("\n"));
}

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const finalUrl = buildApiUrl(path);
  const startedAt = performance.now();
  logDev("resolved API base URL", API_BASE_URL || "(relative /api proxy)");
  logDev("request URL", finalUrl);
  if (init?.body) {
    logDev("request body", init.body);
  }
  console.info("[util-api] request start", {
    url: finalUrl,
    method: init?.method ?? "GET",
    startedAt: new Date().toISOString(),
  });

  let response: Response;
  try {
    response = await fetch(finalUrl, init);
  } catch (error) {
    console.error("[util-api] request failed before response", {
      url: finalUrl,
      durationMs: performance.now() - startedAt,
      classification: classifyNetworkError(error),
      error,
    });
    throw buildNetworkError(error, finalUrl);
  }

  console.info("[util-api] response received", {
    url: finalUrl,
    status: response.status,
    statusText: response.statusText,
    durationMs: performance.now() - startedAt,
  });

  if (!response.ok) {
    throw await buildResponseError(response, finalUrl);
  }

  try {
    return (await response.json()) as T;
  } catch (error) {
    console.error("[util-api] response parse error", {
      url: finalUrl,
      durationMs: performance.now() - startedAt,
      error,
    });
    throw new Error(
      `Optimize request failed (parse_error): Response from ${finalUrl} was not valid JSON.`
    );
  }
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
export const HEALTH_PATH = "/health";
export const HEALTH_URL = buildApiUrl(HEALTH_PATH);

export async function fetchHealth(): Promise<HealthResponse> {
  return requestJson<HealthResponse>(HEALTH_PATH);
}

export async function interpretOptimization(payload: AiInterpretRequest): Promise<AiInterpretResponse> {
  return requestJson<AiInterpretResponse>("/api/v1/ai/interpret", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}

export async function waitForBackendReady(retries = 20, delayMs = 500): Promise<HealthResponse> {
  let lastError: unknown;

  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      return await fetchHealth();
    } catch (error) {
      lastError = error;
      if (attempt < retries) {
        await new Promise((resolve) => window.setTimeout(resolve, delayMs));
      }
    }
  }

  throw lastError instanceof Error
    ? lastError
    : new Error("Backend health check failed before the app became ready.");
}
