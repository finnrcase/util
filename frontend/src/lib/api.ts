import type { AiInterpretRequest, AiInterpretResponse, CoverageResponse, ExportRequest, ExportResponse, OptimizeRequest, OptimizeResponse } from "../types/api";

const DEFAULT_BACKEND_BASE_URL = "http://127.0.0.1:8000";
const rawApiBaseUrl = import.meta.env.VITE_API_BASE_URL as string | undefined;
const normalizedConfiguredApiBase = (rawApiBaseUrl ?? "").replace(/\/+$/, "");
const isAbsoluteApiBase = /^https?:\/\//i.test(normalizedConfiguredApiBase);
const IS_DEV = import.meta.env.DEV;
const BACKEND_BASE_URL = isAbsoluteApiBase ? normalizedConfiguredApiBase : DEFAULT_BACKEND_BASE_URL;
const API_BASE_URL = IS_DEV
  ? normalizedConfiguredApiBase
  : BACKEND_BASE_URL;
const API_PREFIX = IS_DEV ? "" : API_BASE_URL;

if (!IS_DEV && !normalizedConfiguredApiBase) {
  console.warn(
    `[util-api] No VITE_API_BASE_URL is configured for a non-dev build. Falling back to ${DEFAULT_BACKEND_BASE_URL}.`,
  );
}

type HealthResponse = {
  status: string;
  service: string;
};

type WarmupResponse = {
  status: string;
  service: string;
  steps?: string[];
};

function logDev(label: string, value: unknown): void {
  if (IS_DEV) {
    console.info(`[util-api] ${label}`, value);
  }
}

function buildApiUrl(path: string): string {
  if (/^https?:\/\//i.test(path)) {
    return path;
  }
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${API_PREFIX}${normalizedPath}`;
}

function buildBackendUrl(path: string): string {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${BACKEND_BASE_URL}${normalizedPath}`;
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

  const responseText = await response.text();
  const responsePreview = responseText.slice(0, 300);
  const contentType = response.headers.get("content-type") ?? "";

  console.info("[util-api] response body preview", {
    url: finalUrl,
    status: response.status,
    contentType,
    bodyPreview: responsePreview,
  });

  try {
    return JSON.parse(responseText) as T;
  } catch (error) {
    const parseMessage = error instanceof Error ? error.message : String(error);
    console.error("[util-api] response parse error", {
      url: finalUrl,
      status: response.status,
      contentType,
      bodyPreview: responsePreview,
      durationMs: performance.now() - startedAt,
      error,
    });
    throw new Error(
      `Optimize request failed (parse_error): Response from ${finalUrl} was not valid JSON. `
      + `content-type=${contentType || "<missing>"} preview=${JSON.stringify(responsePreview)} parse=${parseMessage}`
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

export { API_BASE_URL, BACKEND_BASE_URL };
export const RESOLVED_API_MODE = import.meta.env.DEV ? "vite-proxy" : isAbsoluteApiBase ? "configured-absolute" : "desktop-fallback";
export const HEALTH_PATH = "/health";
export const HEALTH_URL = buildBackendUrl(HEALTH_PATH);

export async function fetchHealth(): Promise<HealthResponse> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), 8_000);
  try {
    return await requestJson<HealthResponse>(HEALTH_URL, {
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });
  } finally {
    window.clearTimeout(timer);
  }
}

export async function triggerWarmup(): Promise<WarmupResponse> {
  return requestJson<WarmupResponse>("/api/v1/warmup", {
    method: "POST",
    headers: {
      Accept: "application/json",
    },
  });
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

export async function waitForBackendReady(retries = 30): Promise<HealthResponse> {
  let lastError: unknown;

  for (let attempt = 1; attempt <= retries; attempt += 1) {
    // First 3 attempts poll quickly in case the backend is already up.
    // After that, slow down to 3 s per attempt to survive a Render cold start
    // (wake-up typically takes 30–90 s) without hammering the server.
    const delayMs = attempt <= 3 ? 500 : 3_000;

    console.info("[util-api] backend readiness attempt", {
      attempt,
      retries,
      delayMs,
      healthUrl: HEALTH_URL,
      apiBaseUrl: API_BASE_URL || "<relative>",
      backendBaseUrl: BACKEND_BASE_URL,
      mode: RESOLVED_API_MODE,
    });
    try {
      const health = await fetchHealth();
      void triggerWarmup().catch((error: unknown) => {
        console.warn("[util-api] backend warmup request failed", { error });
      });
      return health;
    } catch (error) {
      lastError = error;
      console.error("[util-api] backend readiness attempt failed", {
        attempt,
        healthUrl: HEALTH_URL,
        backendBaseUrl: BACKEND_BASE_URL,
        error,
      });
      if (attempt < retries) {
        await new Promise((resolve) => window.setTimeout(resolve, delayMs));
      }
    }
  }

  throw lastError instanceof Error
    ? lastError
    : new Error("Backend health check failed before the app became ready.");
}
