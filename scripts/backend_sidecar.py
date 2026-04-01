import os
import time

import uvicorn


def log_startup(message: str) -> None:
    print(f"[util-backend-startup] {message}", flush=True)


def main() -> None:
    os.environ.setdefault("UTIL_PROCESS_STARTED_AT", str(time.time()))
    process_started_at = float(os.environ["UTIL_PROCESS_STARTED_AT"])
    import_started_at = time.perf_counter()
    log_startup(f"python process entered sidecar at {process_started_at:.3f}")

    from src.api.main import app

    log_startup(
        f"fastapi app import completed in {(time.perf_counter() - import_started_at) * 1000.0:.1f} ms"
    )
    log_startup("starting uvicorn server on 127.0.0.1:8000")
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")


if __name__ == "__main__":
    main()
