from __future__ import annotations

import os

import uvicorn


def main() -> None:
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("src.api.main:app", host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
