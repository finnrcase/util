from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import Any

from src.runtime_config import (
    get_project_env_diagnostics,
    get_project_root,
    get_setting,
    load_project_env,
)

try:
    import boto3
except ModuleNotFoundError:
    boto3 = None

try:
    from botocore.exceptions import BotoCoreError, ClientError
except ModuleNotFoundError:
    BotoCoreError = Exception
    ClientError = Exception


logger = logging.getLogger(__name__)
AWS_ENV_VARS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION",
    "S3_BUCKET_NAME",
)


def _normalize_value(value: Any, *, strip_inline_comment: bool = False) -> str:
    text = str(value or "").strip()
    if strip_inline_comment and " #" in text:
        text = text.split(" #", 1)[0].rstrip()
    return text.strip().strip('"').strip("'")


@lru_cache(maxsize=1)
def _log_s3_env_detection() -> tuple[str, list[str]]:
    env_path = load_project_env()
    env_diagnostics = get_project_env_diagnostics()
    diagnostics: list[str] = [
        f"Project root detected: {get_project_root()}",
        f"Env file path: {env_path}",
        f"Env file exists: {'yes' if env_diagnostics['exists'] else 'no'}",
        f"Env file empty: {'yes' if env_diagnostics['is_empty'] else 'no'}",
        f"Parsed env keys: {', '.join(env_diagnostics['parsed_keys']) if env_diagnostics['parsed_keys'] else '<none>'}",
        f"AWS_ACCESS_KEY_ID key present in file: {'yes' if env_diagnostics['has_aws_access_key_id'] else 'no'}",
        f"AWS_SECRET_ACCESS_KEY key present in file: {'yes' if env_diagnostics['has_aws_secret_access_key'] else 'no'}",
        f"AWS_REGION key present in file: {'yes' if env_diagnostics['has_aws_region'] else 'no'}",
        f"S3_BUCKET_NAME key present in file: {'yes' if env_diagnostics['has_s3_bucket_name'] else 'no'}",
    ]

    for message in diagnostics[2:]:
        logger.warning(message)

    for env_name in AWS_ENV_VARS:
        raw_value = get_setting(env_name, "")
        strip_inline_comment = env_name in {"AWS_REGION", "S3_BUCKET_NAME"}
        normalized = _normalize_value(raw_value, strip_inline_comment=strip_inline_comment)
        detected = bool(normalized)
        message = f"{env_name} detected: {'yes' if detected else 'no'}"
        diagnostics.append(message)
        logger.warning(message)

    return str(env_path), diagnostics


def _build_s3_settings_result() -> dict[str, Any]:
    env_path, diagnostics = _log_s3_env_detection()
    access_key = _normalize_value(get_setting("AWS_ACCESS_KEY_ID", ""))
    secret_key = _normalize_value(get_setting("AWS_SECRET_ACCESS_KEY", ""))
    region = _normalize_value(get_setting("AWS_REGION", ""), strip_inline_comment=True)
    bucket_name = _normalize_value(get_setting("S3_BUCKET_NAME", ""), strip_inline_comment=True)

    logger.warning("AWS region in use: %s", region or "<missing>")
    logger.warning("S3 bucket in use: %s", bucket_name or "<missing>")
    diagnostics.append(f"AWS region in use: {region or '<missing>'}")
    diagnostics.append(f"S3 bucket in use: {bucket_name or '<missing>'}")

    if not all([access_key, secret_key, region, bucket_name]):
        return {
            "configured": False,
            "settings": None,
            "message": "Cloud storage not configured, using local only",
            "diagnostics": diagnostics,
            "env_path": env_path,
        }

    return {
        "configured": True,
        "settings": {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "region_name": region,
            "bucket_name": bucket_name,
        },
        "message": "",
        "diagnostics": diagnostics,
        "env_path": env_path,
    }


def _get_s3_settings() -> dict[str, str] | None:
    result = _build_s3_settings_result()
    return result["settings"]


@lru_cache(maxsize=1)
def create_s3_client():
    settings_result = _build_s3_settings_result()
    settings = settings_result["settings"]
    if settings is None or boto3 is None:
        if boto3 is None:
            logger.warning("S3 client initialization skipped: boto3 is not installed.")
        return None

    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=settings["aws_access_key_id"],
            aws_secret_access_key=settings["aws_secret_access_key"],
            region_name=settings["region_name"],
        )
        logger.warning(
            "S3 client initialized for bucket=%s region=%s",
            settings["bucket_name"],
            settings["region_name"],
        )
        return client
    except Exception as exc:
        logger.exception("S3 client initialization failed: %s", exc)
        return None


def upload_file_to_s3(local_path, s3_key):
    settings = _get_s3_settings()
    client = create_s3_client()
    if settings is None or client is None:
        return None

    local_file = Path(local_path)
    try:
        client.upload_file(str(local_file), settings["bucket_name"], s3_key)
        logger.info("S3 upload succeeded for %s -> %s", local_file.name, s3_key)
        return {
            "file_name": local_file.name,
            "local_path": str(local_file),
            "s3_key": s3_key,
            "bucket_name": settings["bucket_name"],
        }
    except (BotoCoreError, ClientError, OSError) as exc:
        logger.exception("S3 upload failed for %s -> %s: %s", local_file, s3_key, exc)
        return {
            "file_name": local_file.name,
            "local_path": str(local_file),
            "s3_key": s3_key,
            "bucket_name": settings["bucket_name"],
            "error": str(exc),
        }


def create_presigned_download_url(s3_key, expires_in=3600):
    settings = _get_s3_settings()
    client = create_s3_client()
    if settings is None or client is None:
        return None

    try:
        return client.generate_presigned_url(
            "get_object",
            Params={"Bucket": settings["bucket_name"], "Key": s3_key},
            ExpiresIn=expires_in,
        )
    except (BotoCoreError, ClientError) as exc:
        logger.exception("Failed to generate S3 presigned URL for %s: %s", s3_key, exc)
        return None


def upload_run_outputs(run_id, file_paths):
    settings_result = _build_s3_settings_result()
    settings = settings_result["settings"]
    client = create_s3_client()
    if settings is None:
        env_diagnostics = get_project_env_diagnostics()
        if not env_diagnostics["exists"]:
            message = (
                "Cloud storage is disabled because no .env file is present in the runtime workspace "
                f"at {settings_result['env_path']}"
            )
        else:
            message = "Cloud storage not configured, using local only"
        return {
            "configured": False,
            "message": message,
            "bucket_name": None,
            "files": [],
            "diagnostics": settings_result["diagnostics"],
            "region_name": None,
            "env_path": settings_result["env_path"],
        }
    if client is None:
        return {
            "configured": False,
            "message": "Cloud storage unavailable, using local only",
            "bucket_name": settings["bucket_name"],
            "files": [],
            "diagnostics": settings_result["diagnostics"] + ["S3 client initialization failed. Check logs for the exception message."],
            "region_name": settings["region_name"],
            "env_path": settings_result["env_path"],
        }

    uploaded_files: list[dict[str, Any]] = []
    for file_path in file_paths:
        local_file = Path(file_path)
        s3_key = f"runs/{run_id}/{local_file.name}"
        upload_result = upload_file_to_s3(local_file, s3_key)
        if upload_result is None:
            continue

        upload_result["download_url"] = (
            create_presigned_download_url(s3_key) if not upload_result.get("error") else None
        )
        uploaded_files.append(upload_result)

    if uploaded_files and all(not item.get("error") for item in uploaded_files):
        message = f"Uploaded {len(uploaded_files)} files to cloud storage."
    elif uploaded_files:
        message = "Some cloud uploads failed."
    else:
        message = "No cloud outputs were uploaded."

    return {
        "configured": True,
        "message": message,
        "bucket_name": settings["bucket_name"],
        "files": uploaded_files,
        "diagnostics": settings_result["diagnostics"],
        "region_name": settings["region_name"],
        "env_path": settings_result["env_path"],
    }
