from __future__ import annotations

import logging
from functools import lru_cache
from http import HTTPStatus
from pathlib import Path
import traceback
from typing import Any

from src.runtime_config import (
    get_project_env_diagnostics,
    load_project_env,
    resolve_cloud_config,
)

try:
    import boto3
    _BOTO3_IMPORT_ERROR = None
except ModuleNotFoundError as exc:
    boto3 = None
    _BOTO3_IMPORT_ERROR = exc

try:
    import botocore
    from botocore.exceptions import BotoCoreError, ClientError
    _BOTOCORE_IMPORT_ERROR = None
except ModuleNotFoundError as exc:
    botocore = None
    _BOTOCORE_IMPORT_ERROR = exc
    class BotoCoreError(Exception):
        pass

    class ClientError(Exception):
        pass


logger = logging.getLogger(__name__)


def _normalize_value(value: Any, *, strip_inline_comment: bool = False) -> str:
    text = str(value or "").strip()
    if strip_inline_comment and " #" in text:
        text = text.split(" #", 1)[0].rstrip()
    return text.strip().strip('"').strip("'")


def _build_import_diagnostics() -> dict[str, str]:
    boto3_version = getattr(boto3, "__version__", "<missing>") if boto3 is not None else "<missing>"
    return {
        "boto3_import_success": "yes" if boto3 is not None else "no",
        "botocore_import_success": "yes" if "botocore" in globals() and botocore is not None else "no",
        "boto3_version": boto3_version,
        "boto3_import_error": str(_BOTO3_IMPORT_ERROR) if _BOTO3_IMPORT_ERROR is not None else "",
        "botocore_import_error": str(_BOTOCORE_IMPORT_ERROR) if _BOTOCORE_IMPORT_ERROR is not None else "",
    }


@lru_cache(maxsize=1)
def _build_cloud_status_detail() -> tuple[str, str]:
    env_path = load_project_env().resolve()
    cloud_config = resolve_cloud_config()
    if cloud_config["source"] == "streamlit secrets":
        detail = "Cloud config source: streamlit secrets"
    elif cloud_config["source"] == "environment/.env":
        detail = "Cloud config source: environment/.env"
    else:
        detail = "Cloud config source: missing"

    logger.info(detail)
    return str(env_path), detail


def _classify_s3_exception(exc: Exception) -> tuple[str, str]:
    if isinstance(exc, ClientError):
        error = exc.response.get("Error", {})
        code = str(error.get("Code", "")).strip()
        message = str(error.get("Message", "")).strip() or str(exc)
        http_status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if code in {"InvalidAccessKeyId", "SignatureDoesNotMatch", "AuthFailure"}:
            return "invalid credentials", message
        if code in {"AccessDenied", "AllAccessDisabled"}:
            return "access denied", message
        if code in {"NoSuchBucket", "404"} or http_status == HTTPStatus.NOT_FOUND:
            return "bucket not found", message
        if code in {"PermanentRedirect", "AuthorizationHeaderMalformed"}:
            return "wrong region", message
        if http_status == HTTPStatus.FORBIDDEN:
            return "access denied", message
        return "aws client error", message

    if isinstance(exc, BotoCoreError):
        return "client initialization failure", str(exc)

    return "unexpected error", str(exc)


def _validate_s3_bucket_access(client, bucket_name: str) -> tuple[bool, str | None, str | None]:
    try:
        client.head_bucket(Bucket=bucket_name)
        logger.info("S3 bucket access verified for %s", bucket_name)
        return True, None, None
    except (BotoCoreError, ClientError) as exc:
        failure_reason, message = _classify_s3_exception(exc)
        logger.exception("S3 bucket validation failed for %s: %s", bucket_name, message)
        return False, failure_reason, message


def _build_s3_settings_result() -> dict[str, Any]:
    env_path, status_detail = _build_cloud_status_detail()
    cloud_config = resolve_cloud_config()
    values = cloud_config["values"]
    access_key = _normalize_value(values.get("AWS_ACCESS_KEY_ID", ""))
    secret_key = _normalize_value(values.get("AWS_SECRET_ACCESS_KEY", ""))
    region = _normalize_value(values.get("AWS_REGION", ""), strip_inline_comment=True)
    bucket_name = _normalize_value(values.get("S3_BUCKET_NAME", ""), strip_inline_comment=True)
    diagnostics = _build_import_diagnostics()
    debug_detail = (
        "Cloud diagnostics: "
        f"config source={cloud_config['source']}, "
        f"boto3 import success={diagnostics['boto3_import_success']}, "
        f"botocore import success={diagnostics['botocore_import_success']}, "
        f"boto3 version={diagnostics['boto3_version']}, "
        f"AWS key present after strip={'yes' if bool(access_key) else 'no'}, "
        f"AWS secret present after strip={'yes' if bool(secret_key) else 'no'}, "
        f"AWS region value={region or '<missing>'}, "
        f"bucket name value={bucket_name or '<missing>'}"
    )
    detail = status_detail

    if not all([access_key, secret_key, region, bucket_name]):
        return {
            "configured": False,
            "settings": None,
            "message": "Cloud storage not configured, using local only",
            "status_detail": detail,
            "env_path": env_path,
            "debug_detail": debug_detail,
        }

    logger.info("S3 configured for bucket=%s region=%s", bucket_name, region)
    return {
        "configured": True,
        "settings": {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "region_name": region,
            "bucket_name": bucket_name,
        },
        "message": "",
        "status_detail": detail,
        "env_path": env_path,
        "debug_detail": debug_detail,
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
        logger.info("S3 client initialized for bucket=%s region=%s", settings["bucket_name"], settings["region_name"])
        return client
    except Exception as exc:
        failure_reason, message = _classify_s3_exception(exc)
        logger.exception(
            "S3 client initialization failed (%s): type=%s message=%s traceback=%s",
            failure_reason,
            type(exc).__name__,
            message,
            traceback.format_exc(),
        )
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
        failure_reason, message = _classify_s3_exception(exc)
        logger.exception("S3 upload failed for %s -> %s (%s): %s", local_file, s3_key, failure_reason, message)
        return {
            "file_name": local_file.name,
            "local_path": str(local_file),
            "s3_key": s3_key,
            "bucket_name": settings["bucket_name"],
            "error": message,
            "failure_reason": failure_reason,
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
        failure_reason, message = _classify_s3_exception(exc)
        logger.exception("Failed to generate S3 presigned URL for %s (%s): %s", s3_key, failure_reason, message)
        return None


def upload_run_outputs(run_id, file_paths):
    settings_result = _build_s3_settings_result()
    settings = settings_result["settings"]
    client = create_s3_client()
    if settings is None:
        env_diagnostics = get_project_env_diagnostics()
        cloud_config = resolve_cloud_config()
        if cloud_config["source"] == "missing" and not env_diagnostics["exists"]:
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
            "status_detail": settings_result["status_detail"],
            "region_name": None,
            "env_path": settings_result["env_path"],
            "failure_reason": "missing configuration",
            "error_detail": None,
            "debug_detail": settings_result["debug_detail"],
        }
    if client is None:
        failure_reason = "boto3 not installed" if boto3 is None else "s3 client initialization failure"
        error_detail = (
            _build_import_diagnostics()["boto3_import_error"] or "No module named 'boto3'"
            if boto3 is None
            else "Failed to initialize the S3 client. Check logs for the exact exception message."
        )
        return {
            "configured": False,
            "message": "Cloud storage unavailable, using local only",
            "bucket_name": settings["bucket_name"],
            "files": [],
            "status_detail": settings_result["status_detail"],
            "region_name": settings["region_name"],
            "env_path": settings_result["env_path"],
            "failure_reason": failure_reason,
            "error_detail": error_detail,
            "debug_detail": settings_result["debug_detail"],
        }

    bucket_ok, bucket_failure_reason, bucket_error_detail = _validate_s3_bucket_access(client, settings["bucket_name"])
    if not bucket_ok:
        return {
            "configured": False,
            "message": "Cloud storage unavailable, using local only",
            "bucket_name": settings["bucket_name"],
            "files": [],
            "status_detail": settings_result["status_detail"],
            "region_name": settings["region_name"],
            "env_path": settings_result["env_path"],
            "failure_reason": bucket_failure_reason or "bucket access failure",
            "error_detail": bucket_error_detail,
            "debug_detail": settings_result["debug_detail"],
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
        if upload_result.get("download_url") is None and not upload_result.get("error"):
            upload_result["error"] = "Failed to generate presigned URL."
            upload_result["failure_reason"] = "presigned URL generation failure"
        uploaded_files.append(upload_result)

    if uploaded_files and all(not item.get("error") for item in uploaded_files):
        message = f"Uploaded {len(uploaded_files)} files to cloud storage."
        failure_reason = None
        error_detail = None
    elif uploaded_files:
        first_error = next((item for item in uploaded_files if item.get("error")), None)
        failure_reason = first_error.get("failure_reason") if first_error else "upload failure"
        error_detail = first_error.get("error") if first_error else "One or more cloud uploads failed."
        message = "Cloud storage unavailable, using local only"
    else:
        failure_reason = "upload failure"
        error_detail = "No cloud outputs were uploaded."
        message = "Cloud storage unavailable, using local only"

    return {
        "configured": not bool(failure_reason),
        "message": message,
        "bucket_name": settings["bucket_name"],
        "files": uploaded_files,
        "status_detail": settings_result["status_detail"],
        "region_name": settings["region_name"],
        "env_path": settings_result["env_path"],
        "failure_reason": failure_reason,
        "error_detail": error_detail,
        "debug_detail": settings_result["debug_detail"],
    }
