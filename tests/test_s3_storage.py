from pathlib import Path
import sys
import shutil
import uuid

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.cloud import s3_storage
from src.exporter import generate_export_package
from src import runtime_config


def _make_workspace_temp_dir() -> Path:
    temp_dir = Path(__file__).resolve().parent / "_tmp" / uuid.uuid4().hex
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


def test_upload_run_outputs_returns_local_only_when_unconfigured(monkeypatch) -> None:
    s3_storage.create_s3_client.cache_clear()
    s3_storage._build_cloud_status_detail.cache_clear()
    monkeypatch.setattr(s3_storage, "load_project_env", lambda: Path("C:/dev/util/.env"))
    monkeypatch.setattr(
        s3_storage,
        "get_project_env_diagnostics",
        lambda: {
            "env_path": "C:/dev/util/.env",
            "exists": True,
            "is_empty": False,
            "parsed_keys": ["WATTTIME_USERNAME"],
            "has_aws_access_key_id": False,
            "has_aws_secret_access_key": False,
            "has_aws_region": False,
            "has_s3_bucket_name": False,
        },
    )
    monkeypatch.setattr(
        s3_storage,
        "resolve_cloud_config",
        lambda: {
            "source": "missing",
            "values": {
                "AWS_ACCESS_KEY_ID": "",
                "AWS_SECRET_ACCESS_KEY": "",
                "AWS_REGION": "",
                "S3_BUCKET_NAME": "",
            },
            "configured": False,
        },
    )

    result = s3_storage.upload_run_outputs("util-test-run", [])

    assert result["configured"] is False
    assert result["message"] == "Cloud storage not configured, using local only"
    assert result["files"] == []
    assert result["region_name"] is None
    assert result["failure_reason"] == "missing configuration"
    assert "Cloud diagnostics:" in result["debug_detail"]


def test_upload_run_outputs_builds_s3_keys_and_urls(monkeypatch) -> None:
    settings = {
        "AWS_ACCESS_KEY_ID": "key",
        "AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_REGION": "us-west-2",
        "S3_BUCKET_NAME": "util-private-bucket",
    }
    uploads: list[str] = []

    monkeypatch.setattr(s3_storage, "create_s3_client", lambda: object())
    monkeypatch.setattr(s3_storage, "_validate_s3_bucket_access", lambda client, bucket_name: (True, None, None))
    s3_storage._build_cloud_status_detail.cache_clear()
    monkeypatch.setattr(s3_storage, "load_project_env", lambda: Path("C:/dev/util/.env"))
    monkeypatch.setattr(
        s3_storage,
        "get_project_env_diagnostics",
        lambda: {
            "env_path": "C:/dev/util/.env",
            "exists": True,
            "is_empty": False,
            "parsed_keys": list(settings.keys()),
            "has_aws_access_key_id": True,
            "has_aws_secret_access_key": True,
            "has_aws_region": True,
            "has_s3_bucket_name": True,
        },
    )
    monkeypatch.setattr(
        s3_storage,
        "resolve_cloud_config",
        lambda: {
            "source": "environment/.env",
            "values": settings,
            "configured": True,
        },
    )

    def fake_upload_file_to_s3(local_path, s3_key):
        uploads.append(s3_key)
        return {
            "file_name": Path(local_path).name,
            "local_path": str(local_path),
            "s3_key": s3_key,
            "bucket_name": settings["S3_BUCKET_NAME"],
        }

    monkeypatch.setattr(s3_storage, "upload_file_to_s3", fake_upload_file_to_s3)
    monkeypatch.setattr(s3_storage, "create_presigned_download_url", lambda s3_key, expires_in=3600: f"https://signed/{s3_key}")

    temp_dir = _make_workspace_temp_dir()
    try:
        first = temp_dir / "util_run_summary.csv"
        second = temp_dir / "util_input_assumptions.csv"
        first.write_text("a,b\n1,2\n", encoding="utf-8")
        second.write_text("a,b\n3,4\n", encoding="utf-8")

        result = s3_storage.upload_run_outputs("util-test-run", [first, second])

        assert result["configured"] is True
        assert uploads == [
            "runs/util-test-run/util_run_summary.csv",
            "runs/util-test-run/util_input_assumptions.csv",
        ]
        assert result["files"][0]["download_url"] == "https://signed/runs/util-test-run/util_run_summary.csv"
        assert result["files"][1]["download_url"] == "https://signed/runs/util-test-run/util_input_assumptions.csv"
        assert result["bucket_name"] == "util-private-bucket"
        assert result["region_name"] == "us-west-2"
        assert result["failure_reason"] is None
        assert "boto3 import success=" in result["debug_detail"]
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_generate_export_package_includes_cloud_metadata(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.exporter.upload_run_outputs",
        lambda run_id, file_paths: {
            "configured": True,
            "message": "Uploaded 6 files to cloud storage.",
            "bucket_name": "util-private-bucket",
            "region_name": "us-west-2",
            "status_detail": "Cloud config source: environment/.env",
            "env_path": "C:/dev/util/.env",
            "failure_reason": None,
            "error_detail": None,
            "debug_detail": "Cloud diagnostics: boto3 import success=yes",
            "files": [
                {
                    "file_name": Path(file_paths[0]).name,
                    "local_path": str(file_paths[0]),
                    "s3_key": f"runs/{run_id}/{Path(file_paths[0]).name}",
                    "download_url": "https://signed/example",
                }
            ],
        },
    )

    class Workload:
        objective = "cost"
        machine_watts = 1000
        compute_hours_required = 1
        deadline = None
        price_weight = None
        carbon_weight = None

    import pandas as pd

    timestamps = pd.date_range("2026-03-24 00:00:00", periods=2, freq="h", tz="UTC")
    base_df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "price_per_kwh": [0.1, 0.2],
            "carbon_g_per_kwh": [100, 120],
        }
    )

    result = {
        "workload_input": Workload(),
        "metrics": {
            "optimized_cost": 0.1,
            "baseline_cost": 0.2,
            "optimized_carbon_kg": 0.1,
            "baseline_carbon_kg": 0.2,
        },
        "region": "us-west-2",
        "schedule": base_df.assign(eligible_flag=[1, 1]),
        "forecast": base_df,
        "optimized": base_df.assign(run_flag=[1, 0]),
        "baseline": base_df.assign(baseline_run_flag=[1, 0]),
    }

    temp_dir = _make_workspace_temp_dir()
    try:
        export_package = generate_export_package(
            result=result,
            export_root=temp_dir,
            run_id="util-fixed-run",
            enable_cloud_upload=True,
        )

        assert export_package["run_id"] == "util-fixed-run"
        assert export_package["cloud_save_enabled"] is True
        assert export_package["cloud_storage_configured"] is True
        assert export_package["cloud_message"] == "Uploaded 6 files to cloud storage."
        assert export_package["cloud_outputs"][0]["s3_key"] == "runs/util-fixed-run/util_optimization_recommendation.csv"
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_generate_export_package_skips_cloud_upload_when_disabled(monkeypatch) -> None:
    called = {"upload": False}

    def fake_upload_run_outputs(run_id, file_paths):
        called["upload"] = True
        return {}

    monkeypatch.setattr("src.exporter.upload_run_outputs", fake_upload_run_outputs)

    class Workload:
        objective = "cost"
        machine_watts = 1000
        compute_hours_required = 1
        deadline = None
        price_weight = None
        carbon_weight = None

    import pandas as pd

    timestamps = pd.date_range("2026-03-24 00:00:00", periods=2, freq="h", tz="UTC")
    base_df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "price_per_kwh": [0.1, 0.2],
            "carbon_g_per_kwh": [100, 120],
        }
    )

    result = {
        "workload_input": Workload(),
        "metrics": {
            "optimized_cost": 0.1,
            "baseline_cost": 0.2,
            "optimized_carbon_kg": 0.1,
            "baseline_carbon_kg": 0.2,
        },
        "region": "us-west-2",
        "schedule": base_df.assign(eligible_flag=[1, 1]),
        "forecast": base_df,
        "optimized": base_df.assign(run_flag=[1, 0]),
        "baseline": base_df.assign(baseline_run_flag=[1, 0]),
    }

    temp_dir = _make_workspace_temp_dir()
    try:
        export_package = generate_export_package(
            result=result,
            export_root=temp_dir,
            run_id="util-fixed-run",
            enable_cloud_upload=False,
        )

        assert called["upload"] is False
        assert export_package["cloud_save_enabled"] is False
        assert export_package["cloud_outputs"] == []
        assert export_package["cloud_message"] == ""
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_cloud_status_detail_reports_env_path_without_secret_values(monkeypatch, caplog) -> None:
    settings = {
        "AWS_ACCESS_KEY_ID": "visible-only-as-present",
        "AWS_SECRET_ACCESS_KEY": "super-secret-value",
        "AWS_REGION": "",
        "S3_BUCKET_NAME": "util-private-bucket",
    }

    s3_storage._build_cloud_status_detail.cache_clear()
    monkeypatch.setattr(s3_storage, "load_project_env", lambda: Path("C:/dev/util/.env"))
    monkeypatch.setattr(
        s3_storage,
        "get_project_env_diagnostics",
        lambda: {
            "env_path": "C:/dev/util/.env",
            "exists": True,
            "is_empty": False,
            "parsed_keys": ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "S3_BUCKET_NAME"],
            "has_aws_access_key_id": True,
            "has_aws_secret_access_key": True,
            "has_aws_region": True,
            "has_s3_bucket_name": True,
        },
    )
    monkeypatch.setattr(
        s3_storage,
        "resolve_cloud_config",
        lambda: {
            "source": "environment/.env",
            "values": settings,
            "configured": True,
        },
    )

    with caplog.at_level("INFO"):
        env_path, detail = s3_storage._build_cloud_status_detail()

    log_text = "\n".join(caplog.messages)
    expected_path = str(Path("C:/dev/util/.env"))
    assert env_path == expected_path
    assert detail == "Cloud config source: environment/.env"
    assert "Cloud config source: environment/.env" in log_text
    assert "super-secret-value" not in log_text


def test_build_s3_settings_strips_bucket_comment_suffix(monkeypatch) -> None:
    settings = {
        "AWS_ACCESS_KEY_ID": "key",
        "AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_REGION": "us-west-2 # west",
        "S3_BUCKET_NAME": "util-private-bucket # prod",
    }

    monkeypatch.setattr(s3_storage, "load_project_env", lambda: Path("C:/dev/util/.env"))
    monkeypatch.setattr(
        s3_storage,
        "get_project_env_diagnostics",
        lambda: {
            "env_path": "C:/dev/util/.env",
            "exists": True,
            "is_empty": False,
            "parsed_keys": ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "S3_BUCKET_NAME"],
            "has_aws_access_key_id": True,
            "has_aws_secret_access_key": True,
            "has_aws_region": True,
            "has_s3_bucket_name": True,
        },
    )
    monkeypatch.setattr(
        s3_storage,
        "resolve_cloud_config",
        lambda: {
            "source": "environment/.env",
            "values": settings,
            "configured": True,
        },
    )
    s3_storage._build_cloud_status_detail.cache_clear()

    result = s3_storage._build_s3_settings_result()

    assert result["configured"] is True
    assert result["settings"]["region_name"] == "us-west-2"
    assert result["settings"]["bucket_name"] == "util-private-bucket"


def test_upload_run_outputs_reports_bucket_access_failure(monkeypatch) -> None:
    settings = {
        "AWS_ACCESS_KEY_ID": "key",
        "AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_REGION": "us-east-2",
        "S3_BUCKET_NAME": "util-exports-finncase",
    }

    monkeypatch.setattr(s3_storage, "load_project_env", lambda: Path("C:/dev/util/.env"))
    monkeypatch.setattr(
        s3_storage,
        "get_project_env_diagnostics",
        lambda: {
            "env_path": "C:/dev/util/.env",
            "exists": True,
            "is_empty": False,
            "parsed_keys": list(settings.keys()),
            "has_aws_access_key_id": True,
            "has_aws_secret_access_key": True,
            "has_aws_region": True,
            "has_s3_bucket_name": True,
        },
    )
    monkeypatch.setattr(
        s3_storage,
        "resolve_cloud_config",
        lambda: {
            "source": "environment/.env",
            "values": settings,
            "configured": True,
        },
    )
    s3_storage._build_cloud_status_detail.cache_clear()
    monkeypatch.setattr(s3_storage, "create_s3_client", lambda: object())
    monkeypatch.setattr(s3_storage, "_validate_s3_bucket_access", lambda client, bucket_name: (False, "access denied", "Access Denied"))

    result = s3_storage.upload_run_outputs("util-test-run", [])

    assert result["configured"] is False
    assert result["message"] == "Cloud storage unavailable, using local only"
    assert result["failure_reason"] == "access denied"
    assert result["error_detail"] == "Access Denied"
    assert "bucket name value=util-exports-finncase" in result["debug_detail"]


def test_resolve_cloud_config_prefers_streamlit_secrets(monkeypatch) -> None:
    runtime_config.resolve_cloud_config.cache_clear()
    runtime_config._streamlit_secrets_dict.cache_clear()
    monkeypatch.setattr(
        runtime_config,
        "_streamlit_secrets_dict",
        lambda: {
            "AWS_ACCESS_KEY_ID": "secret-key-id",
            "AWS_SECRET_ACCESS_KEY": "secret-access-key",
            "AWS_REGION": "us-east-2",
            "S3_BUCKET_NAME": "streamlit-bucket",
        },
    )
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "env-key-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-access-key")
    monkeypatch.setenv("AWS_REGION", "us-west-1")
    monkeypatch.setenv("S3_BUCKET_NAME", "env-bucket")

    result = runtime_config.resolve_cloud_config()

    assert result["source"] == "streamlit secrets"
    assert result["values"]["S3_BUCKET_NAME"] == "streamlit-bucket"
    assert result["configured"] is True


def test_resolve_cloud_config_falls_back_to_env(monkeypatch) -> None:
    runtime_config.resolve_cloud_config.cache_clear()
    runtime_config._streamlit_secrets_dict.cache_clear()
    monkeypatch.setattr(runtime_config, "_streamlit_secrets_dict", lambda: {})
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "env-key-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-access-key")
    monkeypatch.setenv("AWS_REGION", "us-west-1")
    monkeypatch.setenv("S3_BUCKET_NAME", "env-bucket")

    result = runtime_config.resolve_cloud_config()

    assert result["source"] == "environment/.env"
    assert result["values"]["S3_BUCKET_NAME"] == "env-bucket"
    assert result["configured"] is True
