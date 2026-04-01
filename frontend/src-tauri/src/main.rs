use std::{
    collections::HashMap,
    error::Error,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Instant,
};

use tauri::{AppHandle, Manager};

type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const RUNTIME_ENV_FILE_NAME: &str = "runtime.env";
const DEBUG_DIR: &str = r"C:\temp";

#[cfg(target_os = "windows")]
const BACKEND_SIDECAR_FILE: &str = "util-backend-x86_64-pc-windows-msvc.exe";

const ALLOWED_RUNTIME_KEYS: &[&str] = &[
    "WATTTIME_USERNAME",
    "WATTTIME_PASSWORD",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION",
    "S3_BUCKET_NAME",
    "UTIL_PYTHON_BIN",
];

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            let _ = fs::create_dir_all(DEBUG_DIR);
            let _ = fs::write(Path::new(DEBUG_DIR).join("setup_entered.txt"), "setup entered\n");

            if let Err(error) = start_backend(app.app_handle().clone()) {
                let _ = write_debug_file("util_setup_error.txt", &format!("start_backend failed: {error}\n"));
                let _ = append_wrapper_log(&format!("backend startup failed: {error}"));
            }

            let _ = fs::write(Path::new(DEBUG_DIR).join("setup_finished.txt"), "setup finished\n");
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("failed to build Util desktop app")
        .run(|_, _| {});
}

fn start_backend(app: AppHandle) -> AppResult<()> {
    let startup_started_at = Instant::now();
    fs::create_dir_all(DEBUG_DIR)?;
    write_debug_file("util_debug_1.txt", "entered start_backend\n")?;
    append_wrapper_log("start_backend entered")?;

    let runtime_env = load_runtime_env(&app)?;
    write_debug_file(
        "util_runtime_env_count.txt",
        &format!("loaded {} runtime env vars\n", runtime_env.len()),
    )?;

    let backend_binary = resolve_backend_binary(&app)?;
    write_debug_file(
        "util_backend_path.txt",
        &format!("backend path: {}\n", backend_binary.display()),
    )?;

    let backend_dir = backend_binary
        .parent()
        .ok_or_else(|| format!("Backend binary has no parent directory: {}", backend_binary.display()))?
        .to_path_buf();
    write_debug_file(
        "util_backend_cwd.txt",
        &format!("backend current_dir: {}\n", backend_dir.display()),
    )?;

    let (stdout_log, stderr_log) = prepare_log_files()?;
    append_wrapper_log(&format!(
        "spawning backend from {} with cwd {}",
        backend_binary.display(),
        backend_dir.display()
    ))?;

    let spawn_started_at = Instant::now();
    let child = Command::new(&backend_binary)
        .current_dir(&backend_dir)
        .envs(runtime_env)
        .stdout(Stdio::from(stdout_log))
        .stderr(Stdio::from(stderr_log))
        .spawn()?;

    let spawn_elapsed_ms = spawn_started_at.elapsed().as_secs_f64() * 1000.0;
    let total_elapsed_ms = startup_started_at.elapsed().as_secs_f64() * 1000.0;
    write_debug_file("util_debug_2.txt", &format!("spawned pid={}\n", child.id()))?;
    append_wrapper_log(&format!(
        "spawned backend pid={} from {} spawn_ms={:.1} total_start_backend_ms={:.1}",
        child.id(),
        backend_binary.display(),
        spawn_elapsed_ms,
        total_elapsed_ms
    ))?;

    Ok(())
}

fn resolve_backend_binary(app: &AppHandle) -> AppResult<PathBuf> {
    let mut checked_paths: Vec<PathBuf> = Vec::new();

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    push_candidate(&mut checked_paths, manifest_dir.join("binaries").join(BACKEND_SIDECAR_FILE));

    if let Ok(current_exe) = std::env::current_exe() {
        if let Some(exe_dir) = current_exe.parent() {
            push_candidate(&mut checked_paths, exe_dir.join(BACKEND_SIDECAR_FILE));
            push_candidate(&mut checked_paths, exe_dir.join("binaries").join(BACKEND_SIDECAR_FILE));
            push_candidate(
                &mut checked_paths,
                exe_dir
                    .join("..")
                    .join("Resources")
                    .join("binaries")
                    .join(BACKEND_SIDECAR_FILE),
            );
        }
    }

    if let Ok(resource_dir) = app.path().resource_dir() {
        push_candidate(&mut checked_paths, resource_dir.join(BACKEND_SIDECAR_FILE));
        push_candidate(&mut checked_paths, resource_dir.join("binaries").join(BACKEND_SIDECAR_FILE));
    }

    if let Ok(app_local_data_dir) = app.path().app_local_data_dir() {
        push_candidate(
            &mut checked_paths,
            app_local_data_dir.join("binaries").join(BACKEND_SIDECAR_FILE),
        );
    }

    for candidate in &checked_paths {
        if candidate.is_file() {
            return Ok(candidate.clone());
        }
    }

    let checked = checked_paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join("\n - ");

    Err(format!(
        "Bundled backend executable was not found. Checked paths:\n - {}",
        checked
    )
    .into())
}

fn push_candidate(checked_paths: &mut Vec<PathBuf>, candidate: PathBuf) {
    if !checked_paths.iter().any(|existing| existing == &candidate) {
        checked_paths.push(candidate);
    }
}

fn prepare_log_files() -> AppResult<(File, File)> {
    let log_dir = PathBuf::from(DEBUG_DIR);
    fs::create_dir_all(&log_dir)?;

    let stdout_path = log_dir.join("backend.stdout.log");
    let stderr_path = log_dir.join("backend.stderr.log");
    let wrapper_path = log_dir.join("desktop-wrapper.log");

    let mut bootstrap = OpenOptions::new()
        .create(true)
        .append(true)
        .open(wrapper_path)?;

    writeln!(
        bootstrap,
        "Starting Util backend executable {}. stdout={}, stderr={}",
        BACKEND_SIDECAR_FILE,
        stdout_path.display(),
        stderr_path.display()
    )?;

    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(stdout_path)?;
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(stderr_path)?;

    Ok((stdout, stderr))
}

fn append_wrapper_log(message: &str) -> AppResult<()> {
    let log_dir = PathBuf::from(DEBUG_DIR);
    fs::create_dir_all(&log_dir)?;

    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join("desktop-wrapper.log"))?;
    writeln!(log, "{message}")?;
    Ok(())
}

fn write_debug_file(name: &str, contents: &str) -> AppResult<()> {
    fs::create_dir_all(DEBUG_DIR)?;
    fs::write(Path::new(DEBUG_DIR).join(name), contents)?;
    Ok(())
}

fn load_runtime_env(app: &AppHandle) -> AppResult<HashMap<String, String>> {
    let runtime_env_path = resolve_runtime_env_path(app)?;
    if !runtime_env_path.exists() {
        return Ok(HashMap::new());
    }

    let file = File::open(runtime_env_path)?;
    let reader = BufReader::new(file);
    let mut values = HashMap::new();

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();

        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let Some((raw_key, raw_value)) = trimmed.split_once('=') else {
            continue;
        };

        let key = raw_key.trim();
        if !ALLOWED_RUNTIME_KEYS.contains(&key) {
            continue;
        }

        let value = raw_value.trim().trim_matches('"').trim_matches('\'');
        values.insert(key.to_string(), value.to_string());
    }

    Ok(values)
}

fn resolve_runtime_env_path(app: &AppHandle) -> AppResult<PathBuf> {
    if let Ok(override_path) = std::env::var("UTIL_RUNTIME_ENV_FILE") {
        let path = PathBuf::from(override_path);
        if path.is_absolute() {
            return Ok(path);
        }

        return Ok(std::env::current_dir()?.join(path));
    }

    let config_dir = app.path().app_config_dir()?;
    fs::create_dir_all(&config_dir)?;
    Ok(config_dir.join(RUNTIME_ENV_FILE_NAME))
}
