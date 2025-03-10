use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, exit};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use anyhow::{Result, anyhow};
use ignore::gitignore::{Gitignore, GitignoreBuilder};

use ariana_server::traces::Trace;
use ariana_server::web::traces::{CodeInstrumentationRequest, CodeInstrumentationResponse, PushTracesRequest};
use ariana_server::web::vaults::VaultPublicData;
use clap::Parser;
use tokio::fs::create_dir_all;
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio::time::interval;
use ariana_server::traces::instrumentalization::ecma::EcmaImportStyle;
use rand::{thread_rng, distributions::Alphanumeric, Rng};
use sha2::{Sha256, Digest};
use async_recursion::async_recursion;
use dirs;
use ctrlc;

#[derive(Parser)]
#[command(
    version, 
    about = "Ariana CLI - Instrumentalize your code to collect Ariana traces while it runs."
)]

struct Cli {
    /// API URL for Ariana server
    #[arg(long, default_value_t = if cfg!(debug_assertions) { "http://localhost:8080/".to_string() } else { "https://api.ariana.dev/".to_string() })]
    api_url: String,

    /// Instrumentalize the original code files instead of a copy of them under .ariana. PS: this will still backup and try to restore the original files, whether the command fails abruptly or gracefully
    #[arg(long)]
    inplace: bool,

    /// Also save traces locally under .ariana_saved_traces instead of deleting them after sending & saving them to the Ariana server
    #[arg(long)]
    save_local: bool,

    /// Only save traces locally under .ariana_saved_traces, do not send & save them to the Ariana server. (This will still send your code to the server for instrumentation)
    #[arg(long)]
    only_local: bool,

    /// Start MCP server for model context protocol
    #[arg(long)]
    mcp: bool,

    /// The command to execute in the instrumented code directory
    #[arg(required = true, trailing_var_arg = true)]
    command: Vec<String>,
}

const TRACE_DIR: &str = ".traces";
const ARIANA_DIR: &str = ".ariana";
const SAVED_TRACES_DIR: &str = ".ariana_saved_traces";

#[tokio::main]
async fn main() -> Result<()> {
    // Enable backtraces
    env::set_var("RUST_BACKTRACE", "1");
    
    let cli = Cli::parse();

    if cli.mcp {
        launch_mcp_server().await
    } else {
        match run_main(cli).await {
            Ok(_) => Ok(()),
            Err(e) => {
                eprintln!("Error occurred: {:#}", e);
                
                let backtrace = e.backtrace();
                eprintln!("\nBacktrace:\n{}", backtrace);
    
                Err(e)
            }
        }
    }
}

async fn run_main(cli: Cli) -> Result<()> {
    // Get the current directory to process
    let current_dir = env::current_dir()?;

    // Create a new vault
    println!("[Ariana] Creating a new vault...");
    let vault_key = create_vault(&cli.api_url).await?;

    // Create the .ariana directory for backups/instrumented code
    let ariana_dir = current_dir.join(ARIANA_DIR);
    if ariana_dir.exists() {
        println!("[Ariana] Removing existing .ariana directory");
        fs::remove_dir_all(&ariana_dir)?;
    }
    create_dir_all(&ariana_dir).await?;

    // Set up the trace directory - either in original directory or .ariana based on inplace flag
    let trace_dir = if cli.inplace {
        current_dir.join(TRACE_DIR)
    } else {
        ariana_dir.join(TRACE_DIR)
    };
    create_dir_all(&trace_dir).await?;

    // Create active flag file
    let active_flag = trace_dir.join(".active");
    tokio::fs::write(&active_flag, "1").await?;

    // Add .ariana to .gitignore
    add_to_gitignore(&current_dir).await?;

    // Backup or copy files based on inplace flag
    let working_dir = if cli.inplace {
        // Backup files to .ariana without instrumentation
        process_directory_backup(&current_dir, &ariana_dir, &current_dir)?;
        current_dir.clone()
    } else {
        // Copy and instrument files to .ariana
        println!("[Ariana] Instrumenting your code...");
        instrumentalize_project(&cli.api_url, &current_dir, &vault_key, false).await?;
        ariana_dir.clone()
    };

    // If running in-place, instrument the files in the original location
    if cli.inplace {
        println!("[Ariana] Instrumenting your code in-place...");
        instrumentalize_project(&cli.api_url, &current_dir, &vault_key, true).await?;
    }

    // Start trace watcher in a separate task
    let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
    let api_url = cli.api_url.clone();
    let save_local = cli.save_local || cli.only_local;
    let only_local = cli.only_local;
    let trace_watcher = spawn(async move {
        let _ = watch_traces(&trace_dir, &api_url, &vault_key, &mut stop_rx, save_local, only_local).await;
    });

    // Prepare the command to run
    let mut command_args = cli.command.clone();
    let command = command_args.remove(0);
    
    println!("[Ariana] Running command in {}/ : {} {}", working_dir.file_name().unwrap().to_str().unwrap(), command, command_args.join(" "));
    if !cli.inplace {
        println!("[Ariana] tip: To run the command in the original directory, use the --inplace flag (in that case original files will be temporarily edited and then restored).");
    }

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    // Execute the command in the working directory
    let status = if cfg!(windows) {
        tokio::process::Command::new("cmd")
            .args(&["/C", &command])
            .args(&command_args)
            .current_dir(&working_dir)
            .env("TRACE_DIR", TRACE_DIR)
            .status()
    } else {
        tokio::process::Command::new(&command)
            .args(&command_args)
            .current_dir(&working_dir)
            .env("TRACE_DIR", TRACE_DIR)
            .status()
    };
    
    let _ = ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    });

    let status = status.await?;

    println!("[Ariana] Command status: {:?}", status);
        
    // Stop the trace watcher
    let _ = stop_tx.send(()).await;

    // wait for 3 seconds
    tokio::time::sleep(Duration::from_secs(3)).await;
    
    // Clean up
    let _ = tokio::fs::remove_file(active_flag).await;
    
    // Wait for the trace watcher to complete
    let _ = trace_watcher.await;

    // If running in-place, restore original files
    if cli.inplace {
        restore_from_backup(&working_dir, &ariana_dir)?;
        // Don't delete the backup directory, just in case
        println!("[Ariana] Your instrumented code files just got restored from backup. In case something went wrong, please find the backup preserved in {}", ariana_dir.display());
    }
    
    cleanup_traces_active(&current_dir)?;

    // Exit with the same status code as the command
    if !status.success() {
        exit(status.code().unwrap_or(1));
    }

    Ok(())
}

async fn watch_traces(
    trace_dir: &Path,
    api_url: &str,
    vault_key: &str,
    stop_rx: &mut mpsc::Receiver<()>,
    save_local: bool,
    only_local: bool,
) -> Result<()> {
    let mut interval = interval(Duration::from_millis(500));
    
    let mut stop_requested = false;
    let mut stop_time = None;

    // Create saved traces directory if needed
    let saved_traces_dir = if save_local {
        let dir = Path::new(".").join(SAVED_TRACES_DIR).join(vault_key);
        create_dir_all(&dir).await?;
        Some(dir)
    } else {
        None
    };

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Check if the flag file exists, if not, break
                if !trace_dir.join(".active").exists() {
                    break;
                }
                
                // Find all trace files
                let trace_files = match fs::read_dir(trace_dir) {
                    Ok(entries) => entries
                        .filter_map(|e| e.ok())
                        .take(100)
                        .filter(|e| {
                            let file_name = e.file_name();
                            let file_name = file_name.to_string_lossy();
                            file_name.starts_with("trace-") && file_name.ends_with(".json")
                        })
                        .collect::<Vec<_>>(),
                    Err(e) => {
                        eprintln!("[Ariana] Error reading trace directory: {}", e);
                        continue;
                    }
                };

                if trace_files.len() == 0 && stop_requested {
                    if let Some(stop_time) = stop_time {
                        if tokio::time::Instant::now() >= stop_time {
                            break;
                        }
                    }
                }

                if stop_requested {
                    println!("[Ariana] Stop requested, waiting for a few seconds to process remaining traces...");
                }
                
                // Process all trace files concurrently
                let futures: Vec<_> = trace_files.iter().map(|file| {
                    let file_path = file.path();
                    let api_url = api_url.to_string();
                    let vault_key = vault_key.to_string();
                    let saved_traces_dir = saved_traces_dir.clone();
                    async move {
                        if !only_local {
                            match process_trace(&file_path, &api_url, &vault_key).await {
                                Ok(_) => {},
                                Err(e) => {
                                    eprintln!("[Ariana] Error processing trace {}: {}", file_path.display(), e);
                                }
                            };
                        }

                        if let Some(saved_traces_dir) = saved_traces_dir {
                            if let Err(e) = save_trace_locally(&file_path, &saved_traces_dir) {
                                eprintln!("[Ariana] Error saving trace to {}: {}", saved_traces_dir.display(), e);
                            }
                        } else {
                            if let Err(e) = fs::remove_file(&file_path) {
                                eprintln!("[Ariana] Error deleting processed trace {}: {}", file_path.display(), e);
                            }
                        }
                    }
                }).collect();

                futures::future::join_all(futures).await;
            }
            _ = stop_rx.recv() => {
                stop_requested = true;
                stop_time = Some(tokio::time::Instant::now() + Duration::from_secs(3));
            }
        }
    }

    Ok(())
}

fn save_trace_locally(
    trace_path: &Path,
    saved_traces_dir: &Path
) -> Result<()> {
    let new_path = saved_traces_dir.join(trace_path.file_name().unwrap());

    let content = fs::read_to_string(trace_path)?;
    let trace_data: serde_json::Value = serde_json::from_str(&content)?;

    // Extract trace from the trace file
    let trace = serde_json::from_value::<Trace>(trace_data["trace"].clone())
        .map_err(|e| anyhow!("Failed to parse trace data: {}", e))?;

    // Just save the trace to the new path
    fs::write(&new_path, serde_json::to_string_pretty(&trace).unwrap())?;

    Ok(())
}

async fn process_trace(
    trace_path: &Path,
    api_url: &str,
    vault_key: &str
) -> Result<()> {
    let content = fs::read_to_string(trace_path)?;
    let trace_data: serde_json::Value = serde_json::from_str(&content)?;

    // Extract trace from the trace file
    let trace = serde_json::from_value::<Trace>(trace_data["trace"].clone())
        .map_err(|e| anyhow!("Failed to parse trace data: {}", e))?;
    
    // Create a properly typed request
    let request = PushTracesRequest {
        traces: vec![trace],
    };
    
    // Send the trace to the server
    let client = reqwest::Client::new();
    let response = client
        .post(&format!("{}/vaults/traces/{}/push", api_url, vault_key))
        .header("Content-Type", "application/json")
        .json(&request)
        .send()
        .await?;
    
    if !response.status().is_success() {
        return Err(anyhow!("Failed to process trace: {}", response.status()));
    }
    
    Ok(())
}

async fn instrumentalize_project(
    api_url: &str,
    project_root: &Path,
    vault_key: &str,
    inplace: bool
) -> Result<()> {
    // Create the .ariana directory
    let ariana_dir = project_root.join(ARIANA_DIR);
    create_dir_all(&ariana_dir).await?;

    let vault_secret_key_path = ariana_dir.join(".vault_secret_key");
    let vault_secret_key_content = format!("{}\nDO NOT SHARE THE ABOVE KEY WITH ANYONE, DO NOT COMMIT IT TO VERSION CONTROL", vault_key);
    tokio::fs::write(&vault_secret_key_path, vault_secret_key_content).await?;

    // Detect project import style
    let import_style = detect_project_import_style(project_root).await?;
    
    // Process the directory
    process_directory(
        project_root.to_path_buf(),
        if inplace { project_root.to_path_buf() } else { ariana_dir.to_path_buf() },
        project_root,
        api_url,
        vault_key,
        &import_style,
        inplace
    ).await?;
    
    println!("[Ariana] Instrumentation complete!");
    
    Ok(())
}

async fn detect_project_import_style(project_root: &Path) -> Result<EcmaImportStyle> {
    let package_json_path = project_root.join("package.json");
    
    if package_json_path.exists() {
        let package_json = fs::read_to_string(package_json_path)?;
        let package_data: serde_json::Value = serde_json::from_str(&package_json)?;
        
        // Check package.json "type" field first
        if let Some(type_field) = package_data.get("type") {
            if type_field.as_str() == Some("module") {
                return Ok(EcmaImportStyle::ESM);
            }
        }
        
        // Check for specific indicators in package.json
        let has_esm_indicators = 
            package_data.get("exports").is_some() || 
            package_data.get("module").is_some() || 
            package_data.get("mjs").is_some();
        
        if has_esm_indicators {
            return Ok(EcmaImportStyle::ESM);
        }
    }
    
    // Default to CJS if no strong ESM indicators
    Ok(EcmaImportStyle::CJS)
}

#[async_recursion]
async fn process_directory(
    src: PathBuf,
    dest: PathBuf,
    project_root: &Path,
    api_url: &str,
    vault_key: &str,
    import_style: &EcmaImportStyle,
    inplace: bool
) -> Result<()> {
    // Create the destination directory if not in-place mode
    if !inplace {
        create_dir_all(dest.clone()).await?;
    }

    let mut subdirs_processing_futures = vec![];
    let mut files_processing_futures = vec![];

    // Load .gitignore and .arianaignore patterns
    let mut builder = GitignoreBuilder::new(&src);
    if let Ok(Some(gitignore)) = find_nearest_gitignore(&src) {
        builder.add(gitignore);
    }
    let ariana_ignore = src.join(".arianaignore");
    if ariana_ignore.exists() {
        builder.add(ariana_ignore);
    }
    let ignore_matcher = builder.build().unwrap_or_else(|_| Gitignore::empty());

    // Get all entries in the source directory
    for entry in fs::read_dir(src.clone())? {
        let entry = entry?;
        let entry_path = entry.path();
        
        // Compute the equivalent destination path
        let rel_path = entry_path.strip_prefix(Path::new(&src))?;
        let dest_path = dest.join(Path::new(&rel_path));
        
        // Process based on entry type
        if entry_path.is_dir() {
            let dir_name = entry_path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
                
            if should_skip_directory(dir_name) {
                // For node_modules and other special dirs, create a symlink or copy if not in-place
                if !inplace {
                    create_link_or_copy(&entry_path, &dest_path).await?;
                }
                continue;
            }
            
            // Clone paths to owned PathBufs to ensure they live long enough
            let entry_path_owned = entry_path.to_path_buf();
            let dest_path_owned = dest_path.to_path_buf();
            
            subdirs_processing_futures.push(process_directory(
                entry_path_owned,
                dest_path_owned,
                project_root,
                api_url,
                vault_key,
                import_style,
                inplace
            ));
        } else if entry_path.is_file() {
            let filename = entry_path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
                
            if filename.ends_with(".config.js") || filename.ends_with(".config.ts") {
                if !inplace {
                    fs::copy(&entry_path, &dest_path)?;
                }
                continue;
            }

            // Process the file
            if let Some(ext) = entry_path.extension() {
                let ext_str = ext.to_string_lossy().to_lowercase();
                if ["js", "jsx", "ts", "tsx", "py"].contains(&ext_str.as_str()) {
                    let content = fs::read_to_string(&entry_path)?;
                    let entry_path_owned = entry_path.to_path_buf();
                    let project_root_owned = project_root.to_path_buf();
                    let dest_path_owned = if inplace { entry_path.to_path_buf() } else { dest_path.to_path_buf() };
                    
                    // Check if file should be ignored for instrumentation
                    let should_instrument = !ignore_matcher.matched(&entry_path, false).is_ignore();
                    
                    if should_instrument {
                        files_processing_futures.push(async move {
                            let file_path_str = entry_path_owned.to_string_lossy().to_string();
                            match instrument_file(
                                entry_path_owned,
                                content,
                                project_root_owned,
                                api_url.to_string(),
                                vault_key.to_string(),
                                import_style.clone()
                            ).await {
                                Ok(instrumented_content) => {
                                    if let Err(_) = fs::write(dest_path_owned, instrumented_content) {
                                        return Err(file_path_str);
                                    }
                                    Ok(())
                                },
                                Err(_) => Err(file_path_str)
                            }
                        });
                    } else if !inplace {
                        // For ignored files, just copy them without instrumentation
                        fs::copy(&entry_path, &dest_path)?;
                    }
                } else if !inplace {
                    // Just copy non-JS/TS files when not in-place
                    fs::copy(&entry_path, &dest_path)?;
                }
            } else {
                fs::copy(&entry_path, &dest_path)?;
            }
        }
    }

    // Wait for all subdirectories to be processed
    let subdir_results = futures::future::join_all(subdirs_processing_futures).await;
    for result in subdir_results {
        result?;
    }

    // Wait for all files to be processed
    let file_results = futures::future::join_all(files_processing_futures).await;
    let failed_files: Vec<String> = file_results.into_iter()
        .filter_map(|r| r.err())
        .collect();
    
    if !failed_files.is_empty() {
        eprintln!("[Ariana] Failed to process the following files:");
        for file in failed_files {
            eprintln!("  {}", file);
        }
        return Err(anyhow!("Failed to process some files"));
    }

    Ok(())
}

fn should_skip_directory(dir_name: &str) -> bool {
    let skip_list = ["node_modules", ".git", ".ariana", "dist", "build", "target"];
    skip_list.contains(&dir_name)
}

async fn create_link_or_copy(src: &Path, dest: &Path) -> Result<()> {
    // Try to create a symlink first
    #[cfg(unix)]
    {
        match std::os::unix::fs::symlink(src, dest) {
            Ok(_) => return Ok(()),
            Err(_) => {
                // If symlink fails, use fs_extra to copy
                let options = fs_extra::dir::CopyOptions::new();
                fs_extra::dir::copy(src, dest.parent().unwrap(), &options)?;
                return Ok(());
            }
        }
    }
    
    #[cfg(windows)]
    {
        match std::os::windows::fs::symlink_dir(src, dest) {
            Ok(_) => return Ok(()),
            Err(_) => {
                // If symlink fails, use fs_extra to copy
                let options = fs_extra::dir::CopyOptions::new();
                fs_extra::dir::copy(src, dest.parent().unwrap(), &options)?;
                return Ok(());
            }
        }
    }
    
    #[cfg(not(any(unix, windows)))]
    {
        // For other platforms, just copy
        let options = fs_extra::dir::CopyOptions::new();
        fs_extra::dir::copy(src, dest.parent().unwrap(), &options)?;
        Ok(())
    }
}

async fn instrument_file(
    file_path: PathBuf,
    content: String,
    project_root: PathBuf,
    api_url: String,
    vault_key: String,
    project_import_style: EcmaImportStyle
) -> Result<String> {
    // Create a properly typed request
    let request = CodeInstrumentationRequest {
        file_content: content.to_string(),
        file_path: file_path.to_string_lossy().to_string(),
        project_root: project_root.to_string_lossy().to_string(),
        project_import_style: Some(project_import_style.clone()),
    };
    
    // Call the server API to instrumentalize the code
    let client = reqwest::Client::new();
    let response = client
        .post(&format!("{}/vaults/traces/{}/instrumentalize", api_url, vault_key))
        .header("Content-Type", "application/json")
        .json(&request)
        .send()
        .await?;
    
    if !response.status().is_success() {
        return Err(anyhow!("Failed to instrumentalize code: HTTP {}", response.status()));
    }
    
    // Parse the response to get the instrumented content
    let response_data: CodeInstrumentationResponse = response.json().await?;
    
    Ok(response_data.instrumented_content)
}

async fn add_to_gitignore(project_root: &Path) -> Result<()> {
    println!("[Ariana] Adding ariana temporary files & secrets to .gitignore...");
    
    // Find the nearest .gitignore file
    let gitignore_path = find_nearest_gitignore(project_root)?;
    let entries_to_add = vec![".ariana/", ".ariana.json", ".vault_secret_key", ".traces/", ".ariana_saved_traces/"];
    
    // Create a new .gitignore if none exists
    if gitignore_path.is_none() {
        println!("[Ariana] No .gitignore found, creating new one...");
        let content = entries_to_add.join("\n") + "\n";
        fs::write(project_root.join(".gitignore"), content)?;
        return Ok(());
    }
    
    // Read existing .gitignore
    let gitignore_path = gitignore_path.unwrap();
    let content = fs::read_to_string(&gitignore_path)?;
    let mut lines: Vec<String> = content
        .lines()
        .map(|line| line.trim().to_string())
        .collect();
    
    // Add new entries if they don't exist
    let mut modified = false;
    for entry in entries_to_add {
        if !lines.contains(&entry.to_string()) {
            lines.push(entry.to_string());
            modified = true;
        }
    }
    
    // Save if changes were made
    if modified {
        let new_content = lines.join("\n") + "\n";
        fs::write(gitignore_path, new_content)?;
        println!("[Ariana] Updated .gitignore");
    }
    
    Ok(())
}

fn find_nearest_gitignore(start_path: &Path) -> Result<Option<PathBuf>> {
    let mut current_path = start_path.to_path_buf();
    
    loop {
        let gitignore_path = current_path.join(".gitignore");
        if gitignore_path.exists() {
            return Ok(Some(gitignore_path));
        }
        
        // Move up to parent directory
        if !current_path.pop() {
            // We've reached the root and found no .gitignore
            return Ok(None);
        }
    }
}

/// Create a new vault and return the vault secret key
async fn create_vault(api_url: &str) -> Result<String> {
    // Generate a machine hash (just a random ID in this case)
    let machine_hash = generate_machine_id()?;
    
    // Call the server API to create a vault
    let client = reqwest::Client::new();
    let response = client
        .post(&format!("{}/unauthenticated/vaults/create", api_url))
        .header("X-Machine-Hash", machine_hash)
        .send()
        .await?;
    
    if !response.status().is_success() {
        return Err(anyhow!("Failed to create vault: HTTP {}", response.status()));
    }
    
    // Parse the response to get the vault key
    let vault_data: VaultPublicData = response.json().await?;
    
    Ok(vault_data.secret_key)
}

/// Generate a unique machine ID (for X-Machine-Hash header)
fn generate_machine_id() -> Result<String> {
    // Try to get a stable machine ID if possible, otherwise generate a random one
    let id = match get_stable_machine_id() {
        Some(id) => id,
        None => {
            // Generate a random ID and save it for future use
            let rng = thread_rng();
            let random_id: String = rng
                .sample_iter(&Alphanumeric)
                .take(32)
                .map(char::from)
                .collect();
            
            // Save this ID for future use
            let ariana_dir = dirs::home_dir()
                .ok_or_else(|| anyhow!("Could not determine home directory"))?
                .join(".ariana");
            
            fs::create_dir_all(&ariana_dir)?;
            fs::write(ariana_dir.join("machine-id"), &random_id)?;
            
            random_id
        }
    };
    
    // Hash the ID for privacy
    let mut hasher = Sha256::new();
    hasher.update(id.as_bytes());
    let result = hasher.finalize();
    
    Ok(format!("{:x}", result))
}

/// Try to get a stable machine ID from the filesystem
fn get_stable_machine_id() -> Option<String> {
    // First check if we've already created an ID
    if let Some(home_dir) = dirs::home_dir() {
        let ariana_id_path = home_dir.join(".ariana").join("machine-id");
        if let Ok(id) = fs::read_to_string(&ariana_id_path) {
            return Some(id);
        }
    }
    
    // Try to get a system machine ID (Windows/Linux specific)
    #[cfg(windows)]
    {
        if let Ok(output) = Command::new("wmic").args(["csproduct", "get", "UUID"]).output() {
            let output = String::from_utf8_lossy(&output.stdout);
            let uuid = output.lines().nth(1).unwrap_or("").trim();
            if !uuid.is_empty() {
                return Some(uuid.to_string());
            }
        }
    }
    
    #[cfg(unix)]
    {
        if let Ok(id) = fs::read_to_string("/etc/machine-id") {
            return Some(id.trim().to_string());
        }
        
        if let Ok(id) = fs::read_to_string("/var/lib/dbus/machine-id") {
            return Some(id.trim().to_string());
        }
    }
    
    None
}

/// Process directory by copying files without instrumentation (for backup)
fn process_directory_backup(src: &Path, dest: &Path, project_root: &Path) -> Result<()> {
    if should_skip_directory(src.file_name().unwrap_or_default().to_str().unwrap_or_default()) {
        return Ok(());
    }

    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let relative_path = path.strip_prefix(project_root)?;
        let target_path = dest.join(relative_path);

        if path.is_dir() {
            fs::create_dir_all(&target_path)?;
            process_directory_backup(&path, dest, project_root)?;
        } else if path.is_file() {
            fs::create_dir_all(target_path.parent().unwrap())?;
            fs::copy(&path, &target_path)?;
        }
    }
    Ok(())
}

/// Restore files from backup
fn restore_from_backup(working_dir: &Path, backup_dir: &Path) -> Result<()> {
    println!("[Ariana] Restoring files from backup...");
    
    // First, collect all files that need to be restored
    let mut files_to_restore = Vec::new();
    collect_files_to_restore(backup_dir, backup_dir, &mut files_to_restore)?;

    // Then restore each file
    for (backup_path, relative_path) in files_to_restore {
        let target_path = working_dir.join(&relative_path);
        
        // Create parent directory if it doesn't exist
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Copy the file, overwriting the instrumented version
        if backup_path.is_file() {
            fs::copy(&backup_path, &target_path)?;
        }
    }

    Ok(())
}

/// Helper function to collect all files that need to be restored
fn collect_files_to_restore(
    current_path: &Path,
    backup_root: &Path,
    files: &mut Vec<(PathBuf, PathBuf)>
) -> Result<()> {
    if current_path.is_dir() {
        for entry in fs::read_dir(current_path)? {
            let entry = entry?;
            let path = entry.path();
            
            // Skip .git directory
            if path.is_dir() && path.file_name().unwrap_or_default() == ".git" {
                continue;
            }

            // Skip .ariana directory
            if path.is_dir() && path.file_name().unwrap_or_default() == ARIANA_DIR {
                continue;
            }

            // Get the relative path from the backup root
            let binding = path.clone();
            let relative_path = binding.strip_prefix(backup_root)?;

            if path.is_dir() {
                collect_files_to_restore(&path, backup_root, files)?;
            } else if path.is_file() {
                files.push((path, relative_path.to_path_buf()));
            }
        }
    }
    Ok(())
}

/// Ensure cleanup of .traces/.active even if process is interrupted
fn cleanup_traces_active(project_root: &Path) -> Result<()> {
    let active_file = project_root.join(TRACE_DIR).join(".active");
    if active_file.exists() {
        match fs::remove_file(&active_file) {
            Ok(_) => Ok(()),
            Err(e) => {
                // If we can't delete it, just warn but don't fail
                eprintln!("[Ariana] Warning: Could not delete .traces/.active file: {}", e);
                Ok(())
            }
        }
    } else {
        Ok(())
    }
}